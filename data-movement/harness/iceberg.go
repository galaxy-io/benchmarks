package harness

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
)

const (
	MinIOImage          = "minio/minio:latest"
	IcebergCatalogImage = "apache/iceberg-rest-fixture:latest"

	IcebergBucket = "warehouse"
	IcebergKey    = "bench"
	IcebergSecret = "benchbench"
	IcebergRegion = "us-east-1"
)

// Props keys an Iceberg sink carries beyond its DSN. A provider fills these in
// and the SUTs read them, so the names are a contract between the two.
const (
	PropWarehouse  = "warehouse"
	PropS3Endpoint = "s3.endpoint"
	PropS3Key      = "s3.access-key-id"
	PropS3Secret   = "s3.secret-access-key"
	PropS3Token    = "s3.session-token"
	PropS3Region   = "s3.region"

	// PropS3HostEndpoint is the object store as the host sees it, which is
	// where Count attaches from; PropS3Endpoint is how the SUTs reach it. Both
	// are empty for S3 itself, which every client already knows how to find.
	PropS3HostEndpoint = "s3.host-endpoint"

	// MinIO answers plaintext on a path-style URL and S3 does neither, so the
	// provider that handed the store over says which, rather than each writer
	// assuming. Values are "true" and "false".
	PropS3UseSSL    = "s3.use-ssl"
	PropS3PathStyle = "s3.path-style"
)

type icebergEngine struct{}

// Name identifies the engine.
func (icebergEngine) Name() string { return "iceberg" }

// CreateIcebergNamespace creates the bench namespace on a REST catalog,
// mirroring how the SQL engines pre-create their bench database. The fixture's
// SQLite backend also races on concurrent creation from parallel writers.
func CreateIcebergNamespace(ctx context.Context, catalogURL string) error {
	body := fmt.Sprintf(`{"namespace":[%q],"properties":{}}`, Namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		catalogURL+"/v1/namespaces", strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("create iceberg namespace: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusConflict {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("create iceberg namespace: %s", resp.Status)
	}
	return nil
}

// VerifyIcebergStore writes an object to the warehouse and reads it back before
// the timed run. Creating a namespace only touches the catalog, so it cannot
// detect a broken policy, route, or host credential chain. This check uses the
// same DuckDB path as destination validation; each SUT still uses its own SDK.
func VerifyIcebergStore(ctx context.Context, db *DB) error {
	secret, err := s3Secret(db)
	if err != nil {
		return err
	}
	object := fmt.Sprintf("s3://%s/_preflight.parquet", db.Props[PropWarehouse])
	script := fmt.Sprintf(`INSTALL httpfs; LOAD httpfs;
%s
COPY (SELECT 1 AS ok) TO %s (FORMAT parquet);
SELECT count(*) FROM read_parquet(%s);`, secret, duckDBString(object), duckDBString(object))
	out, err := exec.CommandContext(ctx, "duckdb", "-csv", "-noheader", "-c", script).CombinedOutput()
	if err != nil {
		return fmt.Errorf("warehouse %s is not writable: %w:\n%s", db.Props[PropWarehouse], err, out)
	}
	return nil
}

// Open fails because Iceberg has no database/sql handle; validation uses Count.
func (icebergEngine) Open(db *DB) (*sql.DB, error) {
	return nil, fmt.Errorf("iceberg has no sql handle")
}

// Load fails: iceberg is sink-only and is never seeded.
func (icebergEngine) Load(ctx context.Context, db *DB, t TableDef) (int64, error) {
	return 0, fmt.Errorf("iceberg is sink-only")
}

// s3Secret renders the DuckDB secret Count reads the warehouse through. A
// MinIO store is addressed at its own endpoint and S3 at the region's, which
// DuckDB finds on its own.
func s3Secret(db *DB) (string, error) {
	region := db.Props[PropS3Region]
	if region == "" {
		return "", fmt.Errorf("iceberg db has no %s prop", PropS3Region)
	}
	fields := []string{
		"REGION " + duckDBString(region),
		fmt.Sprintf("USE_SSL %s", boolProp(db, PropS3UseSSL)),
	}
	// Without a static key, DuckDB walks the same default chain the adapters
	// do and reaches the host's instance role.
	if key := db.Props[PropS3Key]; key == "" {
		fields = append(fields, "PROVIDER credential_chain")
	} else {
		fields = append(fields,
			"KEY_ID "+duckDBString(key),
			"SECRET "+duckDBString(db.Props[PropS3Secret]))
		if token := db.Props[PropS3Token]; token != "" {
			fields = append(fields, "SESSION_TOKEN "+duckDBString(token))
		}
	}
	if endpoint := db.Props[PropS3HostEndpoint]; endpoint != "" {
		fields = append(fields, "ENDPOINT "+duckDBString(endpoint))
	}
	if boolProp(db, PropS3PathStyle) == "true" {
		fields = append(fields, "URL_STYLE 'path'")
	}
	return fmt.Sprintf("CREATE SECRET mc (TYPE s3, %s);", strings.Join(fields, ", ")), nil
}

func duckDBString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// boolProp reads a "true"/"false" prop, defaulting to false.
func boolProp(db *DB, key string) string {
	if db.Props[key] == "true" {
		return "true"
	}
	return "false"
}

// Count counts one bench table through DuckDB's Iceberg extension, attaching
// the REST catalog from the host.
func (icebergEngine) Count(ctx context.Context, db *DB, table string) (int64, error) {
	if _, err := qualifiedTable(table); err != nil {
		return 0, err
	}
	secret, err := s3Secret(db)
	if err != nil {
		return 0, err
	}
	script := fmt.Sprintf(`INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
%s
ATTACH %s AS ice (TYPE iceberg, ENDPOINT %s, AUTHORIZATION_TYPE 'none');
SELECT count(*) FROM ice.%s.%s;`,
		secret, duckDBString(db.Props[PropWarehouse]), duckDBString(db.DSN), Namespace, table)
	out, err := exec.CommandContext(ctx, "duckdb", "-csv", "-noheader", "-c", script).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("duckdb iceberg count %s: %w:\n%s", table, err, out)
	}
	lines := strings.Fields(strings.TrimSpace(string(out)))
	if len(lines) == 0 {
		return 0, fmt.Errorf("duckdb iceberg count %s: empty output", table)
	}
	n, err := strconv.ParseInt(lines[len(lines)-1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("duckdb iceberg count %s: parse %q: %w", table, lines[len(lines)-1], err)
	}
	return n, nil
}
