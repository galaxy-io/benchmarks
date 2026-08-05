package harness

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	MinIOImage          = "minio/minio:latest"
	IcebergCatalogImage = "apache/iceberg-rest-fixture:latest"

	icebergBucket = "warehouse"
	icebergKey    = "bench"
	icebergSecret = "benchbench"
)

type icebergEngine struct{}

// Name identifies the engine.
func (icebergEngine) Name() string { return "iceberg" }

// Start runs MinIO and an Iceberg REST catalog on net; the catalog is the main
// container and MinIO rides along in Aux. The internal DSN carries the catalog
// endpoint and the S3 settings a writer needs, in one URI.
func (e icebergEngine) Start(ctx context.Context, net *tc.DockerNetwork, alias, runID string) (*DB, error) {
	minioAlias := alias + "-minio"
	minio, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:        MinIOImage,
			Cmd:          []string{"server", "/data"},
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     icebergKey,
				"MINIO_ROOT_PASSWORD": icebergSecret,
			},
			Labels:         map[string]string{LabelRun: runID, LabelRole: minioAlias},
			Networks:       []string{net.Name},
			NetworkAliases: map[string][]string{net.Name: {minioAlias}},
			WaitingFor:     wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("minio: %w", err)
	}
	// MinIO's filesystem backend surfaces a top-level directory as a bucket.
	if code, _, err := minio.Exec(ctx, []string{"mkdir", "-p", "/data/" + icebergBucket}); err != nil || code != 0 {
		return nil, fmt.Errorf("create bucket (exit %d): %w", code, err)
	}

	catalog, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:        IcebergCatalogImage,
			ExposedPorts: []string{"8181/tcp"},
			Env: map[string]string{
				"AWS_ACCESS_KEY_ID":              icebergKey,
				"AWS_SECRET_ACCESS_KEY":          icebergSecret,
				"AWS_REGION":                     "us-east-1",
				"CATALOG_WAREHOUSE":              "s3://" + icebergBucket + "/",
				"CATALOG_IO__IMPL":               "org.apache.iceberg.aws.s3.S3FileIO",
				"CATALOG_S3_ENDPOINT":            "http://" + minioAlias + ":9000",
				"CATALOG_S3_PATH__STYLE__ACCESS": "true",
			},
			Labels:         map[string]string{LabelRun: runID, LabelRole: alias},
			Networks:       []string{net.Name},
			NetworkAliases: map[string][]string{net.Name: {alias}},
			WaitingFor:     wait.ForListeningPort("8181/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		_ = minio.Terminate(ctx)
		return nil, fmt.Errorf("iceberg catalog: %w", err)
	}

	host, port, err := hostPort(ctx, catalog, "8181")
	if err != nil {
		return nil, err
	}
	return &DB{
		Container:   catalog,
		Aux:         []tc.Container{minio},
		Engine:      e,
		DSN:         fmt.Sprintf("http://%s:%s", host, port),
		InternalDSN: fmt.Sprintf("http://%s:8181", alias),
		Props: map[string]string{
			"warehouse":            icebergBucket,
			"s3.endpoint":          "http://" + minioAlias + ":9000",
			"s3.access-key-id":     icebergKey,
			"s3.secret-access-key": icebergSecret,
			"s3.region":            "us-east-1",
		},
	}, nil
}

// Open fails: iceberg has no database/sql handle; parity goes through Count.
func (icebergEngine) Open(db *DB) (*sql.DB, error) {
	return nil, fmt.Errorf("iceberg has no sql handle")
}

// Load fails: iceberg is sink-only and is never seeded.
func (icebergEngine) Load(ctx context.Context, db *DB, t TableDef) (int64, error) {
	return 0, fmt.Errorf("iceberg is sink-only")
}

// Count counts one bench table through duckdb's iceberg extension, attaching
// the REST catalog from the host.
func (icebergEngine) Count(ctx context.Context, db *DB, table string) (int64, error) {
	if len(db.Aux) == 0 {
		return 0, fmt.Errorf("iceberg db has no minio container")
	}
	minioHost, minioPort, err := hostPort(ctx, db.Aux[0], "9000")
	if err != nil {
		return 0, err
	}
	script := fmt.Sprintf(`INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET mc (TYPE s3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s:%s', USE_SSL false, URL_STYLE 'path');
ATTACH '%s' AS ice (TYPE iceberg, ENDPOINT '%s', AUTHORIZATION_TYPE 'none');
SELECT count(*) FROM ice.%s.%s;`,
		icebergKey, icebergSecret, minioHost, minioPort,
		icebergBucket, db.DSN, Namespace, table)
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
