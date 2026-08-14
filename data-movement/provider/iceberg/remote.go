package iceberg

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
	"github.com/galaxy-io/benchmarks/data-movement/provider/testcontainers"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Remote writes the warehouse to an S3 bucket and runs the REST catalog in a
// container on the benchmark host. Every Iceberg adapter uses that catalog.
type Remote struct {
	// Bucket is the warehouse bucket and optional prefix, with or without s3://.
	Bucket string
	Region string
	Key    string
	Secret string
	Token  string
}

// RemoteFromEnv reads the warehouse from the DSN and explicit credentials from
// the environment. Without explicit credentials, each container's AWS SDK uses
// its default chain and obtains refreshable credentials from the host's instance
// role.
func RemoteFromEnv(dsn string) (Remote, error) {
	r := Remote{
		Bucket: strings.TrimSuffix(strings.TrimPrefix(dsn, "s3://"), "/"),
		Region: os.Getenv("AWS_REGION"),
		Key:    os.Getenv("AWS_ACCESS_KEY_ID"),
		Secret: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		Token:  os.Getenv("AWS_SESSION_TOKEN"),
	}
	if r.Bucket == "" {
		return r, fmt.Errorf("no bucket in iceberg dsn %q", dsn)
	}
	if r.Region == "" {
		return r, fmt.Errorf("iceberg on s3 needs AWS_REGION")
	}
	// Half a static credential is a typo, not a request for the default chain,
	// and a session token alone is the same mistake one field further on.
	if (r.Key == "") != (r.Secret == "") {
		return r, fmt.Errorf("iceberg on s3 needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY together, or neither")
	}
	if r.Key == "" && r.Token != "" {
		return r, fmt.Errorf("iceberg on s3 has AWS_SESSION_TOKEN without a key and secret; unset it to use the instance role")
	}
	return r, nil
}

// Static reports whether the run carries explicit credentials rather than
// leaving each client to its own default chain.
func (r Remote) Static() bool { return r.Key != "" }

// Provision starts the catalog in front of the bucket. Teardown stops the
// catalog and leaves the warehouse: emptying a bucket is the bucket owner's
// call, not a benchmark's.
func (r Remote) Provision(ctx context.Context, spec harness.ProvisionSpec) (*harness.DB, error) {
	warehouse := r.Bucket + "/" + spec.RunID
	// An empty credential variable is not the same as an absent one: the SDK
	// reads it as a set-but-blank key and stops before the instance role.
	env := map[string]string{
		"AWS_REGION":        r.Region,
		"CATALOG_WAREHOUSE": "s3://" + warehouse + "/",
		"CATALOG_IO__IMPL":  "org.apache.iceberg.aws.s3.S3FileIO",
	}
	if r.Static() {
		env["AWS_ACCESS_KEY_ID"] = r.Key
		env["AWS_SECRET_ACCESS_KEY"] = r.Secret
		if r.Token != "" {
			env["AWS_SESSION_TOKEN"] = r.Token
		}
	}
	catalog, err := testcontainers.Start(ctx, spec, spec.Role, tc.ContainerRequest{
		Image:        harness.IcebergCatalogImage,
		ExposedPorts: []string{"8181/tcp"},
		Env:          env,
		WaitingFor:   wait.ForListeningPort("8181/tcp").WithStartupTimeout(60 * time.Second),
	})
	if err != nil {
		return nil, fmt.Errorf("iceberg catalog: %w", err)
	}
	host, port, err := testcontainers.HostPort(ctx, catalog, "8181")
	if err != nil {
		_ = catalog.Terminate(ctx)
		return nil, err
	}
	dsn := fmt.Sprintf("http://%s:%s", host, port)
	if err := harness.CreateIcebergNamespace(ctx, dsn); err != nil {
		_ = catalog.Terminate(ctx)
		return nil, err
	}
	props := map[string]string{
		harness.PropWarehouse:   warehouse,
		harness.PropS3Region:    r.Region,
		harness.PropS3UseSSL:    "true",
		harness.PropS3PathStyle: "false",
	}
	if r.Static() {
		props[harness.PropS3Key] = r.Key
		props[harness.PropS3Secret] = r.Secret
		if r.Token != "" {
			props[harness.PropS3Token] = r.Token
		}
	}
	db := &harness.DB{
		Engine:      spec.Engine,
		Role:        spec.Role,
		DSN:         dsn,
		InternalDSN: fmt.Sprintf("http://%s:8181", spec.Role),
		// No endpoint props: S3 is where every client looks by default, and
		// virtual-host addressing over TLS is how it answers. Credential props
		// stay absent unless they were given, so each adapter leaves the
		// credential fields out of its config and its SDK finds the role.
		Props: props,
		Close: testcontainers.Terminate(catalog),
	}
	// Creating a namespace only touches the catalog, so a broken policy, a
	// missing endpoint route, or an unreachable instance role would otherwise
	// surface as a failed write in the middle of a timed run.
	if err := harness.VerifyIcebergStore(ctx, db); err != nil {
		_ = catalog.Terminate(ctx)
		return nil, err
	}
	return db, nil
}
