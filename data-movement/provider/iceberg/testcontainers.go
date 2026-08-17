// Package iceberg provisions the benchmark's Iceberg sink.
package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
	"github.com/galaxy-io/benchmarks/data-movement/provider/testcontainers"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Testcontainers runs MinIO and a REST catalog as containers on the run's
// network.
type Testcontainers struct{}

// Provision starts the object store and the catalog in front of it. The two
// are torn down together, and the props carry what a writer needs to address
// the store.
func (Testcontainers) Provision(ctx context.Context, spec harness.ProvisionSpec) (*harness.DB, error) {
	minioAlias := spec.Role + "-minio"
	minio, err := testcontainers.Start(ctx, spec, minioAlias, tc.ContainerRequest{
		Image:        harness.MinIOImage,
		Cmd:          []string{"server", "/data"},
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     harness.IcebergKey,
			"MINIO_ROOT_PASSWORD": harness.IcebergSecret,
		},
		WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(60 * time.Second),
	})
	if err != nil {
		return nil, fmt.Errorf("minio: %w", err)
	}
	// MinIO's filesystem backend surfaces a top-level directory as a bucket.
	if code, _, err := minio.Exec(ctx, []string{"mkdir", "-p", "/data/" + harness.IcebergBucket}); err != nil || code != 0 {
		_ = minio.Terminate(ctx)
		return nil, fmt.Errorf("create bucket (exit %d): %w", code, err)
	}

	catalog, err := testcontainers.Start(ctx, spec, spec.Role, tc.ContainerRequest{
		Image:        harness.IcebergCatalogImage,
		ExposedPorts: []string{"8181/tcp"},
		Env: map[string]string{
			"AWS_ACCESS_KEY_ID":              harness.IcebergKey,
			"AWS_SECRET_ACCESS_KEY":          harness.IcebergSecret,
			"AWS_REGION":                     harness.IcebergRegion,
			"CATALOG_WAREHOUSE":              "s3://" + harness.IcebergBucket + "/",
			"CATALOG_IO__IMPL":               "org.apache.iceberg.aws.s3.S3FileIO",
			"CATALOG_S3_ENDPOINT":            "http://" + minioAlias + ":9000",
			"CATALOG_S3_PATH__STYLE__ACCESS": "true",
		},
		WaitingFor: wait.ForListeningPort("8181/tcp").WithStartupTimeout(60 * time.Second),
	})
	if err != nil {
		_ = minio.Terminate(ctx)
		return nil, fmt.Errorf("iceberg catalog: %w", err)
	}
	closeBoth := func(ctx context.Context) error {
		err := catalog.Terminate(ctx)
		if minioErr := minio.Terminate(ctx); err == nil {
			err = minioErr
		}
		return err
	}

	host, port, err := testcontainers.HostPort(ctx, catalog, "8181")
	if err != nil {
		_ = closeBoth(ctx)
		return nil, err
	}
	minioHost, minioPort, err := testcontainers.HostPort(ctx, minio, "9000")
	if err != nil {
		_ = closeBoth(ctx)
		return nil, err
	}
	dsn := fmt.Sprintf("http://%s:%s", host, port)
	if err := harness.CreateIcebergNamespace(ctx, dsn); err != nil {
		_ = closeBoth(ctx)
		return nil, err
	}
	return &harness.DB{
		Engine:      spec.Engine,
		Role:        spec.Role,
		DSN:         dsn,
		InternalDSN: fmt.Sprintf("http://%s:8181", spec.Role),
		Props: map[string]string{
			harness.PropWarehouse:      harness.IcebergBucket,
			harness.PropS3Endpoint:     "http://" + minioAlias + ":9000",
			harness.PropS3HostEndpoint: fmt.Sprintf("%s:%s", minioHost, minioPort),
			harness.PropS3Key:          harness.IcebergKey,
			harness.PropS3Secret:       harness.IcebergSecret,
			harness.PropS3Region:       harness.IcebergRegion,
			harness.PropS3UseSSL:       "false",
			harness.PropS3PathStyle:    "true",
		},
		Close: closeBoth,
	}, nil
}
