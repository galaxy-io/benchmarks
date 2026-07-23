// Package filament runs filament's standalone image (one container: embedded
// NATS, API, engine, in-memory store) and drives it over its Connect API.
// DSNs reach the process as environment secrets resolved by secret refs.
package filament

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const Image = "galaxy-io/filament-standalone:latest"

// Options is the run configuration, recorded in the result document.
var Options = map[string]any{
	"batchMaxRows":        25000,
	"snapshotParallelism": 4,
}

// Filament runs the standalone image and drives one pipeline through it.
type Filament struct {
	serverURL  string
	pipelineID string
	container  tc.Container
}

// New returns the filament tool.
func New() *Filament { return &Filament{} }

// Name identifies the tool.
func (f *Filament) Name() string { return "filament" }

// Image reports the image under test.
func (f *Filament) Image() string { return Image }

// Config reports the run options.
func (f *Filament) Config() map[string]any { return Options }

// Setup starts the container on the env's network and registers a pipeline
// with one snapshot-replace edge per table.
func (f *Filament) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image: Image,
			Env: map[string]string{
				"SOURCE_DSN": env.Source.InternalDSN,
				"SINK_DSN":   env.Sink.InternalDSN,
			},
			ExposedPorts: []string{"8080/tcp"},
			Labels:       map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "filament"},
			Networks:     []string{env.Net.Name},
			WaitingFor:   wait.ForListeningPort("8080/tcp").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("filament standalone: %w", err)
	}
	f.container = c

	host, err := c.Host(ctx)
	if err != nil {
		return err
	}
	port, err := c.MappedPort(ctx, "8080")
	if err != nil {
		return err
	}
	f.serverURL = fmt.Sprintf("http://%s:%s", host, port.Port())

	f.pipelineID, err = f.createPipeline(ctx, tables)
	return err
}

// Teardown stops the container.
func (f *Filament) Teardown(ctx context.Context) {
	if f.container != nil {
		_ = f.container.Terminate(ctx)
	}
}

// Run executes the pipeline and blocks until every per-table run is terminal,
// erroring if any failed.
func (f *Filament) Run(ctx context.Context) error {
	req := map[string]any{"pipelineId": f.pipelineID, "options": Options}
	var started struct {
		Runs []struct {
			RunID string `json:"runId"`
		} `json:"runs"`
	}
	if err := f.call(ctx, "RunPipeline", req, &started); err != nil {
		return fmt.Errorf("run pipeline: %w", err)
	}
	if len(started.Runs) == 0 {
		return fmt.Errorf("run pipeline: no runs returned")
	}

	pending := make(map[string]bool, len(started.Runs))
	for _, r := range started.Runs {
		pending[r.RunID] = true
	}
	for len(pending) > 0 {
		for id := range pending {
			var resp struct {
				Snapshot struct {
					Run struct {
						Status string `json:"status"`
						Error  string `json:"error"`
					} `json:"run"`
				} `json:"snapshot"`
			}
			if err := f.call(ctx, "GetRun", map[string]any{"runId": id}, &resp); err != nil {
				return err
			}
			switch resp.Snapshot.Run.Status {
			case "RUN_STATUS_COMPLETED":
				delete(pending, id)
			case "RUN_STATUS_FAILED", "RUN_STATUS_CANCELED", "RUN_STATUS_PARTIAL", "RUN_STATUS_PAUSED":
				return fmt.Errorf("run %s: %s: %s", id, resp.Snapshot.Run.Status, resp.Snapshot.Run.Error)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return nil
}

// createPipeline registers both connections and the pipeline, returning its id.
func (f *Filament) createPipeline(ctx context.Context, tables []string) (string, error) {
	srcID, err := f.createConnection(ctx, "CONNECTOR_KIND_SOURCE", "bench-source", "source/dsn")
	if err != nil {
		return "", err
	}
	dstID, err := f.createConnection(ctx, "CONNECTOR_KIND_SINK", "bench-sink", "sink/dsn")
	if err != nil {
		return "", err
	}

	edges := make([]map[string]any, 0, len(tables))
	for _, t := range tables {
		edges = append(edges, map[string]any{
			"fromNode":      "src",
			"resource":      t,
			"toNode":        "dst",
			"ingestionType": "INGESTION_TYPE_SNAPSHOT_REPLACE",
		})
	}
	var resp struct {
		Pipeline struct {
			ID string `json:"id"`
		} `json:"pipeline"`
	}
	err = f.call(ctx, "CreatePipeline", map[string]any{
		"tenant": "t1",
		"name":   "bench",
		"nodes": []map[string]any{
			{"id": "src", "kind": "CONNECTOR_KIND_SOURCE", "connectionId": srcID, "config": map[string]any{"schema": "public"}},
			{"id": "dst", "kind": "CONNECTOR_KIND_SINK", "connectionId": dstID, "config": map[string]any{"schema": "public"}},
		},
		"edges": edges,
	}, &resp)
	if err != nil {
		return "", fmt.Errorf("create pipeline: %w", err)
	}
	return resp.Pipeline.ID, nil
}

// createConnection registers a connection whose dsn resolves from ref in the container env.
func (f *Filament) createConnection(ctx context.Context, kind, name, ref string) (string, error) {
	var resp struct {
		Connection struct {
			ID string `json:"id"`
		} `json:"connection"`
	}
	err := f.call(ctx, "CreateConnection", map[string]any{
		"tenant":     "t1",
		"kind":       kind,
		"name":       name,
		"connector":  "postgres",
		"secretRefs": map[string]string{"dsn": ref},
	}, &resp)
	if err != nil {
		return "", fmt.Errorf("create connection %s: %w", name, err)
	}
	return resp.Connection.ID, nil
}

// call POSTs one Connect JSON request and decodes the response into out.
func (f *Filament) call(ctx context.Context, method string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	url := f.serverURL + "/ingestion.v1.IngestionService/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s: %s", method, resp.Status, raw)
	}
	return json.Unmarshal(raw, out)
}
