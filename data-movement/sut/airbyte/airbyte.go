// Package airbyte pipes Airbyte source and destination connector images over
// the Airbyte protocol. It omits the server, worker, and Temporal services. A
// driver container uses the host Docker socket to launch both connectors and a
// named volume to share configuration. Destination typing is enabled so records
// are written to final tables.
package airbyte

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/moby/moby/api/types/container"
	tc "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const (
	Image            = "docker:28-cli"
	SourcePGImage    = "airbyte/source-postgres:3.8.1"
	DestPGImage      = "airbyte/destination-postgres:3.0.13"
	SourceMySQLImage = "airbyte/source-mysql:3.53.1"
	DestMySQLImage   = "airbyte/destination-mysql:1.1.1"
)

func connectorMemoryLimit() string {
	if limit := os.Getenv("BENCH_AIRBYTE_CONNECTOR_MEMORY"); limit != "" {
		return limit
	}
	return "16g"
}

// sourceImage picks the source connector image for an engine.
func sourceImage(e harness.Engine) string {
	switch e {
	case harness.Postgres:
		return SourcePGImage
	case harness.MySQL:
		return SourceMySQLImage
	}
	return ""
}

// destImage picks the destination connector image for an engine.
func destImage(e harness.Engine) string {
	switch e {
	case harness.Postgres:
		return DestPGImage
	case harness.MySQL:
		return DestMySQLImage
	}
	return ""
}

// Airbyte pipes the source connector into the destination connector.
type Airbyte struct {
	srcImage  string
	dstImage  string
	container tc.Container
	volume    string
	network   string
	script    string
	sourceCtr string
	destCtr   string
}

// New returns the airbyte tool with the route's connector images pinned.
func New(route string) *Airbyte {
	src, dst, err := harness.ParseRoute(route)
	if err != nil {
		return &Airbyte{}
	}
	return &Airbyte{srcImage: sourceImage(src), dstImage: destImage(dst)}
}

// Name identifies the tool.
func (a *Airbyte) Name() string { return "airbyte" }

// Image reports the images under test.
func (a *Airbyte) Image() string { return a.srcImage + " | " + a.dstImage }

// Routes lists every route; airbyte ships connectors for both engines.
func (a *Airbyte) Routes() []string { return []string{"pg-pg", "pg-mysql", "mysql-mysql", "mysql-pg"} }

// Config reports the sync configuration.
func (a *Airbyte) Config() map[string]any {
	return map[string]any{
		"syncMode":          "full_refresh/overwrite",
		"replicationMethod": "Standard",
		"typeDedupe":        true,
		"connectorMemory":   connectorMemoryLimit(),
	}
}

// Setup starts the orchestrator, writes connector configs to the shared
// volume, pulls both connector images, and discovers the catalog; all of it
// stays outside the timed window.
func (a *Airbyte) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	srcCfg, err := sourceConfig(env.Source)
	if err != nil {
		return fmt.Errorf("source config: %w", err)
	}
	dstCfg, err := destConfig(env.Sink)
	if err != nil {
		return fmt.Errorf("destination config: %w", err)
	}

	a.volume = env.RunID + "-airbyte"
	a.network = env.Net.Name
	memoryLimit := connectorMemoryLimit()
	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:  Image,
			Cmd:    []string{"sleep", "infinity"},
			Labels: map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "airbyte"},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.Binds = append(hc.Binds,
					"/var/run/docker.sock:/var/run/docker.sock",
					a.volume+":/secrets")
			},
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("airbyte container: %w", err)
	}
	a.container = c

	for name, cfg := range map[string][]byte{"source.json": srcCfg, "destination.json": dstCfg} {
		if err := c.CopyToContainer(ctx, cfg, "/secrets/"+name, 0o644); err != nil {
			return fmt.Errorf("copy %s: %w", name, err)
		}
	}
	for _, img := range []string{a.srcImage, a.dstImage} {
		code, out, err := a.exec(ctx, []string{"docker", "pull", "-q", img})
		if err != nil {
			return fmt.Errorf("pull %s: %w", img, err)
		}
		if code != 0 {
			return fmt.Errorf("pull %s exited %d:\n%s", img, code, out)
		}
	}

	catalog, err := a.discover(ctx, tables)
	if err != nil {
		return err
	}
	if err := c.CopyToContainer(ctx, catalog, "/secrets/catalog.json", 0o644); err != nil {
		return fmt.Errorf("copy catalog: %w", err)
	}

	// Create both job containers during setup. The timed window starts their
	// connector processes, but does not include Docker create/pull overhead.
	a.sourceCtr = env.RunID + "-airbyte-source"
	a.destCtr = env.RunID + "-airbyte-destination"
	connector := func(name, image, role, verb, cfg string, interactive bool) error {
		args := []string{"docker", "create", "--name", name, "--network", env.Net.Name,
			"-m", memoryLimit, "-v", a.volume + ":/secrets",
			"-l", harness.LabelRun + "=" + env.RunID, "-l", harness.LabelRole + "=" + role}
		if interactive {
			args = append(args, "-i")
		}
		args = append(args, image, verb, "--config", "/secrets/"+cfg+".json", "--catalog", "/secrets/catalog.json")
		code, out, err := a.exec(ctx, args)
		if err != nil {
			return err
		}
		if code != 0 {
			return fmt.Errorf("create %s exited %d:\n%s", role, code, out)
		}
		return nil
	}
	if err := connector(a.sourceCtr, a.srcImage, "airbyte-source", "read", "source", false); err != nil {
		return err
	}
	if err := connector(a.destCtr, a.dstImage, "airbyte-destination", "write", "destination", true); err != nil {
		return err
	}
	// The platform's worker forwards only RECORD and STATE to the
	// destination; it rejects LOG and TRACE, so the pipe filters the same way.
	a.script = "set -o pipefail\n" +
		"docker start -a " + a.sourceCtr +
		` | grep -E '"type":"(RECORD|STATE)"' | ` +
		"docker start -a -i " + a.destCtr
	return nil
}

// Teardown removes the two job containers, orchestrator, and shared volume.
func (a *Airbyte) Teardown(ctx context.Context) {
	if a.container == nil {
		return
	}
	for _, name := range []string{a.sourceCtr, a.destCtr} {
		if name != "" {
			_, _, _ = a.exec(ctx, []string{"docker", "rm", "-f", name})
		}
	}
	_ = a.container.Terminate(ctx, tc.RemoveVolumes(a.volume))
}

// Run executes the connector pipe and blocks, erroring on a non-zero exit.
func (a *Airbyte) Run(ctx context.Context) error {
	code, out, err := a.exec(ctx, []string{"sh", "-c", a.script})
	if err != nil {
		return fmt.Errorf("airbyte run: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("airbyte exited %d:\n%s", code, tail(out))
	}
	return nil
}

// discover runs the source's discover command and returns the configured
// catalog for the seeded tables: full refresh, overwrite, one generation.
func (a *Airbyte) discover(ctx context.Context, tables []string) ([]byte, error) {
	cmd := fmt.Sprintf("docker run --rm --network %s -v %s:/secrets %s discover --config /secrets/source.json", a.network, a.volume, a.srcImage)
	code, out, err := a.exec(ctx, []string{"sh", "-c", cmd})
	if err != nil {
		return nil, fmt.Errorf("discover: %w", err)
	}
	if code != 0 {
		return nil, fmt.Errorf("discover exited %d:\n%s", code, tail(out))
	}

	streams := map[string]json.RawMessage{}
	sc := bufio.NewScanner(strings.NewReader(out))
	sc.Buffer(make([]byte, 0, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var msg struct {
			Type    string `json:"type"`
			Catalog struct {
				Streams []json.RawMessage `json:"streams"`
			} `json:"catalog"`
		}
		if json.Unmarshal(sc.Bytes(), &msg) != nil || msg.Type != "CATALOG" {
			continue
		}
		for _, raw := range msg.Catalog.Streams {
			var s struct {
				Name string `json:"name"`
			}
			if json.Unmarshal(raw, &s) == nil {
				streams[s.Name] = raw
			}
		}
	}

	type configured struct {
		Stream              json.RawMessage `json:"stream"`
		SyncMode            string          `json:"sync_mode"`
		DestinationSyncMode string          `json:"destination_sync_mode"`
		GenerationID        int64           `json:"generation_id"`
		MinimumGenerationID int64           `json:"minimum_generation_id"`
		SyncID              int64           `json:"sync_id"`
	}
	catalog := struct {
		Streams []configured `json:"streams"`
	}{}
	for _, t := range tables {
		raw, ok := streams[t]
		if !ok {
			return nil, fmt.Errorf("discover found no stream for table %q", t)
		}
		catalog.Streams = append(catalog.Streams, configured{
			Stream:              raw,
			SyncMode:            "full_refresh",
			DestinationSyncMode: "overwrite",
			GenerationID:        1,
			MinimumGenerationID: 1,
			SyncID:              1,
		})
	}
	return json.Marshal(catalog)
}

// sourceConfig renders the source connector's config JSON for db's engine.
func sourceConfig(db *harness.DB) ([]byte, error) {
	host, port, name, user, pass, err := splitDSN(db.InternalDSN)
	if err != nil {
		return nil, err
	}
	switch db.Engine {
	case harness.Postgres:
		return json.Marshal(map[string]any{
			"host":               host,
			"port":               port,
			"database":           name,
			"schemas":            []string{harness.Namespace},
			"username":           user,
			"password":           pass,
			"ssl_mode":           map[string]any{"mode": sslMode(db.InternalDSN)},
			"tunnel_method":      map[string]any{"tunnel_method": "NO_TUNNEL"},
			"replication_method": map[string]any{"method": "Standard"},
		})
	case harness.MySQL:
		ssl := mysqlTLS(db.InternalDSN)
		return json.Marshal(map[string]any{
			"host":               host,
			"port":               port,
			"database":           name,
			"username":           user,
			"password":           pass,
			"ssl_mode":           map[string]any{"mode": map[bool]string{true: "required", false: "disabled"}[ssl]},
			"tunnel_method":      map[string]any{"tunnel_method": "NO_TUNNEL"},
			"replication_method": map[string]any{"method": "STANDARD"},
		})
	}
	return nil, fmt.Errorf("no source config for engine %v", db.Engine)
}

// destConfig renders the destination connector's config JSON for db's engine.
func destConfig(db *harness.DB) ([]byte, error) {
	host, port, name, user, pass, err := splitDSN(db.InternalDSN)
	if err != nil {
		return nil, err
	}
	switch db.Engine {
	case harness.Postgres:
		return json.Marshal(map[string]any{
			"host":          host,
			"port":          port,
			"database":      name,
			"schema":        harness.Namespace,
			"username":      user,
			"password":      pass,
			"ssl_mode":      map[string]any{"mode": sslMode(db.InternalDSN)},
			"tunnel_method": map[string]any{"tunnel_method": "NO_TUNNEL"},
		})
	case harness.MySQL:
		return json.Marshal(map[string]any{
			"host":          host,
			"port":          port,
			"database":      name,
			"username":      user,
			"password":      pass,
			"ssl":           mysqlTLS(db.InternalDSN),
			"tunnel_method": map[string]any{"tunnel_method": "NO_TUNNEL"},
		})
	}
	return nil, fmt.Errorf("no destination config for engine %v", db.Engine)
}

// splitDSN breaks a database URL into the parts the connector configs need.
// sslMode reads the postgres dsn's sslmode, which the connector names the same
// way libpq does. A container serves plaintext and rds refuses it, so the mode
// has to follow the dsn rather than be assumed.
func sslMode(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "disable"
	}
	if m := u.Query().Get("sslmode"); m != "" {
		return m
	}
	return "disable"
}

func splitDSN(dsn string) (host string, port int, db, user, pass string, err error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", 0, "", "", "", err
	}
	portText := u.Port()
	if portText == "" {
		switch u.Scheme {
		case "postgres", "postgresql":
			portText = "5432"
		case "mysql":
			portText = "3306"
		}
	}
	port, err = strconv.Atoi(portText)
	if err != nil {
		return "", 0, "", "", "", fmt.Errorf("port in %q: %w", u.Host, err)
	}
	pass, _ = u.User.Password()
	return u.Hostname(), port, strings.TrimPrefix(u.Path, "/"), u.User.Username(), pass, nil
}

func mysqlTLS(dsn string) bool {
	u, err := url.Parse(dsn)
	if err != nil {
		return false
	}
	tls := u.Query().Get("tls")
	return tls != "" && tls != "false"
}

// exec runs cmd in the orchestrator, returning its exit code and full output.
func (a *Airbyte) exec(ctx context.Context, cmd []string) (int, string, error) {
	code, r, err := a.container.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		return 0, "", err
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return code, "(output unavailable)", nil
	}
	return code, string(b), nil
}

// tail keeps the last stretch of output for error messages.
func tail(s string) string {
	if len(s) > 4000 {
		return s[len(s)-4000:]
	}
	return s
}
