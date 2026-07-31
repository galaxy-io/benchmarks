package harness

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const MySQLImage = "mysql:8.4"

type mysqlEngine struct{}

// Name identifies the engine.
func (mysqlEngine) Name() string { return "mysql" }

// Start runs a mysql container on net; the alias doubles as the sampler role.
// The bench user gets global grants so tools can create their own working
// databases, and local_infile is on for LOAD DATA seeding.
func (e mysqlEngine) Start(ctx context.Context, net *tc.DockerNetwork, alias, runID string) (*DB, error) {
	grants := "GRANT ALL PRIVILEGES ON *.* TO 'bench'@'%'; FLUSH PRIVILEGES;\n"
	req := tc.ContainerRequest{
		Image: MySQLImage,
		// Sized for the benchmark machine; stock 128MB punishes non-sequential writers.
		Cmd:          []string{"--local-infile=ON", "--innodb-buffer-pool-size=8G", "--innodb-redo-log-capacity=2G"},
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "bench",
			"MYSQL_USER":          "bench",
			"MYSQL_PASSWORD":      "bench",
			"MYSQL_DATABASE":      "bench",
		},
		Files: []tc.ContainerFile{{
			Reader:            strings.NewReader(grants),
			ContainerFilePath: "/docker-entrypoint-initdb.d/grants.sql",
			FileMode:          0o644,
		}},
		Labels:         map[string]string{LabelRun: runID, LabelRole: alias},
		Networks:       []string{net.Name},
		NetworkAliases: map[string][]string{net.Name: {alias}},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").
			WithStartupTimeout(120 * time.Second),
	}
	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, err
	}
	host, port, err := hostPort(ctx, c, "3306")
	if err != nil {
		return nil, err
	}
	return &DB{
		Container:   c,
		Engine:      e,
		DSN:         fmt.Sprintf("mysql://bench:bench@%s:%s/bench", host, port),
		InternalDSN: fmt.Sprintf("mysql://bench:bench@%s:3306/bench", alias),
	}, nil
}

// Open opens a database/sql handle to db from the host.
func (mysqlEngine) Open(db *DB) (*sql.DB, error) {
	dsn, err := MySQLDSN(db.DSN)
	if err != nil {
		return nil, err
	}
	return sql.Open("mysql", dsn)
}

// Load creates t in the bench database and loads its csv with
// LOAD DATA LOCAL INFILE, the fastest client-side path mysql offers.
func (e mysqlEngine) Load(ctx context.Context, db *DB, t TableDef) (int64, error) {
	conn, err := e.Open(db)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	name := Namespace + "." + t.Name
	if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+name); err != nil {
		return 0, fmt.Errorf("drop %s: %w", t.Name, err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s %s", name, t.DDL)); err != nil {
		return 0, fmt.Errorf("create %s: %w", t.Name, err)
	}
	mysql.RegisterLocalFile(t.CSV)
	defer mysql.DeregisterLocalFile(t.CSV)
	res, err := conn.ExecContext(ctx, fmt.Sprintf(
		`LOAD DATA LOCAL INFILE '%s' INTO TABLE %s
		 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY ''
		 LINES TERMINATED BY '\n'`, t.CSV, name))
	if err != nil {
		return 0, fmt.Errorf("load %s: %w", t.Name, err)
	}
	return res.RowsAffected()
}

// MySQLDSN converts a mysql:// URL into the go-sql-driver DSN form.
func MySQLDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	cfg := mysql.NewConfig()
	cfg.User = u.User.Username()
	cfg.Passwd, _ = u.User.Password()
	cfg.Net = "tcp"
	cfg.Addr = u.Host
	cfg.DBName = strings.TrimPrefix(u.Path, "/")
	return cfg.FormatDSN(), nil
}
