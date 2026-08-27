package harness

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-sql-driver/mysql"
)

const MySQLImage = "mysql:8.4"

type mysqlEngine struct{}

// Name identifies the engine.
func (mysqlEngine) Name() string { return "mysql" }

// Open opens a database/sql handle to db from the host.
func (mysqlEngine) Open(db *DB) (*sql.DB, error) {
	dsn, err := MySQLDSN(db.DSN)
	if err != nil {
		return nil, err
	}
	return sql.Open("mysql", dsn)
}

// Count counts one bench table.
func (mysqlEngine) Count(ctx context.Context, db *DB, table string) (int64, error) {
	return sqlCount(ctx, db, table)
}

// Load creates t in the bench database and loads its CSV with
// LOAD DATA LOCAL INFILE.
func (e mysqlEngine) Load(ctx context.Context, db *DB, t TableDef) (int64, error) {
	name, err := qualifiedTable(t.Name)
	if err != nil {
		return 0, err
	}
	conn, err := e.Open(db)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+name); err != nil {
		return 0, fmt.Errorf("drop %s: %w", t.Name, err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s %s", name, t.DDL)); err != nil {
		return 0, fmt.Errorf("create %s: %w", t.Name, err)
	}
	mysql.RegisterLocalFile(t.CSV)
	defer mysql.DeregisterLocalFile(t.CSV)
	csv := strings.ReplaceAll(t.CSV, "'", "''")
	res, err := conn.ExecContext(ctx, fmt.Sprintf(
		`LOAD DATA LOCAL INFILE '%s' INTO TABLE %s
		 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY ''
		 LINES TERMINATED BY '\n'`, csv, name))
	if err != nil {
		return 0, fmt.Errorf("load %s: %w", t.Name, err)
	}
	// InnoDB needs no post-load pass for readers, but fresh statistics keep
	// the seeded table in the same state on every engine.
	if _, err := conn.ExecContext(ctx, "ANALYZE TABLE "+name); err != nil {
		return 0, fmt.Errorf("analyze %s: %w", t.Name, err)
	}
	return res.RowsAffected()
}

// MySQLDSN converts a mysql:// URL into the go-sql-driver DSN form.
func MySQLDSN(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse mysql DSN: %w", err)
	}
	if u.Scheme != "mysql" {
		return "", fmt.Errorf("mysql DSN has scheme %q, want mysql", u.Scheme)
	}
	if u.User == nil || u.User.Username() == "" {
		return "", fmt.Errorf("mysql DSN has no user")
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("mysql DSN has no host")
	}
	if strings.TrimPrefix(u.Path, "/") == "" {
		return "", fmt.Errorf("mysql DSN has no database")
	}
	cfg := mysql.NewConfig()
	cfg.User = u.User.Username()
	cfg.Passwd, _ = u.User.Password()
	cfg.Net = "tcp"
	cfg.Addr = u.Host
	cfg.DBName = strings.TrimPrefix(u.Path, "/")
	query := u.Query()
	if tls := query.Get("tls"); tls != "" {
		cfg.TLSConfig = tls
		query.Del("tls")
	}
	if len(query) > 0 {
		cfg.Params = make(map[string]string, len(query))
		for key, values := range query {
			if len(values) > 0 {
				cfg.Params[key] = values[len(values)-1]
			}
		}
	}
	return cfg.FormatDSN(), nil
}
