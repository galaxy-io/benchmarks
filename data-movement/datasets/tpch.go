// Package datasets seeds benchmark datasets into source databases.
package datasets

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Table is one seeded table and its row count.
type Table struct {
	Name string
	Rows int64
}

// tpchTables is the load order and postgres DDL for the eight TPC-H tables.
var tpchTables = []struct{ name, ddl string }{
	{"region", `(r_regionkey INT PRIMARY KEY, r_name TEXT, r_comment TEXT)`},
	{"nation", `(n_nationkey INT PRIMARY KEY, n_name TEXT, n_regionkey INT, n_comment TEXT)`},
	{"supplier", `(s_suppkey INT PRIMARY KEY, s_name TEXT, s_address TEXT, s_nationkey INT, s_phone TEXT, s_acctbal NUMERIC, s_comment TEXT)`},
	{"customer", `(c_custkey INT PRIMARY KEY, c_name TEXT, c_address TEXT, c_nationkey INT, c_phone TEXT, c_acctbal NUMERIC, c_mktsegment TEXT, c_comment TEXT)`},
	{"part", `(p_partkey INT PRIMARY KEY, p_name TEXT, p_mfgr TEXT, p_brand TEXT, p_type TEXT, p_size INT, p_container TEXT, p_retailprice NUMERIC, p_comment TEXT)`},
	{"partsupp", `(ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment TEXT, PRIMARY KEY (ps_partkey, ps_suppkey))`},
	{"orders", `(o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus TEXT, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority TEXT, o_clerk TEXT, o_shippriority INT, o_comment TEXT)`},
	{"lineitem", `(l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag TEXT, l_linestatus TEXT, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT, PRIMARY KEY (l_orderkey, l_linenumber))`},
}

// SeedTPCH loads TPC-H at scale factor sf into the postgres database at dsn;
// requires duckdb on PATH.
func SeedTPCH(ctx context.Context, dsn string, sf float64) ([]Table, error) {
	dir, err := os.MkdirTemp("", "tpch")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	var script strings.Builder
	fmt.Fprintf(&script, "INSTALL tpch; LOAD tpch; CALL dbgen(sf = %v);\n", sf)
	for _, t := range tpchTables {
		fmt.Fprintf(&script, "COPY %s TO '%s' (FORMAT csv, HEADER false);\n",
			t.name, filepath.Join(dir, t.name+".csv"))
	}
	if out, err := exec.CommandContext(ctx, "duckdb", "-c", script.String()).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("duckdb dbgen: %w:\n%s", err, out)
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close(ctx) }()

	tables := make([]Table, 0, len(tpchTables))
	for _, t := range tpchTables {
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s; CREATE TABLE %s %s", t.name, t.name, t.ddl)); err != nil {
			return nil, fmt.Errorf("create %s: %w", t.name, err)
		}
		f, err := os.Open(filepath.Join(dir, t.name+".csv"))
		if err != nil {
			return nil, err
		}
		tag, err := conn.PgConn().CopyFrom(ctx, f, fmt.Sprintf("COPY %s FROM STDIN WITH (FORMAT csv)", t.name))
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("copy %s: %w", t.name, err)
		}
		tables = append(tables, Table{Name: t.name, Rows: tag.RowsAffected()})
	}
	return tables, nil
}
