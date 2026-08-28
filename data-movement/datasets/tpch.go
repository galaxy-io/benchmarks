// Package datasets seeds benchmark datasets into source databases.
package datasets

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

// Table is one seeded table and its row count.
type Table struct {
	Name string
	Rows int64
}

// tpchTables is the load order and DDL for the eight TPC-H tables. The DDL
// stays valid on both engines; NUMERIC carries a precision because mysql
// reads a bare NUMERIC as DECIMAL(10,0) and truncates.
var tpchTables = []struct{ name, ddl string }{
	{"region", `(r_regionkey INT PRIMARY KEY, r_name TEXT, r_comment TEXT)`},
	{"nation", `(n_nationkey INT PRIMARY KEY, n_name TEXT, n_regionkey INT, n_comment TEXT)`},
	{"supplier", `(s_suppkey INT PRIMARY KEY, s_name TEXT, s_address TEXT, s_nationkey INT, s_phone TEXT, s_acctbal NUMERIC(15,2), s_comment TEXT)`},
	{"customer", `(c_custkey INT PRIMARY KEY, c_name TEXT, c_address TEXT, c_nationkey INT, c_phone TEXT, c_acctbal NUMERIC(15,2), c_mktsegment TEXT, c_comment TEXT)`},
	{"part", `(p_partkey INT PRIMARY KEY, p_name TEXT, p_mfgr TEXT, p_brand TEXT, p_type TEXT, p_size INT, p_container TEXT, p_retailprice NUMERIC(15,2), p_comment TEXT)`},
	{"partsupp", `(ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC(15,2), ps_comment TEXT, PRIMARY KEY (ps_partkey, ps_suppkey))`},
	{"orders", `(o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus TEXT, o_totalprice NUMERIC(15,2), o_orderdate DATE, o_orderpriority TEXT, o_clerk TEXT, o_shippriority INT, o_comment TEXT)`},
	{"lineitem", `(l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC(15,2), l_extendedprice NUMERIC(15,2), l_discount NUMERIC(15,2), l_tax NUMERIC(15,2), l_returnflag TEXT, l_linestatus TEXT, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT, PRIMARY KEY (l_orderkey, l_linenumber))`},
}

// SeedTPCH loads TPC-H at scale factor sf into db's bench namespace;
// requires duckdb on PATH.
func SeedTPCH(ctx context.Context, db *harness.DB, sf float64) ([]Table, error) {
	if sf <= 0 || math.IsNaN(sf) || math.IsInf(sf, 0) {
		return nil, fmt.Errorf("TPC-H scale factor must be finite and positive, got %v", sf)
	}
	dir, err := os.MkdirTemp("", "tpch")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	var script strings.Builder
	fmt.Fprintf(&script, "INSTALL tpch; LOAD tpch; CALL dbgen(sf = %v);\n", sf)
	for _, t := range tpchTables {
		fmt.Fprintf(&script, "COPY %s TO %s (FORMAT csv, HEADER false);\n",
			t.name, duckDBString(filepath.Join(dir, t.name+".csv")))
	}
	if out, err := exec.CommandContext(ctx, "duckdb", "-c", script.String()).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("duckdb dbgen: %w:\n%s", err, out)
	}

	tables := make([]Table, 0, len(tpchTables))
	for _, t := range tpchTables {
		def := harness.TableDef{Name: t.name, DDL: t.ddl, CSV: filepath.Join(dir, t.name+".csv")}
		rows, err := db.Engine.Load(ctx, db, def)
		if err != nil {
			return nil, err
		}
		tables = append(tables, Table{Name: t.name, Rows: rows})
	}
	return tables, nil
}
