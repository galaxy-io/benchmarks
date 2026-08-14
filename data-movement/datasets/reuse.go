package datasets

import (
	"context"
	"fmt"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

// markerTable records which dataset a source already holds. A full load only
// reads the source, so one seed serves every SUT and route in a sweep; the
// label guards against reusing a seed built with different parameters.
const markerTable = "_seed"

// MarkSeed records label as the dataset now loaded in db.
func MarkSeed(ctx context.Context, db *harness.DB, label string) error {
	conn, err := db.Open()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	name := harness.Namespace + "." + markerTable
	for _, q := range []string{
		"DROP TABLE IF EXISTS " + name,
		fmt.Sprintf("CREATE TABLE %s (label VARCHAR(255))", name),
		fmt.Sprintf("INSERT INTO %s (label) VALUES ('%s')", name, label),
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("mark seed: %w", err)
		}
	}
	return nil
}

// LoadedSeed reports the label db was last seeded with, empty if none.
func LoadedSeed(ctx context.Context, db *harness.DB) string {
	conn, err := db.Open()
	if err != nil {
		return ""
	}
	defer func() { _ = conn.Close() }()
	var label string
	q := fmt.Sprintf("SELECT label FROM %s.%s", harness.Namespace, markerTable)
	if err := conn.QueryRowContext(ctx, q).Scan(&label); err != nil {
		return ""
	}
	return label
}

// CountSeeded counts the dataset's tables in place, for a source that already
// holds the wanted seed.
func CountSeeded(ctx context.Context, db *harness.DB, names []string) ([]Table, error) {
	out := make([]Table, 0, len(names))
	for _, name := range names {
		n, err := db.Engine.Count(ctx, db, name)
		if err != nil {
			return nil, fmt.Errorf("count seeded %s: %w", name, err)
		}
		out = append(out, Table{Name: name, Rows: n})
	}
	return out, nil
}

// TPCHTableNames lists the tables SeedTPCH loads.
func TPCHTableNames() []string {
	names := make([]string, 0, len(tpchTables))
	for _, t := range tpchTables {
		names = append(names, t.name)
	}
	return names
}

// TaxiTableNames lists the tables SeedTaxi loads.
func TaxiTableNames() []string { return []string{"trips", "fhv_trips"} }
