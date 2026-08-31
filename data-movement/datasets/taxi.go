package datasets

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

// The nyc-taxi dataset covers published NYC TLC records from January 2009
// through December 2024. The trips table contains yellow-taxi records. The
// fhv_trips table contains FHV and high-volume FHV records. The full dataset is
// about 4 billion rows. Each table projects its Parquet schema eras onto one
// layout. Months count backward from taxiEnd, and each era clips to its file
// range.
var taxiEnd = month{2024, 12}

// taxiMaxMonths spans January 2009 through December 2024.
const taxiMaxMonths = 192

// taxiDDL is the yellow-taxi trips table for both SQL engines. The source files
// have no primary key, so the benchmark adds a synthetic id. Rows from the
// latitude/longitude era (2009-2016) use zero for location ids.
const taxiDDL = `(id BIGINT PRIMARY KEY, vendor_id INT, pickup_at TIMESTAMP, dropoff_at TIMESTAMP,
 passenger_count INT, trip_distance NUMERIC(15,2), ratecode_id INT, store_and_fwd_flag TEXT,
 pu_location_id INT, do_location_id INT, payment_type INT, fare_amount NUMERIC(15,2),
 extra NUMERIC(15,2), mta_tax NUMERIC(15,2), tip_amount NUMERIC(15,2), tolls_amount NUMERIC(15,2),
 improvement_surcharge NUMERIC(15,2), total_amount NUMERIC(15,2), congestion_surcharge NUMERIC(15,2),
 airport_fee NUMERIC(15,2))`

// fhvDDL is the for-hire fhv_trips table, valid on both engines; fhv and fhvhv
// rows project onto this one layout.
const fhvDDL = `(id BIGINT PRIMARY KEY, dispatching_base_num TEXT, pickup_at TIMESTAMP,
 dropoff_at TIMESTAMP, pu_location_id INT, do_location_id INT, sr_flag INT,
 affiliated_base_number TEXT)`

// Era projections. Each selects the DDL columns after id; numerics coalesce so
// both engines load the same values (mysql turns empty csv fields into zeros),
// and legacy text codes map onto the modern numeric ones.
const (
	// yellow 2009: vendor_name/Trip_Pickup_DateTime layout, lat/lon, text codes.
	yellow2009Select = `SELECT
 CASE upper(vendor_name) WHEN 'CMT' THEN 1 WHEN 'VTS' THEN 2 WHEN 'DDS' THEN 3 ELSE 0 END,
 CAST(Trip_Pickup_DateTime AS TIMESTAMP), CAST(Trip_Dropoff_DateTime AS TIMESTAMP),
 COALESCE(Passenger_Count, 0), COALESCE(Trip_Distance, 0), COALESCE(TRY_CAST(Rate_Code AS INTEGER), 0),
 COALESCE(CAST(store_and_forward AS VARCHAR), ''), 0, 0,
 CASE WHEN upper(CAST(Payment_Type AS VARCHAR)) LIKE 'CRE%' THEN 1 WHEN upper(CAST(Payment_Type AS VARCHAR)) LIKE 'CAS%' THEN 2 ELSE 0 END,
 COALESCE(Fare_Amt, 0), COALESCE(surcharge, 0), COALESCE(mta_tax, 0), COALESCE(Tip_Amt, 0),
 COALESCE(Tolls_Amt, 0), 0, COALESCE(Total_Amt, 0), 0, 0`

	// yellow 2010: lowercase names, varchar timestamps and codes, lat/lon.
	yellow2010Select = `SELECT
 CASE upper(vendor_id) WHEN 'CMT' THEN 1 WHEN 'VTS' THEN 2 WHEN 'DDS' THEN 3 ELSE 0 END,
 CAST(pickup_datetime AS TIMESTAMP), CAST(dropoff_datetime AS TIMESTAMP),
 COALESCE(passenger_count, 0), COALESCE(trip_distance, 0), COALESCE(TRY_CAST(rate_code AS INTEGER), 0),
 COALESCE(store_and_fwd_flag, ''), 0, 0,
 CASE WHEN upper(payment_type) LIKE 'CRE%' THEN 1 WHEN upper(payment_type) LIKE 'CAS%' THEN 2 ELSE 0 END,
 COALESCE(fare_amount, 0), COALESCE(surcharge, 0), COALESCE(mta_tax, 0), COALESCE(tip_amount, 0),
 COALESCE(tolls_amount, 0), 0, COALESCE(total_amount, 0), 0, 0`

	// yellow 2011 onward: the harmonized modern layout.
	yellowModernSelect = `SELECT
 COALESCE(TRY_CAST(VendorID AS INTEGER), 0), tpep_pickup_datetime, tpep_dropoff_datetime,
 COALESCE(TRY_CAST(passenger_count AS INTEGER), 0), COALESCE(trip_distance, 0), COALESCE(TRY_CAST(RatecodeID AS INTEGER), 0),
 COALESCE(store_and_fwd_flag, ''), COALESCE(TRY_CAST(PULocationID AS INTEGER), 0), COALESCE(TRY_CAST(DOLocationID AS INTEGER), 0),
 COALESCE(TRY_CAST(payment_type AS INTEGER), 0), COALESCE(fare_amount, 0), COALESCE(extra, 0), COALESCE(mta_tax, 0),
 COALESCE(tip_amount, 0), COALESCE(tolls_amount, 0), COALESCE(improvement_surcharge, 0),
 COALESCE(total_amount, 0), COALESCE(congestion_surcharge, 0), COALESCE(Airport_fee, 0)`

	// fhv 2015 onward: one layout for all ten years, integer widths vary.
	fhvSelect = `SELECT
 COALESCE(dispatching_base_num, ''), pickup_datetime, COALESCE(dropOff_datetime, pickup_datetime),
 COALESCE(TRY_CAST(PUlocationID AS INTEGER), 0), COALESCE(TRY_CAST(DOlocationID AS INTEGER), 0),
 COALESCE(TRY_CAST(SR_Flag AS INTEGER), 0), COALESCE(Affiliated_base_number, '')`

	// fhvhv 2019 onward, projected onto the fhv layout: the originating base
	// stands in for the affiliated base, shared_request_flag for sr_flag.
	fhvhvSelect = `SELECT
 COALESCE(dispatching_base_num, ''), pickup_datetime, dropoff_datetime,
 COALESCE(PULocationID, 0), COALESCE(DOLocationID, 0),
 CASE WHEN shared_request_flag = 'Y' THEN 1 ELSE 0 END, COALESCE(originating_base_num, '')`
)

// month is one TLC file month.
type month struct{ y, m int }

// idx orders months on a single axis for range arithmetic.
func (m month) idx() int { return m.y*12 + m.m - 1 }

// next is the month after m.
func (m month) next() month {
	if m.m == 12 {
		return month{m.y + 1, 1}
	}
	return month{m.y, m.m + 1}
}

// monthAt is the month with the given idx.
func monthAt(i int) month { return month{i / 12, i%12 + 1} }

// part is one schema era of a table: a file prefix, its month range, and the
// projection plus timestamp expressions the era needs.
type part struct {
	prefix, sel, pickup, dropoff string
	start, end                   month
}

// tripsParts lists the yellow-taxi eras.
var tripsParts = []part{
	{"yellow_tripdata", yellow2009Select, "CAST(Trip_Pickup_DateTime AS TIMESTAMP)", "CAST(Trip_Dropoff_DateTime AS TIMESTAMP)", month{2009, 1}, month{2009, 12}},
	{"yellow_tripdata", yellow2010Select, "CAST(pickup_datetime AS TIMESTAMP)", "CAST(dropoff_datetime AS TIMESTAMP)", month{2010, 1}, month{2010, 12}},
	{"yellow_tripdata", yellowModernSelect, "tpep_pickup_datetime", "tpep_dropoff_datetime", month{2011, 1}, taxiEnd},
}

// fhvParts lists the for-hire filesets.
var fhvParts = []part{
	{"fhv_tripdata", fhvSelect, "pickup_datetime", "COALESCE(dropOff_datetime, pickup_datetime)", month{2015, 1}, taxiEnd},
	{"fhvhv_tripdata", fhvhvSelect, "pickup_datetime", "dropoff_datetime", month{2019, 2}, taxiEnd},
}

// SeedTaxi loads the last months of the nyc-taxi dataset into db's bench
// namespace. taxiMaxMonths covers the fixed January 2009 through December 2024
// range, about 4 billion rows. It requires DuckDB on PATH and network access for
// uncached months.
func SeedTaxi(ctx context.Context, db *harness.DB, months int) ([]Table, error) {
	if months < 1 || months > taxiMaxMonths {
		return nil, fmt.Errorf("taxi months must be 1..%d, got %d", taxiMaxMonths, months)
	}
	begin := monthAt(taxiEnd.idx() - (months - 1))

	tables := make([]Table, 0, 2)
	for _, t := range []struct {
		name, ddl string
		parts     []part
	}{
		{"trips", taxiDDL, tripsParts},
		{"fhv_trips", fhvDDL, fhvParts},
	} {
		tbl, err := seedTaxiTable(ctx, db, t.name, t.ddl, t.parts, begin)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tbl)
	}
	return tables, nil
}

// seedTaxiTable downloads each era's months inside the window, exports their
// union as one csv, and loads it.
func seedTaxiTable(ctx context.Context, db *harness.DB, table, ddl string, parts []part, begin month) (Table, error) {
	var selects []string
	var fileOrdinal int64
	for _, p := range parts {
		lo, hi := p.start, p.end
		if begin.idx() > lo.idx() {
			lo = begin
		}
		if lo.idx() > hi.idx() {
			continue
		}
		files, err := taxiParquet(ctx, p.prefix, lo, hi)
		if err != nil {
			return Table{}, err
		}
		// TLC files carry a few stray rows dated outside their month; the
		// range filter keeps counts deterministic and every timestamp inside
		// mysql's TIMESTAMP range.
		upper := hi.next()
		projection := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(p.sel), "SELECT"))
		for _, file := range files {
			// Each monthly file gets a stable 32-bit row-number range. This is
			// deterministic without globally sorting the full Taxi dataset.
			idBase := fileOrdinal << 32
			selects = append(selects, fmt.Sprintf(
				"SELECT %d + file_row_number AS id, %s FROM read_parquet(%s, file_row_number=true) WHERE %s >= '%d-%02d-01' AND %s < '%d-%02d-01' AND %s < '%d-%02d-01'",
				idBase, projection, duckDBString(file),
				p.pickup, lo.y, lo.m, p.pickup, upper.y, upper.m, p.dropoff, upper.next().y, upper.next().m))
			fileOrdinal++
		}
	}
	if len(selects) == 0 {
		return Table{}, fmt.Errorf("%s: no files in window", table)
	}
	dir, err := os.MkdirTemp("", "taxi")
	if err != nil {
		return Table{}, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	csv := filepath.Join(dir, table+".csv")
	script := fmt.Sprintf(
		"COPY (SELECT * FROM (%s)) TO %s (FORMAT csv, HEADER false);",
		strings.Join(selects, "\nUNION ALL\n"), duckDBString(csv))
	if out, err := exec.CommandContext(ctx, "duckdb", "-c", script).CombinedOutput(); err != nil {
		return Table{}, fmt.Errorf("duckdb %s export: %w:\n%s", table, err, out)
	}

	rows, err := db.Engine.Load(ctx, db, harness.TableDef{Name: table, DDL: ddl, CSV: csv})
	if err != nil {
		return Table{}, err
	}
	return Table{Name: table, Rows: rows}, nil
}

// taxiParquet returns the cached parquet path per month from lo through hi,
// downloading any that are missing into the user cache so reps and reruns
// skip the network.
func taxiParquet(ctx context.Context, prefix string, lo, hi month) ([]string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}
	cache := filepath.Join(base, "galaxy-benchmarks", "taxi")
	if err := os.MkdirAll(cache, 0o755); err != nil {
		return nil, err
	}

	var files []string
	for m := lo; m.idx() <= hi.idx(); m = m.next() {
		name := fmt.Sprintf("%s_%d-%02d.parquet", prefix, m.y, m.m)
		path := filepath.Join(cache, name)
		info, err := os.Stat(path)
		if err == nil {
			switch {
			case !info.Mode().IsRegular():
				return nil, fmt.Errorf("taxi cache path is not a regular file: %s", path)
			case info.Size() > 0:
				files = append(files, path)
				continue
			default:
				if err := os.Remove(path); err != nil {
					return nil, fmt.Errorf("remove empty taxi cache file %s: %w", path, err)
				}
			}
		}
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("inspect taxi cache %s: %w", path, err)
		}
		if err := downloadTaxiMonth(ctx, name, path); err != nil {
			return nil, err
		}
		files = append(files, path)
	}
	return files, nil
}

// downloadTaxiMonth fetches one month into path. It writes to a temporary file,
// renames the file after a successful download, and retries up to three times.
func downloadTaxiMonth(ctx context.Context, name, path string) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.part")
	if err != nil {
		return fmt.Errorf("create temporary taxi download: %w", err)
	}
	tmp := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close temporary taxi download: %w", err)
	}
	// DuckDB COPY expects to create its destination itself.
	if err := os.Remove(tmp); err != nil {
		return fmt.Errorf("prepare temporary taxi download: %w", err)
	}
	defer func() { _ = os.Remove(tmp) }()

	script := fmt.Sprintf(
		"INSTALL httpfs; LOAD httpfs; COPY (FROM read_parquet(%s)) TO %s (FORMAT parquet);",
		duckDBString("https://d37ci6vzurychx.cloudfront.net/trip-data/"+name), duckDBString(tmp))

	var out []byte
	var downloadErr error
	for attempt := 1; attempt <= 3; attempt++ {
		out, downloadErr = exec.CommandContext(ctx, "duckdb", "-c", script).CombinedOutput()
		if downloadErr == nil {
			return os.Rename(tmp, path)
		}
		_ = os.Remove(tmp)
		if ctx.Err() != nil {
			return fmt.Errorf("download %s: %w", name, ctx.Err())
		}
		if attempt == 3 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt) * 5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("download %s: %w", name, ctx.Err())
		case <-timer.C:
		}
	}
	return fmt.Errorf("download %s: %w:\n%s", name, downloadErr, out)
}
