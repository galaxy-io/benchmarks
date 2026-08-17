package harness

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SnapshotDatabase reads lightweight cumulative counters. It is deliberately
// best-effort: observability trouble is recorded in the result but never turns
// a completed transfer into a failed benchmark.
func SnapshotDatabase(ctx context.Context, db *DB) *DatabaseSnapshot {
	s := &DatabaseSnapshot{CapturedAt: time.Now().UTC(), Counters: map[string]float64{}}
	h, err := db.Open()
	if err != nil {
		s.Error = err.Error()
		return s
	}
	defer func() { _ = h.Close() }()

	switch db.Engine {
	case Postgres:
		snapshotPostgres(ctx, h, s)
	case MySQL:
		snapshotMySQL(ctx, h, s)
	}
	return s
}

func snapshotPostgres(ctx context.Context, db *sql.DB, s *DatabaseSnapshot) {
	var errs []string
	read := func(prefix, query string, names ...string) {
		values := make([]float64, len(names))
		dest := make([]any, len(names))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := db.QueryRowContext(ctx, query).Scan(dest...); err != nil {
			errs = append(errs, prefix+": "+err.Error())
			return
		}
		for i, name := range names {
			s.Counters[prefix+"."+name] = values[i]
		}
	}

	read("database", `SELECT blks_read::float8, blks_hit::float8,
temp_files::float8, temp_bytes::float8, deadlocks::float8,
tup_returned::float8, tup_fetched::float8, tup_inserted::float8,
tup_updated::float8, tup_deleted::float8
FROM pg_stat_database WHERE datname = current_database()`,
		"blocksRead", "blocksHit", "tempFiles", "tempBytes", "deadlocks",
		"tuplesReturned", "tuplesFetched", "tuplesInserted", "tuplesUpdated", "tuplesDeleted")

	read("wal", `SELECT wal_records::float8, wal_fpi::float8, wal_bytes::text::float8,
wal_buffers_full::float8, wal_write::float8, wal_sync::float8,
wal_write_time::float8, wal_sync_time::float8 FROM pg_stat_wal`,
		"records", "fullPageImages", "bytes", "buffersFull", "writes", "syncs", "writeMillis", "syncMillis")

	read("bgwriter", `SELECT checkpoints_timed::float8, checkpoints_req::float8,
checkpoint_write_time::float8, checkpoint_sync_time::float8,
buffers_checkpoint::float8, buffers_clean::float8, maxwritten_clean::float8,
buffers_backend::float8, buffers_backend_fsync::float8, buffers_alloc::float8
FROM pg_stat_bgwriter`,
		"checkpointsTimed", "checkpointsRequested", "checkpointWriteMillis", "checkpointSyncMillis",
		"buffersCheckpoint", "buffersClean", "maxwrittenClean", "buffersBackend", "backendFsyncs", "buffersAllocated")

	read("io", `SELECT COALESCE(sum(reads), 0)::float8, COALESCE(sum(writes), 0)::float8,
COALESCE(sum(writebacks), 0)::float8, COALESCE(sum(extends), 0)::float8,
COALESCE(sum(hits), 0)::float8, COALESCE(sum(evictions), 0)::float8,
COALESCE(sum(reuses), 0)::float8, COALESCE(sum(fsyncs), 0)::float8
FROM pg_stat_io`,
		"reads", "writes", "writebacks", "extends", "hits", "evictions", "reuses", "fsyncs")

	rows, err := db.QueryContext(ctx, `SELECT slot_name, active,
COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint,
COALESCE(wal_status::text, '') FROM pg_replication_slots ORDER BY slot_name`)
	if err != nil {
		errs = append(errs, "replicationSlots: "+err.Error())
	} else {
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var slot ReplicationSlot
			if err := rows.Scan(&slot.Name, &slot.Active, &slot.RetainedBytes, &slot.WALStatus); err != nil {
				errs = append(errs, "replicationSlots: "+err.Error())
				break
			}
			s.Slots = append(s.Slots, slot)
		}
		if err := rows.Err(); err != nil {
			errs = append(errs, "replicationSlots: "+err.Error())
		}
	}
	s.Error = strings.Join(errs, "; ")
}

func snapshotMySQL(ctx context.Context, db *sql.DB, s *DatabaseSnapshot) {
	wanted := map[string]string{
		"Bytes_received":                   "bytesReceived",
		"Bytes_sent":                       "bytesSent",
		"Created_tmp_disk_tables":          "tempDiskTables",
		"Innodb_buffer_pool_read_requests": "bufferPoolReadRequests",
		"Innodb_buffer_pool_reads":         "bufferPoolReads",
		"Innodb_data_reads":                "dataReads",
		"Innodb_data_writes":               "dataWrites",
		"Innodb_os_log_written":            "logBytesWritten",
		"Threads_connected":                "threadsConnected",
		"Threads_running":                  "threadsRunning",
	}
	rows, err := db.QueryContext(ctx, "SHOW GLOBAL STATUS")
	if err != nil {
		s.Error = err.Error()
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, raw string
		if err := rows.Scan(&name, &raw); err != nil {
			s.Error = err.Error()
			return
		}
		key, ok := wanted[name]
		if !ok {
			continue
		}
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			s.Error = fmt.Sprintf("%s: %v", name, err)
			continue
		}
		s.Counters["mysql."+key] = value
	}
	if err := rows.Err(); err != nil {
		s.Error = err.Error()
	}
}
