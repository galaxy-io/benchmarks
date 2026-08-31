package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

// TableParity is one table's row counts on both sides.
type TableParity struct {
	Table      string `json:"table"`
	Source     int64  `json:"source"`
	Sink       int64  `json:"sink"`
	CountMatch bool   `json:"countMatch"`
	Match      bool   `json:"match"`
}

// Endpoint records where one end ran without persisting credentials or a DSN.
type Endpoint struct {
	Engine   string `json:"engine"`
	Provider string `json:"provider"`
	Label    string `json:"label,omitempty"`
}

// ColdStart records the untimed work used to establish a repeatable RDS state.
type ColdStart struct {
	RebootSeconds float64 `json:"rebootSeconds"`
	SettleSeconds float64 `json:"settleSeconds"`
}

// MetricPoint is one timestamped CloudWatch value.
type MetricPoint struct {
	At    time.Time `json:"at"`
	Value float64   `json:"value"`
}

// MetricSeries retains the raw one-minute RDS CloudWatch points that overlap
// the timed window. Enhanced Monitoring remains available at one-second
// resolution in the RDSOSMetrics CloudWatch Logs group.
type MetricSeries struct {
	Name   string        `json:"name"`
	Stat   string        `json:"stat"`
	Points []MetricPoint `json:"points,omitempty"`
}

// ReplicationSlot is the part of pg_replication_slots that can contaminate a
// later repetition by pinning WAL.
type ReplicationSlot struct {
	Name          string `json:"name"`
	Active        bool   `json:"active"`
	RetainedBytes int64  `json:"retainedBytes"`
	WALStatus     string `json:"walStatus,omitempty"`
}

// DatabaseSnapshot captures engine counters without resetting them. Before and
// after values are retained so analysis can compute deltas without changing
// database state.
type DatabaseSnapshot struct {
	CapturedAt time.Time          `json:"capturedAt"`
	Counters   map[string]float64 `json:"counters,omitempty"`
	Slots      []ReplicationSlot  `json:"replicationSlots,omitempty"`
	Error      string             `json:"error,omitempty"`
}

// EndpointHealth combines database-native snapshots and RDS CloudWatch data.
type EndpointHealth struct {
	Role          string            `json:"role"`
	Engine        string            `json:"engine"`
	RDSIdentifier string            `json:"rdsIdentifier,omitempty"`
	Before        *DatabaseSnapshot `json:"before,omitempty"`
	After         *DatabaseSnapshot `json:"after,omitempty"`
	AfterTeardown *DatabaseSnapshot `json:"afterTeardown,omitempty"`
	CloudWatch    []MetricSeries    `json:"cloudWatch,omitempty"`
	MetricsError  string            `json:"metricsError,omitempty"`
}

// Rep is one repetition: fresh SUT containers, an isolated sink, and one timed
// run. A remote cohort can share one immutable source seed across its reps.
type Rep struct {
	StartedAt         time.Time        `json:"startedAt,omitempty"`
	EndedAt           time.Time        `json:"endedAt,omitempty"`
	ColdStart         *ColdStart       `json:"coldStart,omitempty"`
	ProvisionSeconds  float64          `json:"provisionSeconds,omitempty"`
	SeedSeconds       float64          `json:"seedSeconds,omitempty"`
	SetupSeconds      float64          `json:"setupSeconds"`
	WallSeconds       float64          `json:"wallSeconds"`
	TeardownSeconds   float64          `json:"teardownSeconds,omitempty"`
	ValidationSeconds float64          `json:"validationSeconds,omitempty"`
	RowsPerSec        float64          `json:"rowsPerSec"`
	Resources         []Usage          `json:"resources,omitempty"`
	Parity            []TableParity    `json:"parity"`
	ParityPass        bool             `json:"parityPass"`
	Databases         []EndpointHealth `json:"databases,omitempty"`
}

// Result is the JSON document one benchmark invocation emits.
type Result struct {
	SUT      string   `json:"sut"`
	Scenario string   `json:"scenario"`
	Route    string   `json:"route"`
	Dataset  string   `json:"dataset"`
	Rows     int64    `json:"rows"`
	Cohort   string   `json:"cohort"`
	Topology string   `json:"topology"`
	Source   Endpoint `json:"source"`
	Sink     Endpoint `json:"sink"`

	Image  string         `json:"image"`
	Native bool           `json:"native"`
	Config map[string]any `json:"config,omitempty"`

	StartedAt time.Time `json:"startedAt"`
	// Day overrides the date directory so a session spread over several
	// invocations, and possibly over midnight, files under one date. It is not
	// serialized: StartedAt already records when this result began.
	Day  string `json:"-"`
	Reps []Rep  `json:"reps"`

	MedianWallSeconds float64 `json:"medianWallSeconds"`
	MinWallSeconds    float64 `json:"minWallSeconds"`
	MaxWallSeconds    float64 `json:"maxWallSeconds"`
	MedianRowsPerSec  float64 `json:"medianRowsPerSec"`
	ParityPass        bool    `json:"parityPass"`

	OS           string `json:"os"`
	Arch         string `json:"arch"`
	CPUs         int    `json:"cpus"`
	CPUModel     string `json:"cpuModel,omitempty"`
	MemoryBytes  uint64 `json:"memoryBytes,omitempty"`
	GoVersion    string `json:"goVersion"`
	Machine      string `json:"machine,omitempty"`
	GitCommit    string `json:"gitCommit,omitempty"`
	GitDirty     bool   `json:"gitDirty"`
	Verification string `json:"verification"`
}

// Aggregate fills the medians, spread, and overall parity from the reps.
func (r *Result) Aggregate() {
	if len(r.Reps) == 0 {
		return
	}
	walls := make([]float64, 0, len(r.Reps))
	rates := make([]float64, 0, len(r.Reps))
	r.ParityPass = true
	for _, rep := range r.Reps {
		walls = append(walls, rep.WallSeconds)
		rates = append(rates, rep.RowsPerSec)
		r.ParityPass = r.ParityPass && rep.ParityPass
	}
	sort.Float64s(walls)
	sort.Float64s(rates)
	r.MedianWallSeconds = median(walls)
	r.MedianRowsPerSec = median(rates)
	r.MinWallSeconds = walls[0]
	r.MaxWallSeconds = walls[len(walls)-1]
}

// median of a sorted slice.
func median(s []float64) float64 {
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// CheckParity counts delivered tables and compares them with the immutable
// seed manifest. A full load never mutates its source, so rescanning the source
// after every repetition only pollutes the cache for the next run.
func CheckParity(ctx context.Context, sink *DB, tables []string, expected map[string]int64) ([]TableParity, bool, error) {
	pass := true
	out := make([]TableParity, 0, len(tables))
	for _, table := range tables {
		sourceRows, ok := expected[table]
		if !ok {
			return nil, false, fmt.Errorf("seed manifest has no row count for %s", table)
		}
		p := TableParity{Table: table, Source: sourceRows}
		n, err := sink.Engine.Count(ctx, sink, table)
		if err != nil {
			log.Printf("sink count %s: %v", table, err)
			p.Sink = -1
		} else {
			p.Sink = n
		}
		p.CountMatch = p.Source == p.Sink
		p.Match = p.CountMatch
		pass = pass && p.Match
		out = append(out, p)
	}
	return out, pass, nil
}

// ResultPath returns the immutable output path for a result. Day names the date
// directory; empty means the UTC day the invocation started on.
func ResultPath(dir string, r *Result) string {
	name := fmt.Sprintf("%s-%s-%s-%s.json",
		safeSlug(r.Scenario), safeSlug(r.Route), safeSlug(r.Dataset), safeSlug(r.Topology))
	day := r.Day
	if day == "" {
		day = r.StartedAt.UTC().Format("2006-01-02")
	}
	return filepath.Join(dir, safeSlug(day), safeSlug(r.Cohort), safeSlug(r.SUT), name)
}

// WriteResult writes a result under its date and cohort without overwriting an
// earlier invocation, and returns the path.
func WriteResult(dir string, r *Result) (string, error) {
	populateHostMetadata(r)
	path := ResultPath(dir, r)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", err
	}
	complete := false
	defer func() {
		if !complete {
			_ = f.Close()
			_ = os.Remove(path)
		}
	}()
	if _, err := f.Write(append(data, '\n')); err != nil {
		return "", fmt.Errorf("write result: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close result: %w", err)
	}
	complete = true
	return path, nil
}

func populateHostMetadata(r *Result) {
	r.OS = runtime.GOOS
	r.Arch = runtime.GOARCH
	r.CPUs = runtime.NumCPU()
	r.GoVersion = runtime.Version()
	if info, err := cpu.Info(); err == nil && len(info) > 0 {
		r.CPUModel = info[0].ModelName
	}
	if info, err := mem.VirtualMemory(); err == nil {
		r.MemoryBytes = info.Total
	}
	if r.Machine == "" {
		r.Machine = os.Getenv("BENCH_MACHINE")
	}
	if out, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
		r.GitCommit = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("git", "status", "--porcelain").Output(); err == nil {
		r.GitDirty = len(out) > 0
	}
}

func safeSlug(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return "unlabeled"
	}
	return b.String()
}
