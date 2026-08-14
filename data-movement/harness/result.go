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

// Rep is one repetition: fresh containers, fresh seed, one timed run.
type Rep struct {
	SetupSeconds float64       `json:"setupSeconds"`
	WallSeconds  float64       `json:"wallSeconds"`
	RowsPerSec   float64       `json:"rowsPerSec"`
	Resources    []Usage       `json:"resources,omitempty"`
	Parity       []TableParity `json:"parity"`
	ParityPass   bool          `json:"parityPass"`
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
	Reps      []Rep     `json:"reps"`

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

// CheckParity counts every table on both sides in the bench namespace.
func CheckParity(ctx context.Context, source, sink *DB, tables []string) ([]TableParity, bool, error) {
	pass := true
	out := make([]TableParity, 0, len(tables))
	for _, table := range tables {
		p := TableParity{Table: table}
		n, err := source.Engine.Count(ctx, source, table)
		if err != nil {
			return nil, false, fmt.Errorf("count source %s: %w", table, err)
		}
		p.Source = n
		if n, err = sink.Engine.Count(ctx, sink, table); err != nil {
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

// ResultPath returns the immutable output path for a result.
func ResultPath(dir string, r *Result) string {
	name := fmt.Sprintf("%s-%s-%s-%s.json", r.Scenario, r.Route, r.Dataset, safeSlug(r.Topology))
	return filepath.Join(dir, r.StartedAt.UTC().Format("2006-01-02"), safeSlug(r.Cohort), r.SUT, name)
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
	if _, err := f.Write(append(data, '\n')); err != nil {
		_ = f.Close()
		return "", err
	}
	return path, f.Close()
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
