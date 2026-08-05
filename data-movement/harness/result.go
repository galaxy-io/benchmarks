package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
)

// TableParity is one table's row counts on both sides.
type TableParity struct {
	Table  string `json:"table"`
	Source int64  `json:"source"`
	Sink   int64  `json:"sink"`
	Match  bool   `json:"match"`
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
	SUT      string `json:"sut"`
	Scenario string `json:"scenario"`
	Route    string `json:"route"`
	Dataset  string `json:"dataset"`
	Rows     int64  `json:"rows"`

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

	OS   string `json:"os"`
	Arch string `json:"arch"`
	CPUs int    `json:"cpus"`
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
	for _, t := range tables {
		p := TableParity{Table: t}
		n, err := source.Engine.Count(ctx, source, t)
		if err != nil {
			return nil, false, fmt.Errorf("count source %s: %w", t, err)
		}
		p.Source = n
		if n, err = sink.Engine.Count(ctx, sink, t); err != nil {
			log.Printf("sink count %s: %v", t, err)
			p.Sink = -1
		} else {
			p.Sink = n
		}
		p.Match = p.Source == p.Sink
		pass = pass && p.Match
		out = append(out, p)
	}
	return out, pass, nil
}

// WriteResult writes the result JSON as dir/{date}/{sut}/{scenario}-{route}-{dataset}.json,
// overwriting any same-day result for the same triple, and returns the path.
func WriteResult(dir string, r *Result) (string, error) {
	r.OS = runtime.GOOS
	r.Arch = runtime.GOARCH
	r.CPUs = runtime.NumCPU()
	name := fmt.Sprintf("%s-%s-%s.json", r.Scenario, r.Route, r.Dataset)
	path := filepath.Join(dir, r.StartedAt.UTC().Format("2006-01-02"), r.SUT, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, append(data, '\n'), 0o644)
}
