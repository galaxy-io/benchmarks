package harness

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"
	mobyclient "github.com/moby/moby/client"
	tc "github.com/testcontainers/testcontainers-go"
)

// LabelRun scopes containers to a repetition; LabelRole names their job.
const (
	LabelRun  = "bench.run"
	LabelRole = "bench.role"
)

// Usage is one container's resource consumption over the sampled window.
type Usage struct {
	Role         string  `json:"role"`
	CPUSeconds   float64 `json:"cpuSeconds"`
	PeakMemBytes uint64  `json:"peakMemBytes"`
	NetRxBytes   uint64  `json:"netRxBytes"`
	NetTxBytes   uint64  `json:"netTxBytes"`
	Samples      int     `json:"samples"`
}

type sampleState struct {
	role              string
	firstCPU, lastCPU uint64
	peakMem           uint64
	netRx, netTx      uint64
	samples           int
}

// Sampler polls the Docker stats API for every container carrying a run label.
type Sampler struct {
	cli    *tc.DockerClient
	runID  string
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
	usage  []Usage

	mu    sync.Mutex
	state map[string]*sampleState
}

// StartSampler begins polling containers labeled with runID every interval.
func StartSampler(ctx context.Context, runID string, interval time.Duration) (*Sampler, error) {
	cli, err := tc.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, err
	}
	sctx, cancel := context.WithCancel(ctx)
	s := &Sampler{cli: cli, runID: runID, cancel: cancel, done: make(chan struct{}), state: map[string]*sampleState{}}
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			s.sample(sctx, runID)
			select {
			case <-sctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return s, nil
}

func (s *Sampler) sample(ctx context.Context, runID string) {
	cli := s.cli
	list, err := cli.ContainerList(ctx, mobyclient.ContainerListOptions{
		Filters: mobyclient.Filters{}.Add("label", LabelRun+"="+runID),
	})
	if err != nil {
		return
	}
	for _, c := range list.Items {
		resp, err := cli.ContainerStats(ctx, c.ID, mobyclient.ContainerStatsOptions{})
		if err != nil {
			continue
		}
		var stats container.StatsResponse
		err = json.NewDecoder(resp.Body).Decode(&stats)
		_ = resp.Body.Close()
		if err != nil {
			continue
		}

		s.mu.Lock()
		st, ok := s.state[c.ID]
		if !ok {
			st = &sampleState{role: c.Labels[LabelRole], firstCPU: stats.CPUStats.CPUUsage.TotalUsage}
			s.state[c.ID] = st
		}
		st.lastCPU = stats.CPUStats.CPUUsage.TotalUsage
		if stats.MemoryStats.Usage > st.peakMem {
			st.peakMem = stats.MemoryStats.Usage
		}
		var rx, tx uint64
		for _, n := range stats.Networks {
			rx += n.RxBytes
			tx += n.TxBytes
		}
		st.netRx, st.netTx = rx, tx
		st.samples++
		s.mu.Unlock()
	}
}

// Stop ends polling and returns per-role usage over the sampled window; it is idempotent.
func (s *Sampler) Stop() []Usage {
	s.once.Do(func() {
		s.cancel()
		<-s.done

		// One final sample so CPU burned since the last tick still counts.
		fctx, fcancel := context.WithTimeout(context.Background(), 2*time.Second)
		s.sample(fctx, s.runID)
		fcancel()
		_ = s.cli.Close()

		s.mu.Lock()
		defer s.mu.Unlock()
		s.usage = make([]Usage, 0, len(s.state))
		for _, st := range s.state {
			s.usage = append(s.usage, Usage{
				Role:         st.role,
				CPUSeconds:   float64(st.lastCPU-st.firstCPU) / 1e9,
				PeakMemBytes: st.peakMem,
				NetRxBytes:   st.netRx,
				NetTxBytes:   st.netTx,
				Samples:      st.samples,
			})
		}
	})
	return s.usage
}
