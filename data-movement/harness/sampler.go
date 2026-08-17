package harness

import (
	"context"
	"encoding/json"
	"sort"
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
	Image        string  `json:"image,omitempty"`
	ImageID      string  `json:"imageId,omitempty"`
	CPUSeconds   float64 `json:"cpuSeconds"`
	PeakMemBytes uint64  `json:"peakMemBytes"`
	NetRxBytes   uint64  `json:"netRxBytes"`
	NetTxBytes   uint64  `json:"netTxBytes"`
	Samples      int     `json:"samples"`
}

type sampleState struct {
	role, image, imageID string
	firstCPU, lastCPU    uint64
	firstRx, firstTx     uint64
	peakMem              uint64
	netRx, netTx         uint64
	samples              int
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
	// Establish counters for containers that already exist before the timed
	// window. Containers created later begin at zero and are counted from birth.
	s.sample(sctx, runID, true)
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-ticker.C:
				s.sample(sctx, runID, false)
			}
		}
	}()
	return s, nil
}

func (s *Sampler) sample(ctx context.Context, runID string, baseline bool) {
	cli := s.cli
	list, err := cli.ContainerList(ctx, mobyclient.ContainerListOptions{
		All:     true,
		Filters: mobyclient.Filters{}.Add("label", LabelRun+"="+runID),
	})
	if err != nil {
		return
	}
	for _, c := range list.Items {
		s.mu.Lock()
		st, ok := s.state[c.ID]
		if !ok {
			st = &sampleState{role: c.Labels[LabelRole], image: c.Image, imageID: c.ImageID}
			s.state[c.ID] = st
		}
		s.mu.Unlock()
		if c.State != "running" {
			continue
		}
		resp, err := cli.ContainerStats(ctx, c.ID, mobyclient.ContainerStatsOptions{})
		if err != nil {
			continue
		}
		var stats container.StatsResponse
		err = json.NewDecoder(resp.Body).Decode(&stats)
		_ = resp.Body.Close()
		if err != nil || stats.CPUStats.CPUUsage.TotalUsage == 0 {
			// Exited containers report zeroed stats; folding them in would
			// wrap the CPU delta and erase the network counters.
			continue
		}

		var rx, tx uint64
		for _, n := range stats.Networks {
			rx += n.RxBytes
			tx += n.TxBytes
		}

		s.mu.Lock()
		if st.samples == 0 {
			if baseline {
				st.firstCPU = stats.CPUStats.CPUUsage.TotalUsage
				st.firstRx, st.firstTx = rx, tx
			}
		}
		st.lastCPU = stats.CPUStats.CPUUsage.TotalUsage
		if stats.MemoryStats.Usage > st.peakMem {
			st.peakMem = stats.MemoryStats.Usage
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
		s.sample(fctx, s.runID, false)
		fcancel()
		_ = s.cli.Close()

		s.mu.Lock()
		defer s.mu.Unlock()
		s.usage = make([]Usage, 0, len(s.state))
		for _, st := range s.state {
			cpuSeconds := float64(0)
			if st.lastCPU >= st.firstCPU {
				cpuSeconds = float64(st.lastCPU-st.firstCPU) / 1e9
			}
			var netRx, netTx uint64
			if st.netRx >= st.firstRx {
				netRx = st.netRx - st.firstRx
			}
			if st.netTx >= st.firstTx {
				netTx = st.netTx - st.firstTx
			}
			s.usage = append(s.usage, Usage{
				Role:         st.role,
				Image:        st.image,
				ImageID:      st.imageID,
				CPUSeconds:   cpuSeconds,
				PeakMemBytes: st.peakMem,
				NetRxBytes:   netRx,
				NetTxBytes:   netTx,
				Samples:      st.samples,
			})
		}
		sort.Slice(s.usage, func(i, j int) bool {
			if s.usage[i].Role == s.usage[j].Role {
				return s.usage[i].ImageID < s.usage[j].ImageID
			}
			return s.usage[i].Role < s.usage[j].Role
		})
	})
	return s.usage
}
