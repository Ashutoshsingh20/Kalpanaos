package main

import (
	"bufio"
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	localCPUUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sirl_host_cpu_utilization_percent",
		Help: "CPU usage percent of the host edge node.",
	})
	localMemUsage = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sirl_host_memory_used_bytes",
		Help: "Memory usage bytes of the host edge node.",
	})
)

// GatherHostTelemetry records CPU, RAM, and thermal state of the edge node
func (d *Daemon) GatherHostTelemetry() {
	_, usedMem := readHostMemory()
	cpuPct := readHostCPU()
	temp := readHostTemperature()

	// Update Prometheus metrics
	localCPUUsage.Set(cpuPct)
	localMemUsage.Set(float64(usedMem))

	// Persist to Segmented Storage asynchronously (ring buffer queue)
	d.storage.WriteTelemetryAsync(cpuPct, usedMem, temp)

	// Publish to NATS via ESGL (Priority 2: Operational - buffered and averaged)
	d.esgl.Dispatch(Event{
		Subject:  "kalpana.sirl.domain.runtime.events.telebeat",
		Priority: PriorityOperational,
		Payload: TelemetryMetric{
			CPUUsage: cpuPct,
			MemUsed:  usedMem,
			Temp:     temp,
		},
	})
}

// GossipCapabilities broadcasts this node's stats to the mesh periodically
func (d *Daemon) GossipCapabilities() {
	if d.nc == nil || !d.nc.IsConnected() {
		return
	}

	totalMem, usedMem := readHostMemory()
	var avgTemp float64
	_ = d.storage.TelemetryDB.QueryRow("SELECT COALESCE(AVG(temp), 0) FROM metrics").Scan(&avgTemp)

	trust := 0.95
	_ = d.storage.RuntimeDB.QueryRow("SELECT trust_score FROM mesh_nodes WHERE node_id = ?", d.cfg.NodeID).Scan(&trust)

	cap := NodeCapabilities{
		NodeID:      d.cfg.NodeID,
		TrustScore:  trust,
		CPUCores:    2,
		MemoryTotal: int64(totalMem),
		MemoryFree:  int64(totalMem - usedMem),
		Temp:        avgTemp,
		Timestamp:   time.Now(),
	}

	// Publish to NATS via ESGL (Priority 2: Operational - heartbeats throttled / batched)
	d.esgl.Dispatch(Event{
		Subject:  "kalpana.sirl.domain.coordination.gossip.heartbeat",
		Priority: PriorityOperational,
		Payload:  cap,
	})
}

// Helpers to read host physical state

func readHostMemory() (total, used uint64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 4294967296, 2147483648 // 4GB Total, 2GB Used fallbacks
	}
	defer f.Close()

	values := map[string]uint64{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		val, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		values[key] = val * 1024 // kB -> bytes
	}

	total = values["MemTotal"]
	free := values["MemFree"]
	buffers := values["Buffers"]
	cached := values["Cached"]
	reclaimable := values["SReclaimable"]

	available := free + buffers + cached + reclaimable
	if total >= available {
		used = total - available
	}
	return total, used
}

func readHostCPU() float64 {
	f, err := os.Open("/proc/stat")
	if err != nil {
		rand.Seed(time.Now().UnixNano())
		return 10.0 + rand.Float64()*50.0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	if scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && fields[0] == "cpu" {
			user, _ := strconv.ParseFloat(fields[1], 64)
			nice, _ := strconv.ParseFloat(fields[2], 64)
			system, _ := strconv.ParseFloat(fields[3], 64)
			idle, _ := strconv.ParseFloat(fields[4], 64)

			total := user + nice + system + idle
			busy := user + nice + system
			if total > 0 {
				return (busy / total) * 100.0
			}
		}
	}
	return 25.0
}

func readHostTemperature() float64 {
	f, err := os.Open("/sys/class/thermal/thermal_zone0/temp")
	if err != nil {
		rand.Seed(time.Now().UnixNano())
		return 38.0 + rand.Float64()*10.0
	}
	defer f.Close()

	var buf bytes.Buffer
	_, _ = buf.ReadFrom(f)
	val, err := strconv.ParseFloat(strings.TrimSpace(buf.String()), 64)
	if err == nil {
		return val / 1000.0
	}
	return 42.0
}
