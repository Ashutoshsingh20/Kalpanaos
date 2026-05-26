package main

import (
	"log"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	csiGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sirl_cognitive_stability_index",
		Help: "Cognitive Stability Index reflecting host system drift bounds.",
	})
	acsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sirl_autonomous_convergence_score",
		Help: "Autonomous Convergence Score indicating state oscillation levels.",
	})
	gisGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sirl_governance_integrity_score",
		Help: "Governance Integrity Score checking allowed/denied execution ratios.",
	})
	rsrGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sirl_recovery_stability_ratio",
		Help: "Recovery Stability Ratio checking crash recovery successes.",
	})
)

type SovereignRuntimeDeterminismMetrics struct {
	mu      sync.RWMutex
	storage *SegmentedStorage
	lastACS float64
	lastCSI float64
}

func NewSovereignRuntimeDeterminismMetrics(storage *SegmentedStorage) *SovereignRuntimeDeterminismMetrics {
	return &SovereignRuntimeDeterminismMetrics{
		storage: storage,
		lastACS: 1.0,
		lastCSI: 1.0,
	}
}

func (s *SovereignRuntimeDeterminismMetrics) RecordStabilityMetrics(drift, oscillationCount float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. CSI calculation
	csi := 1.0 - drift
	if csi < 0.0 {
		csi = 0.0
	}
	s.lastCSI = csi
	csiGauge.Set(csi)

	// 2. ACS calculation
	acs := 1.0
	if oscillationCount > 0 {
		acs = 1.0 / (oscillationCount + 1.0)
	}
	s.lastACS = acs
	acsGauge.Set(acs)

	// 3. GIS calculation
	var allowed, denied int
	_ = s.storage.GovernanceDB.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE outcome = 'allowed'").Scan(&allowed)
	_ = s.storage.GovernanceDB.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE outcome = 'denied'").Scan(&denied)
	gis := 1.0
	if allowed+denied > 0 {
		gis = float64(allowed) / float64(allowed+denied)
	}
	gisGauge.Set(gis)

	// 4. RSR calculation
	var successes, crashes int
	_ = s.storage.CognitionDB.QueryRow("SELECT COUNT(*) FROM recovery_log WHERE outcome = 'success'").Scan(&successes)
	_ = s.storage.CognitionDB.QueryRow("SELECT COUNT(*) FROM recovery_log").Scan(&crashes)
	rsr := 1.0
	if crashes > 0 {
		rsr = float64(successes) / float64(crashes)
	}
	rsrGauge.Set(rsr)

	log.Printf("[SRDM] Published determinism metrics: CSI=%.2f, ACS=%.2f, GIS=%.2f, RSR=%.2f", csi, acs, gis, rsr)
}

func (s *SovereignRuntimeDeterminismMetrics) GetMetricsState() map[string]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]float64{
		"csi": s.lastCSI,
		"acs": s.lastACS,
	}
}
