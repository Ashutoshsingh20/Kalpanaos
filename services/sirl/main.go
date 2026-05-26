package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	dockerclient "github.com/docker/docker/client"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ─── Config ─────────────────────────────────────────────────────────────────

type Config struct {
	Port     string
	DBPath   string
	SILURL   string
	PGEURL   string
	IKGURL   string
	SSIURL   string
	CBALURL  string
	NATSURL  string
	Network  string
	NodeID   string
	CertFile string
	KeyFile  string
	CAFile   string
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() Config {
	host, _ := os.Hostname()
	if host == "" {
		host = "edge-node-unknown"
	}
	return Config{
		Port:     envOr("PORT", "8011"),
		DBPath:   envOr("DB_PATH", "/data/sirl/sirl.db"), // Dir path will be used for segmented DBs
		SILURL:   envOr("SIL_URL", "http://sil:8001"),
		PGEURL:   envOr("PGE_URL", "http://pge:8007"),
		IKGURL:   envOr("IKG_URL", "http://ikg:8008"),
		SSIURL:   envOr("SSI_URL", "http://ssi:8003"),
		CBALURL:  envOr("CBAL_URL", "http://cbal:8010"),
		NATSURL:  envOr("NATS_URL", "nats://nats:4222"),
		Network:  envOr("DOCKER_NETWORK", "kalpana-net"),
		NodeID:   envOr("NODE_ID", host),
		CertFile: envOr("CERT_FILE", ""),
		KeyFile:  envOr("KEY_FILE", ""),
		CAFile:   envOr("CA_FILE", ""),
	}
}

// ─── Daemon Context & State ─────────────────────────────────────────────────

type Daemon struct {
	cfg        Config
	storage    *SegmentedStorage
	dockerCli  *dockerclient.Client
	nc         *nats.Conn
	js         nats.JetStreamContext
	httpClient *http.Client

	// Hardened modules
	rib     *RuntimeIsolationBroker
	ige     *IntentGraphEngine
	budget  *CognitionBudgetingEngine
	sandbox *AutonomousGovernanceSandbox
	esgl    *EventSaturationGovernance

	// Cache
	graph    *DependencyGraph
	mu       sync.RWMutex
	peerBids map[string]chan float64
	bidMu    sync.Mutex
}

func main() {
	log.Printf("[SIRL] Starting Hardened Sovereign Infrastructure Runtime Layer...")
	cfg := loadConfig()

	// Initialize Segmented Database Storage
	dbDir := filepath.Dir(cfg.DBPath)
	storage, err := InitSegmentedStorage(dbDir)
	if err != nil {
		log.Fatalf("[SIRL] DB initialization failed: %v", err)
	}
	defer storage.Close()

	// Initialize Docker client
	dockerCli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("[SIRL] Docker client creation failed: %v", err)
	}
	defer dockerCli.Close()

	// Initialize HTTP Client (with optional mTLS)
	httpClient := buildTLSClient(cfg.CertFile, cfg.KeyFile, cfg.CAFile)

	d := &Daemon{
		cfg:        cfg,
		storage:    storage,
		dockerCli:  dockerCli,
		httpClient: httpClient,
		peerBids:   make(map[string]chan float64),
		graph:      NewDependencyGraph(),

		// Instantiate modules
		rib:     NewRuntimeIsolationBroker(cfg),
		ige:     NewIntentGraphEngine(storage.CognitionDB),
		budget:  NewCognitionBudgetingEngine(),
		sandbox: NewAutonomousGovernanceSandbox(storage.GovernanceDB),
	}

	// Connect NATS JetStream
	d.connectNATS()

	// Initialize ESGL
	d.esgl = NewEventSaturationGovernance(d.nc, cfg.NodeID)
	defer d.esgl.Close()

	// Start Background Cognitive Loops
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.StartControlLoops(ctx)

	// Set up router and serve
	r := d.setupRouter()
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: r,
	}

	startSrv, srvType := buildTLSServer(srv, cfg.CertFile, cfg.KeyFile, cfg.CAFile)
	log.Printf("[SIRL] Server starting on port %s in %s mode", cfg.Port, srvType)
	if err := startSrv(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[SIRL] Server failed: %v", err)
	}
}

// ─── NATS Integration ────────────────────────────────────────────────────────

func (d *Daemon) connectNATS() {
	var nc *nats.Conn
	var err error
	for i := 0; i < 5; i++ {
		nc, err = nats.Connect(d.cfg.NATSURL,
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(5),
			nats.ReconnectWait(3*time.Second),
		)
		if err == nil {
			break
		}
		log.Printf("[NATS] Retry connecting (%d/5): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Printf("[NATS] Warning: Running without NATS capability: %v", err)
		return
	}
	d.nc = nc
	log.Printf("[NATS] Connected to %s", d.cfg.NATSURL)

	js, err := nc.JetStream()
	if err != nil {
		log.Printf("[NATS JetStream] JetStream initialization failed: %v", err)
		return
	}
	d.js = js

	// Create JetStream stream for SIRL events
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "SIRL_EVENTS",
		Subjects: []string{"kalpana.sirl.events.>", "kalpana.sirl.domain.>"},
		Storage:  nats.FileStorage,
		MaxAge:   14 * 24 * time.Hour,
		MaxBytes: 50 * 1024 * 1024, // Capped at 50MB
		Discard:  nats.DiscardOld,
	})
	if err != nil {
		log.Printf("[NATS JetStream] Stream creation failed: %v", err)
	}

	d.setupSubscriptions()
}

// ─── TLS / mTLS Helpers ──────────────────────────────────────────────────────

func buildTLSClient(certFile, keyFile, caFile string) *http.Client {
	if certFile == "" || keyFile == "" || caFile == "" {
		return &http.Client{Timeout: 10 * time.Second}
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Printf("[mTLS Client] Load cert failed: %v", err)
		return &http.Client{Timeout: 10 * time.Second}
	}
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Printf("[mTLS Client] Load CA failed: %v", err)
		return &http.Client{Timeout: 10 * time.Second}
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      pool,
			},
		},
	}
}

func buildTLSServer(srv *http.Server, certFile, keyFile, caFile string) (func() error, string) {
	if certFile == "" || keyFile == "" || caFile == "" {
		return srv.ListenAndServe, "HTTP"
	}
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Printf("[mTLS Server] Load CA failed: %v", err)
		return srv.ListenAndServe, "HTTP"
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	srv.TLSConfig = &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  pool,
	}
	return func() error { return srv.ListenAndServeTLS(certFile, keyFile) }, "mTLS"
}

// ─── HTTP Router ─────────────────────────────────────────────────────────────

func (d *Daemon) setupRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "sirl", "node_id": d.cfg.NodeID})
	})

	r.Handle("/metrics", promhttp.Handler())

	// Workload administration REST API
	r.Post("/api/sirl/workloads", d.handlePOSTWorkload)
	r.Get("/api/sirl/recovery/status/{id}", d.handleGETRecoveryStatus)
	r.Get("/api/sirl/node/state", d.handleGETNodeState)

	// Harden verification API
	r.Get("/api/sirl/intent/lineage/{id}", d.handleGETIntentLineage)
	r.Post("/api/sirl/governance/quota/validate", d.handlePOSTQuotaValidate)

	return r
}
