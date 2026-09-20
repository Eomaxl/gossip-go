package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Eomaxl/gossip-go/internal/gossip"
)

func main() {
	var (
		bind      = flag.String("bind", "127.0.0.1:7000", "UDP address to listen on")
		advertise = flag.String("advertise", "", "address peers should reply to (defaults to -bind)")
		httpAddr  = flag.String("http", "127.0.0.1:8000", "HTTP address for the introspection API")
		seedsRaw  = flag.String("seeds", "", "comma-separated seed addresses")
		cluster   = flag.String("cluster", "test-cluster", "cluster name; nodes with different names refuse to talk")
		dc        = flag.String("dc", "DC1", "data center, published as application state")
		rack      = flag.String("rack", "RAC1", "rack, published as application state")
		interval  = flag.Duration("interval", time.Second, "gossip round interval")
		phi       = flag.Float64("phi", gossip.DefaultPhiThreshold, "phi conviction threshold")
		fanout    = flag.Int("fanout", 1, "live peers contacted per round")
		verbose   = flag.Bool("v", false, "debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	adv := *advertise
	if adv == "" {
		adv = *bind
	}

	var seeds []string
	for _, s := range strings.Split(*seedsRaw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			seeds = append(seeds, s)
		}
	}

	g, err := gossip.New(gossip.Config{
		Bind:           *bind,
		Advertise:      adv,
		Seeds:          seeds,
		Cluster:        *cluster,
		GossipInterval: *interval,
		PhiThreshold:   *phi,
		FanOut:         *fanout,
		Logger:         log,
	})
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}

	// Membership transitions are the thing you actually want in logs. Print
	// them at INFO so a demo is readable without -v.
	g.Subscribe(func(e gossip.Event) {
		attrs := []any{"event", string(e.Kind), "endpoint", e.Endpoint}
		if e.Phi > 0 {
			attrs = append(attrs, "phi", fmt.Sprintf("%.2f", e.Phi))
		}
		log.Info("membership change", attrs...)
	})

	// Publish the same categories of application state Cassandra does. These
	// ride along on the gossip stream with no extra machinery.
	g.SetAppState(gossip.AppStatus, "NORMAL")
	g.SetAppState(gossip.AppDC, *dc)
	g.SetAppState(gossip.AppRack, *rack)
	g.SetAppState(gossip.AppRelease, "gossip-go/0.1.0")

	g.Start()
	defer g.Stop()

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           routes(g),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("http api listening", "addr", *httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func routes(g *gossip.Gossiper) http.Handler {
	mux := http.NewServeMux()

	// GET /members — the nodetool status equivalent.
	mux.HandleFunc("GET /members", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, g.Members())
	})

	// GET /stats — round and message counters.
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, g.Stats())
	})

	// PUT /state/{key} with a raw body — publish application state and watch it
	// spread. This is the endpoint that makes the demo worth running: set a key
	// on one node, poll /members on another, count the rounds.
	mux.HandleFunc("PUT /state/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}
		// A single Body.Read() only returns what one read syscall produced,
		// not the whole body — a value split across TCP segments would be
		// silently truncated. MaxBytesReader caps the size; ReadAll drains it.
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
		if err != nil {
			http.Error(w, "body too large or unreadable", http.StatusBadRequest)
			return
		}
		value := strings.TrimSpace(string(body))
		if value == "" {
			http.Error(w, "empty value", http.StatusBadRequest)
			return
		}
		g.SetAppState(key, value)
		writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": value})
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
