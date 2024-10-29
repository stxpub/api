package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/httplog/v2"
	"github.com/jmoiron/sqlx"
	"github.com/madflojo/tasks"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pelletier/go-toml/v2"
	"github.com/stxpub/codec"
)

const (
	sortitionDb  = "burnchain/sortition/marf.sqlite?mode=ro"
	chainstateDb = "chainstate/vm/index.sqlite?mode=ro"
	mempoolDb    = "chainstate/mempool.sqlite?mode=ro"
)

type Config struct {
	DataDir string
	CMCKey  string
}

func (c Config) validate() {
	// check that DataDir exists
	if _, err := os.Stat(c.DataDir); os.IsNotExist(err) {
		log.Fatalf("Data directory does not exist: %s", c.DataDir)
	}
}

var config Config
var minerAddressMap sync.Map

type DotResponse struct {
	Timestamp          time.Time `db:"timestamp"`
	BitcoinBlockHeight int       `db:"bitcoin_block_height"`
	Dot                string    `db:"dot"`
}

func handleMinerViz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	hubDb := sqlx.MustOpen("sqlite3", filepath.Join(config.DataDir, "hub.sqlite?mode=ro"))
	defer hubDb.Close()

	var d DotResponse
	q := "SELECT timestamp, bitcoin_block_height, dot FROM dots ORDER BY timestamp DESC LIMIT 1"
	if err := hubDb.Get(&d, q); err != nil {
		slog.Warn("Error fetching", "query", q, "error", err)
	}

	// Marshal d as JSON and write it to the response
	if err := json.NewEncoder(w).Encode(d); err != nil {
		slog.Warn("Error encoding JSON", "error", err)
	}
}

func handleMinerPower(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(queryMinerPower()); err != nil {
		slog.Warn("Error encoding JSON", "error", err)
	}
}

func handleMempoolStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	hubDb := sqlx.MustOpen("sqlite3", filepath.Join(config.DataDir, "hub.sqlite?mode=ro"))
	defer hubDb.Close()

	var jsonBlob []byte
	q := "SELECT data FROM mempool_stats ORDER BY timestamp DESC LIMIT 1"
	if err := hubDb.Get(&jsonBlob, q); err != nil {
		slog.Warn("Error fetching", "query", q, "error", err)
	}
	// Write the JSON blob to the response
	w.Write(jsonBlob)
}

func handleMempoolSize(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	hubDb := sqlx.MustOpen("sqlite3", filepath.Join(config.DataDir, "hub.sqlite?mode=ro"))
	defer hubDb.Close()

	type SizeSnapshot struct {
		Timestamp time.Time
		Count     int
	}
	var snapshots []SizeSnapshot
	q := "SELECT timestamp, count FROM mempool_stats ORDER BY timestamp DESC LIMIT 60"
	if err := hubDb.Select(&snapshots, q); err != nil {
		slog.Warn("Error fetching", "query", q, "error", err)
	}
	if err := json.NewEncoder(w).Encode(snapshots); err != nil {
		slog.Warn("Error encoding JSON", "error", err)
	}
}

func handleTxDecode(w http.ResponseWriter, r *http.Request) {
	// Read the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// Decode the hex-encoded transaction
	data, err := hex.DecodeString(string(body))
	var tx codec.Transaction
	if err := tx.Decode(bytes.NewReader(data)); err != nil {
		http.Error(w, "Failed to decode transaction", http.StatusBadRequest)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Encode the decoded transaction as JSON and write to response
	if err := json.NewEncoder(w).Encode(tx); err != nil {
		slog.Warn("Error encoding JSON", "error", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func service() http.Handler {
	// Logger
	logger := httplog.NewLogger("api", httplog.Options{
		// JSON:             true,
		LogLevel:         slog.LevelInfo,
		Concise:          true,
		RequestHeaders:   true,
		MessageFieldName: "message",
	})

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(httplog.RequestLogger(logger))
	r.Use(middleware.Recoverer)

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins: []string{"https://hub.stx.pub"},
		AllowedMethods: []string{"GET", "POST"},
	}))

	// Setup API routes
	r.Get("/miners/viz", handleMinerViz)
	r.Get("/miners/power", handleMinerPower)
	r.Get("/mempool/stats", handleMempoolStats)
	r.Get("/mempool/size", handleMempoolSize)
	r.Post("/tx/decode", handleTxDecode)

	return r
}

func main() {
	flag.Parse()

	// Need exactly one arg that points to the config file
	if flag.NArg() != 1 {
		log.Fatalln("Missing argument. Usage: hub <config file>")
	}

	configFile := flag.Arg(0)
	// Check if the file exists
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		log.Fatalf("Config file does not exist: %s", configFile)
	}
	// Read the file
	configData, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}
	// Parse the TOML data
	if err := toml.Unmarshal(configData, &config); err != nil {
		log.Fatalf("Error parsing TOML: %v", err)
	}
	config.validate()

	// Setup the tables
	createTables(filepath.Join(config.DataDir, "hub.sqlite"))

	// Start the Scheduler
	scheduler := tasks.New()
	defer scheduler.Stop()

	// Add dot task, runs every two minutes
	if _, err := scheduler.Add(&tasks.Task{
		Interval: time.Duration(2 * time.Minute),
		TaskFunc: wrapped("dot task", dotsTask),
		ErrFunc:  errFunc("dotsTask"),
	}); err != nil {
		log.Fatalf("Error adding task: %v", err)
	}

	// Add mempool task, runs every two minutes
	if _, err := scheduler.Add(&tasks.Task{
		Interval: time.Duration(2 * time.Minute),
		TaskFunc: wrapped("mempool task", mempoolTask),
		ErrFunc:  errFunc("mempoolTask"),
	}); err != nil {
		log.Fatalf("Error adding task: %v", err)
	}

	// Add CMC task to update STX price, only if CMCKey is non-empty.
	if config.CMCKey != "" {
		if _, err := scheduler.Add(&tasks.Task{
			Interval: time.Duration(15 * time.Minute),
			TaskFunc: wrapped("CMC task", cmcTask),
			ErrFunc:  errFunc("cmcTask"),
		}); err != nil {
			log.Fatalf("Error adding task: %v", err)
		}
	}

	// Add task to update miner address map, every 30 minutes
	if _, err := scheduler.Add(&tasks.Task{
		Interval: time.Duration(30 * time.Minute),
		TaskFunc: wrapped("Update miner address map task", updateMinerAddressMapTask),
		ErrFunc:  errFunc("updateMinerAddressMap"),
	}); err != nil {
		log.Fatalf("Error adding task: %v", err)
	}
	// Let's run it manually once to populate the map
	if err := updateMinerAddressMapTask(); err != nil {
		slog.Warn("Error running updateMinerAddressMapTask", "error", err)
	}

	// Run dot task at startup, ignore errors for now
	dotsTask()

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	server := &http.Server{Addr: ":8123", Handler: service()}
	go func() {
		log.Println("Starting HTTP server")
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
		log.Println("Server stopped accepting new connections")
	}()

	<-ctx.Done()

	// Shutdown signal with grace period of 30 seconds
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	log.Println("Shutting down server...")
	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("graceful shutdown timed out.. forcing exit.")
	}
}
