package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jmoiron/sqlx"
	"github.com/madflojo/tasks"
	"github.com/pelletier/go-toml/v2"
	_ "modernc.org/sqlite"
)

const (
	sortitionDb  = "burnchain/sortition/marf.sqlite?mode=ro"
	chainstateDb = "chainstate/vm/index.sqlite?mode=ro"
	mempoolDb    = "chainstate/mempool.sqlite?mode=ro"
)

type Config struct {
	DataDir     string
	TemplateDir string
}

func (c Config) validate() {
	// check that DataDir exists
	if _, err := os.Stat(c.DataDir); os.IsNotExist(err) {
		log.Fatalf("Data directory does not exist: %s", c.DataDir)
	}
	// check that TemplateDir exists
	if _, err := os.Stat(c.TemplateDir); os.IsNotExist(err) {
		log.Fatalf("Template directory does not exist: %s", c.TemplateDir)
	}
}

var config Config

type DotResponse struct {
	Timestamp          time.Time `db:"timestamp"`
	BitcoinBlockHeight int       `db:"bitcoin_block_height"`
	Dot                string    `db:"dot"`
}

func handleMinerViz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	hubDb := sqlx.MustOpen("sqlite", filepath.Join(config.DataDir, "hub.sqlite?mode=ro"))
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
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if err := json.NewEncoder(w).Encode(queryMinerPower()); err != nil {
		slog.Warn("Error encoding JSON", "error", err)
	}
}

func handleMempoolPopular(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	hubDb := sqlx.MustOpen("sqlite", filepath.Join(config.DataDir, "hub.sqlite?mode=ro"))
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
	w.Header().Set("Access-Control-Allow-Origin", "*")

	hubDb := sqlx.MustOpen("sqlite", filepath.Join(config.DataDir, "hub.sqlite?mode=ro"))
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

func service() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Setup API routes
	r.Get("/miners/viz", handleMinerViz)
	r.Get("/miners/power", handleMinerPower)
	r.Get("/mempool/popular", handleMempoolPopular)
	r.Get("/mempool/size", handleMempoolSize)

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

	if _, err := scheduler.Add(&tasks.Task{
		Interval: time.Duration(15 * time.Minute),
		TaskFunc: wrapped("CMC task", cmcTask),
		ErrFunc:  errFunc("cmcTask"),
	}); err != nil {
		log.Fatalf("Error adding task: %v", err)
	}

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