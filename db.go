package main

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"

	"github.com/jmoiron/sqlx"
)

const (
	sortitionDb  = "burnchain/sortition/marf.sqlite?mode=ro"
	chainstateDb = "chainstate/vm/index.sqlite?mode=ro"
	mempoolDb    = "chainstate/mempool.sqlite?mode=ro"

	dotsSchema = `
	CREATE TABLE IF NOT EXISTS dots (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
	bitcoin_block_height INTEGER,
	dot TEXT NOT NULL
	);`

	mempoolStatsSchema = `
	CREATE TABLE IF NOT EXISTS mempool_stats (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
	count INTEGER,
	data JSONB
	);`

	stxPriceSchema = `
	CREATE TABLE IF NOT EXISTS sats_per_stx (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
	price REAL
	);`
)

type CostVector struct {
	ReadLength  int `json:"read_length"`
	ReadCount   int `json:"read_count"`
	WriteLength int `json:"write_length"`
	WriteCount  int `json:"write_count"`
	Runtime     int `json:"runtime"`
}

type Block struct {
	BlockSize        int        `db:"block_size"`
	Cost             CostVector `db:"cost"`
	TenureCost       CostVector `db:"total_tenure_cost"`
	TenureChanged    bool       `db:"tenure_changed"`
	TenureTxFees     int        `db:"tenure_tx_fees"`
	BlockHeight      int        `db:"block_height"`
	BurnHeaderHeight int        `db:"burn_header_height"`
}

// Scan implements the sql.Scanner interface for CostVector
func (cv *CostVector) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	var b []byte
	switch v := value.(type) {
	case string:
		b = []byte(v)
	case []byte:
		b = v
	default:
		return errors.New("type assertion failed")
	}

	return json.Unmarshal(b, &cv)
}

// Value implements the driver.Valuer interface for CostVector
func (cv CostVector) Value() (driver.Value, error) {
	b, err := json.Marshal(cv)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func createTables(dbPath string) {
	db := sqlx.MustOpen("sqlite3", dbPath)
	defer db.Close()

	tx := db.MustBegin()
	db.MustExec(dotsSchema)
	db.MustExec(mempoolStatsSchema)
	db.MustExec(stxPriceSchema)
	tx.Commit()
}

func getBlocks() []Block {
	dbPath := filepath.Join(config.DataDir, chainstateDb)
	db := sqlx.MustOpen("sqlite3", dbPath)
	defer db.Close()

	var maxBurnHeight int
	err := db.Get(&maxBurnHeight, "SELECT MAX(burn_header_height) FROM nakamoto_block_headers")
	if err != nil {
		slog.Error("Error fetching max burn height", "error", err)
		return nil
	}

	const query = `
	SELECT
		block_size,
		cost,
		total_tenure_cost,
		tenure_changed,
		tenure_tx_fees,
		block_height,
		burn_header_height
	FROM nakamoto_block_headers
	WHERE burn_header_height > ?
	ORDER BY block_height ASC
	`
	var blocks []Block
	err = db.Select(&blocks, query, maxBurnHeight-10)
	if err != nil {
		slog.Error("Error fetching blocks", "error", err)
		return nil
	}
	return blocks
}
