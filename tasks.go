package main

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stxpub/codec"
	"github.com/tidwall/gjson"
)

type BlockCost struct {
	ReadLength  int `json:"read_length"`
	ReadCount   int `json:"read_count"`
	WriteLength int `json:"write_length"`
	WriteCount  int `json:"write_count"`
	Runtime     int `json:"runtime"`
}

type AttributeMap map[string]string

func (attrs AttributeMap) fmt() string {
	var w strings.Builder
	w.WriteString("[")
	for k, v := range attrs {
		w.WriteString(fmt.Sprintf("%s=%s, ", k, v))
	}
	w.WriteString("]")
	return w.String()
}

type hashkey struct {
	blockHeight int
	vtxindex    int
}

type BlockCommit struct {
	burnHeaderHash  string
	txid            string
	vtxindex        int
	sender          string
	burnBlockHeight int
	spend           int
	sortitionId     string
	parentBlockPtr  int
	parentVtxindex  int
	memo            string
	// txid of parent block commit
	parent         string
	stacksHeight   int
	blockHash      string
	won            bool
	canonical      bool
	tip            bool
	coinbaseEarned int
	feesEarned     int
	potentialTip   bool
	nextTip        bool
	key            hashkey
	parentKey      hashkey
}

type BlockCommits struct {
	// Map from sortition id to ??
	SortitionFeesMap map[string]int
	// Map from txid to block commit
	AllCommits map[string]*BlockCommit
	// List of block commits by block height
	CommitsByBlock map[int][]*BlockCommit
}

func openDatabases() (*sqlx.DB, *sqlx.DB) {
	dbPath := filepath.Join(config.DataDir, sortitionDb)
	db := sqlx.MustOpen("sqlite3", dbPath)

	dbPath = filepath.Join(config.DataDir, chainstateDb)
	cdb := sqlx.MustOpen("sqlite3", dbPath)

	return db, cdb
}

func getBlockRange(db *sqlx.DB, numBlocks int) (int, int) {
	var startBlock int
	if err := db.Get(&startBlock, "SELECT MAX(block_height) FROM block_commits"); err != nil {
		log.Fatal(err)
	}
	lowerBound := startBlock - numBlocks
	return startBlock, lowerBound
}

// TODO: run this once at startup, write the mapping to disk.
// Populate an in-memory cache and reset it every 24 hours.
func updateMinerAddressMapTask() error {
	query := `SELECT DISTINCT ifnull(payments.recipient,payments.address),marf.block_commits.apparent_sender
	    FROM nakamoto_block_headers left join payments on nakamoto_block_headers.index_block_hash = payments.index_block_hash
	    left join marf.snapshots on nakamoto_block_headers.consensus_hash = marf.snapshots.consensus_hash
	    left join marf.block_commits on marf.block_commits.sortition_id = marf.snapshots.sortition_id
	    and marf.block_commits.block_header_hash = marf.snapshots.winning_stacks_block_hash
	    ORDER BY nakamoto_block_headers.block_height desc
	    limit ?`

	dbPath := filepath.Join(config.DataDir, chainstateDb)
	cdb := sqlx.MustOpen("sqlite3", dbPath)
	defer cdb.Close()

	dbPath = filepath.Join(config.DataDir, sortitionDb)
	cdb.MustExec(fmt.Sprintf("ATTACH DATABASE 'file:%s' AS marf", dbPath))
	rows, err := cdb.Query(query, 10)
	if err != nil {
		slog.Warn("Error query miner addresses", "query", query, "error", err)
		return err
	}
	defer rows.Close()

	// Clear the existing map
	minerAddressMap.Clear()

	var stxAddr, btcAddr string
	for rows.Next() {
		if err := rows.Scan(&stxAddr, &btcAddr); err != nil {
			slog.Warn("Error scanning miner addresses", "error", err)
			break
		}
		btcAddr = strings.Trim(btcAddr, "\"")
		// Don't overwrite
		slog.Debug("Trying to add mapping", "stx", stxAddr, "btc", btcAddr)
		if old, exists := minerAddressMap.LoadOrStore(stxAddr, btcAddr); exists {
			slog.Debug("Skipping mapping", "stx", stxAddr, "old", old)
		}
	}
	return nil
}

type miner struct {
	BitcoinAddress  string
	StacksRecipient string
	BlocksWon       uint
	BtcSpent        uint
	StxEarnt        float32
	WinRate         float32
}

func queryMinerPower() []miner {
	const numBlocks = 144

	db, cdb := openDatabases()
	defer db.Close()
	defer cdb.Close()

	var tip string
	if err := cdb.Get(&tip, "SELECT index_block_hash FROM payments ORDER BY stacks_block_height DESC LIMIT 1"); err != nil {
		log.Fatal(err)
	}

	_, lowerBound := getBlockRange(db, numBlocks)

	query := `WITH RECURSIVE block_ancestors(burn_header_height,parent_block_id,address,burnchain_commit_burn,stx_reward) AS (
    	SELECT nakamoto_block_headers.burn_header_height,nakamoto_block_headers.parent_block_id,payments.address,payments.burnchain_commit_burn,(payments.coinbase + payments.tx_fees_anchored + payments.tx_fees_streamed) AS stx_reward
        FROM nakamoto_block_headers JOIN payments ON nakamoto_block_headers.index_block_hash = payments.index_block_hash WHERE payments.index_block_hash = ?
    	UNION ALL
        SELECT nakamoto_block_headers.burn_header_height,nakamoto_block_headers.parent_block_id,payments.address,payments.burnchain_commit_burn,(payments.coinbase + payments.tx_fees_anchored + payments.tx_fees_streamed) AS stx_reward
        FROM (nakamoto_block_headers JOIN payments ON nakamoto_block_headers.index_block_hash = payments.index_block_hash) JOIN block_ancestors ON nakamoto_block_headers.index_block_hash = block_ancestors.parent_block_id
    )
    SELECT block_ancestors.burn_header_height,block_ancestors.address,block_ancestors.burnchain_commit_burn,block_ancestors.stx_reward
    FROM block_ancestors LIMIT ?`

	rows, err := cdb.Query(query, tip, numBlocks)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	btcSpent := make(map[string]uint)
	stxEarnt := make(map[string]uint)
	addrCounts := make(map[string]uint)
	var numRows uint = 0

	for rows.Next() {
		var burnHeight int
		var commitBurn, stxReward uint
		var address string
		if err := rows.Scan(&burnHeight, &address, &commitBurn, &stxReward); err != nil {
			log.Fatal(err)
		}
		if burnHeight <= lowerBound {
			// log.Println("Skipping", burnHeight, address, commitBurn, stxReward)
			continue
		}
		addrCounts[address] += 1
		btcSpent[address] += commitBurn
		stxEarnt[address] += stxReward
		numRows += 1
	}

	if !rows.NextResultSet() && rows.Err() != nil {
		log.Fatalf("expected more result sets: %v", rows.Err())
	}
	const noSortitionKey = "No Canonical Sortition"
	addrCounts[noSortitionKey] = numBlocks - numRows
	btcSpent[noSortitionKey] = 0
	stxEarnt[noSortitionKey] = 0

	miners := make([]miner, 0, len(addrCounts))
	for addr, won := range addrCounts {
		btcAddr := ""
		if v, ok := minerAddressMap.Load(addr); ok {
			btcAddr = v.(string)
		}
		m := miner{
			StacksRecipient: addr,
			BitcoinAddress:  btcAddr,
			BlocksWon:       won,
			BtcSpent:        btcSpent[addr],
			StxEarnt:        float32(stxEarnt[addr]) / 1_000_000,
			WinRate:         (float32(won) / numBlocks) * 100,
		}
		miners = append(miners, m)
	}
	slices.SortFunc(miners,
		func(a, b miner) int {
			return cmp.Compare(b.BlocksWon, a.BlocksWon)
		})
	return miners
}

func fetchCommitData(db *sqlx.DB, lower_bound_height, start_block int) BlockCommits {
	sortitionFeesMap := make(map[string]int)
	allCommits := make(map[string]*BlockCommit)
	commitsByBlock := make(map[int][]*BlockCommit)
	// Map (block height, vtxindex) tuples to block commit txid
	hashMap := make(map[hashkey]string)

	query := `SELECT
			burn_header_hash,
			txid,
			apparent_sender,
			sortition_id,
			vtxindex,
			block_height,
			burn_fee,
			parent_block_ptr,
			parent_vtxindex,
			memo
	FROM
			block_commits
	WHERE
			block_height BETWEEN ? AND ?
	ORDER BY
			block_height ASC`

	rows, err := db.Query(query, lower_bound_height, start_block)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var commit BlockCommit
		if err := rows.Scan(
			&commit.burnHeaderHash,
			&commit.txid,
			&commit.sender,
			&commit.sortitionId,
			&commit.vtxindex,
			&commit.burnBlockHeight,
			&commit.spend,
			&commit.parentBlockPtr,
			&commit.parentVtxindex,
			&commit.memo); err != nil {
			log.Fatal(err)
		}
		commit.key = hashkey{commit.burnBlockHeight, commit.vtxindex}
		commit.parentKey = hashkey{commit.parentBlockPtr, commit.parentVtxindex}

		allCommits[commit.txid] = &commit
		parent, exists := hashMap[commit.parentKey]
		if exists {
			commit.parent = parent
		}
		hashMap[commit.key] = commit.txid
	}

	if !rows.NextResultSet() && rows.Err() != nil {
		log.Fatalf("expected more result sets: %v", rows.Err())
	}

	// Now that we have all the commits, group them by block
	for _, commit := range allCommits {
		blockHeight := commit.burnBlockHeight
		if _, exists := commitsByBlock[blockHeight]; !exists {
			commitsByBlock[blockHeight] = make([]*BlockCommit, 0)
		}
		commitsByBlock[blockHeight] = append(commitsByBlock[blockHeight], commit)
		sortitionFeesMap[commit.sortitionId] += commit.spend
	}

	return BlockCommits{
		SortitionFeesMap: sortitionFeesMap,
		AllCommits:       allCommits,
		CommitsByBlock:   commitsByBlock,
	}
}

func processWinningBlocks(db *sqlx.DB, cdb *sqlx.DB, lower_bound_height, start_block int, blockCommits BlockCommits) {
	commits := blockCommits.AllCommits
	blockCommitsMap := blockCommits.CommitsByBlock

	for block_height := lower_bound_height; block_height <= start_block; block_height++ {
		var stacks_height int
		var winningBlockTxid, consensus_hash string

		row := db.QueryRow("SELECT winning_block_txid, canonical_stacks_tip_height, consensus_hash FROM snapshots WHERE block_height = ?;",
			block_height)
		if err := row.Scan(&winningBlockTxid, &stacks_height, &consensus_hash); err != nil {
			log.Fatal(err)
		}

		if _, exists := blockCommitsMap[block_height]; !exists {
			log.Printf("No block commits for block height %d\n", block_height)
			continue // skip blocks that don't have any commits
		}
		block_commits := blockCommitsMap[block_height]
		for _, commit := range block_commits {
			commit.stacksHeight = stacks_height
			parent_commit, exists := commits[commit.parent]
			if commit.txid == winningBlockTxid {
				processWinningCommit(cdb, commit, parent_commit, exists, stacks_height, consensus_hash)
			}
		}
	}
}

func processWinningCommit(cdb *sqlx.DB, commit *BlockCommit, parent_commit *BlockCommit, parentExists bool, stacks_height int, consensus_hash string) {
	commit.won = true
	commit.potentialTip = true
	commit.stacksHeight = stacks_height
	if parentExists {
		parent_commit.potentialTip = false
	}
	// If stacks_height > 0, populate coinbase, fee and block size
	if stacks_height > 0 {
		row := cdb.QueryRow("SELECT block_hash, coinbase FROM payments WHERE consensus_hash = ?;", consensus_hash)
		if err := row.Scan(&commit.blockHash, &commit.coinbaseEarned); err != nil {
			slog.Warn("Error fetching coinbase", "consensus_hash", consensus_hash, "error", err)
		}
		if err := cdb.Get(&commit.feesEarned,
			"SELECT tenure_tx_fees FROM nakamoto_block_headers WHERE burn_header_height = ? ORDER BY height_in_tenure DESC LIMIT 1",
			commit.burnBlockHeight); err != nil {
			slog.Warn("No tenure_tx_fees for block", "burnBlockHeight", commit.burnBlockHeight, "error", err)
		}
	}
}

func processCanonicalTip(db *sqlx.DB, start_block int, commits map[string]*BlockCommit) {
	var canonical_tip string
	if err := db.Get(&canonical_tip, "SELECT winning_block_txid FROM snapshots WHERE block_height = ?;", start_block); err != nil {
		log.Fatal(err)
	}
	tip := canonical_tip
	for {
		commit, exists := commits[tip]
		if !exists {
			break
		}
		if tip == canonical_tip {
			commit.tip = true
		}
		commit.canonical = true
		tip = commit.parent
	}
}

func generateGraph(lower_bound_height, start_block int, blockCommits BlockCommits) string {
	var g strings.Builder
	g.WriteString("digraph block_commits {\n")
	g.WriteString("\tgraph [ratio=compress size=\"18,36\" fontsize=28 fontname=monospace]\n")
	g.WriteString("\tnode [color=black fontsize=24 fontname=monospace fillcolor=white penwidth=1 style=\"filled,dashed\"]\n")
	g.WriteString("\tedge [color=black penwidth=1]\n")

	block_commits_map := blockCommits.CommitsByBlock
	sortition_fees_map := blockCommits.SortitionFeesMap
	commits := blockCommits.AllCommits

	last_height := 0
	for block_height := lower_bound_height; block_height <= start_block; block_height++ {
		if _, exists := block_commits_map[block_height]; !exists {
			continue // skip blocks that don't have any commits
		}

		g.WriteString(fmt.Sprintf("\tsubgraph cluster_block_%d {\n", block_height))
		g.WriteString(fmt.Sprintf("URL=\"https://mempool.space/block/%d\"\n", block_height))
		sortition_spend := 0

		for _, commit := range block_commits_map[block_height] {
			if sortition_spend == 0 {
				sortition_spend = sortition_fees_map[commit.sortitionId]
			} else if sortition_spend != sortition_fees_map[commit.sortitionId] {
				log.Printf("Previous sortition spend %d does not match spend %d in commit %s\n",
					sortition_spend, sortition_fees_map[commit.sortitionId], commit.burnHeaderHash)
			}
			attrs := makeNodeAttributes(commit)
			g.WriteString(fmt.Sprintf("\t\tcommit_%s %s\n",
				commit.txid, attrs.fmt()))

			if commit.parent != "" {
				edgeAttrs := makeEdgeAttributes(commit, commits[commit.parent], last_height)
				g.WriteString(fmt.Sprintf("commit_%s -> commit_%s %s\n",
					commit.parent, commit.txid, edgeAttrs.fmt()))
			}
		}
		g.WriteString(fmt.Sprintf("\t\tlabel = \"₿ %d\n💰 %dK sats\"\n",
			block_height, sortition_spend/1000))
		g.WriteString("\t}\n")
		last_height = block_height
	}
	g.WriteString("}\n")

	return g.String()
}

func makeNodeAttributes(commit *BlockCommit) AttributeMap {
	attrs := make(AttributeMap)
	attrs["color"] = "black"
	attrs["penwidth"] = "1"
	attrs["URL"] = `"https://mempool.space/tx/` + commit.txid + `"`
	label := fmt.Sprintf("⛏️ %s, \n🔗 %d\n💸 %dK sats",
		strings.Trim(commit.sender, `"`)[:8], commit.stacksHeight, commit.spend/1000)
	if commit.won {
		attrs["color"] = "blue"
		attrs["penwidth"] = "4"
	}
	if commit.nextTip {
		attrs["color"] = "green"
	}
	if commit.tip {
		attrs["penwidth"] = "8"
	}
	if commit.canonical {
		attrs["style"] = `"filled,solid"`
	}
	if commit.blockHash != "" {
		attrs["URL"] = `"https://explorer.hiro.so/block/0x` + commit.blockHash + `"`
	}
	attrs["label"] = fmt.Sprintf("\"%s\"", label)
	attrs["fillcolor"] = `"` + stringToColor(commit.sender) + `"`
	return attrs
}

func stringToColor(input string) string {
	pastelColors := []string{
		"#E0BBE4", "#957DAD", "#D291BC", "#FEC8D8", "#FFDFD3", // Purples, Pinks
		"#D9EEF5", "#B6E3F4", "#B5EAD7", "#C7F4F4", "#E8F3F8", // Lighter Blues, Grays
		"#F4F1BB", "#D4E09B", "#99C4C8", "#F2D0A9", "#E9D5DA", // Yellows, Peaches
		"#D8E2DC", "#FFE5D9", "#FFCAD4", "#F4ACB7", "#9D8189", // Greens, Reds
	}

	hashBytes := []byte(input)[:8] // first 8 bytes
	index := int(binary.BigEndian.Uint64(hashBytes)) % len(pastelColors)
	return pastelColors[index]
}

func makeEdgeAttributes(commit *BlockCommit, parentCommit *BlockCommit, last_height int) AttributeMap {
	attrs := make(AttributeMap)
	attrs["color"] = "black"
	attrs["penwidth"] = "1"
	if last_height > 0 && parentCommit.burnBlockHeight != last_height {
		attrs["color"] = "red"
		attrs["penwidth"] = "4"
	}
	if commit.canonical {
		attrs["color"] = "blue"
		attrs["penwidth"] = "8"
	}
	return attrs
}

func wrapped(name string, task func() error) func() error {
	return func() error {
		start := time.Now()
		log.Printf("Running %s\n", name)
		defer func() {
			log.Printf("Finished %s in %s.\n", name, time.Since(start))
		}()
		return task()
	}
}

func errFunc(name string) func(e error) {
	return func(e error) {
		if e != nil {
			log.Printf("Task %s failed with: %s\n", name, e)
		}
	}
}

func dotsTask() error {
	db, cdb := openDatabases()
	defer db.Close()
	defer cdb.Close()

	startBlock, lowerBound := getBlockRange(db, 20)
	blockCommits := fetchCommitData(db, lowerBound, startBlock)
	processWinningBlocks(db, cdb, lowerBound, startBlock, blockCommits)
	processCanonicalTip(db, startBlock, blockCommits.AllCommits)
	dot := generateGraph(lowerBound, startBlock, blockCommits)

	hubDb := sqlx.MustOpen("sqlite3", filepath.Join(config.DataDir, "hub.sqlite"))
	defer hubDb.Close()

	_, err := hubDb.Exec("INSERT INTO dots (bitcoin_block_height, dot) VALUES (?, ?)",
		startBlock, dot)
	return err
}

type mempoolTxn struct {
	Txid   string `db:"txid"`
	TxFee  int    `db:"tx_fee"`
	Length int    `db:"length"`
	Age    int    `db:"age"`
	TxBlob string `db:"tx"`
}

type ContractCount struct {
	Contract string
	Count    int
}

type MempoolData struct {
	Popular          []ContractCount
	FeeDistribution  []hdrhistogram.Bracket
	SizeDistribution []hdrhistogram.Bracket
	AgeDistribution  []hdrhistogram.Bracket
}

func mempoolTask() error {
	// ideas for a potential mempool endpoint
	// - number of "old" transactions
	mdb := sqlx.MustOpen("sqlite3", filepath.Join(config.DataDir, mempoolDb))
	defer mdb.Close()

	mempool := []mempoolTxn{}
	if err := mdb.Select(&mempool,
		"SELECT txid, tx_fee, length, (unixepoch() - accept_time) as age, LOWER(HEX(tx)) AS tx FROM mempool"); err != nil {
		log.Fatal(err)
	}
	// Proactively close mdb
	mdb.Close()

	fees := []float32{}
	lengths := []int{}
	txnCounts := make(map[string]int)

	// Technically fee is uncapped, but 1000 STX is a good upper bound
	feeHist := hdrhistogram.New(1, 1_000_000_000, 1)
	// Size can't be more than 2MB. Limit to 10MB to be safe
	sizeHist := hdrhistogram.New(1, 10*1024*1024, 1)
	// Age can't greater than 256 Bitcoin blocks. Limit to 500 to be safe
	ageHist := hdrhistogram.New(1, 500*10*60, 1)

	for _, txn := range mempool {
		feeHist.RecordValue(int64(txn.TxFee))
		sizeHist.RecordValue(int64(txn.Length))
		ageHist.RecordValue(int64(txn.Age))

		// ageHist.RecordValue(txn.)
		fees = append(fees, float32(txn.TxFee)/1_000_000)
		lengths = append(lengths, txn.Length)

		var tx codec.Transaction
		data, err := hex.DecodeString(txn.TxBlob)
		if err != nil {
			log.Printf("Failed to hex decode txid %s, blob %s\n", txn.Txid, txn.TxBlob)
			continue
		}
		err = tx.Decode(bytes.NewReader(data))
		if err != nil {
			log.Printf("Failed to txn decode txid %s, blob %s\n", txn.Txid, txn.TxBlob)
			continue
		}
		if tx.Payload.Transfer != nil {
			txnCounts["simple-token-transfer"] += 1
		} else if tx.Payload.ContractCall != nil {
			name := fmt.Sprintf("%s.%s", tx.Payload.ContractCall.Origin.ToStacks(),
				tx.Payload.ContractCall.Contract)
			txnCounts[name] += 1
		}
	}

	counters := []ContractCount{}
	for k, v := range txnCounts {
		counters = append(counters, ContractCount{k, v})
	}
	slices.SortFunc(counters, func(i, j ContractCount) int {
		// reverse the order to get descending
		return cmp.Compare(j.Count, i.Count)
	})

	hubDb := sqlx.MustOpen("sqlite3", filepath.Join(config.DataDir, "hub.sqlite"))
	defer hubDb.Close()

	var d MempoolData
	d.Popular = counters[:25]
	d.FeeDistribution = feeHist.CumulativeDistribution()
	d.SizeDistribution = sizeHist.CumulativeDistribution()
	d.AgeDistribution = ageHist.CumulativeDistribution()

	// TODO: handle errors
	blob, _ := json.Marshal(d)
	_, err := hubDb.Exec("INSERT INTO mempool_stats (count, data) VALUES (?, ?)",
		len(mempool), blob)
	if err != nil {
		log.Printf("Error inserting mempool stats: %v\n", err)
	}
	return err
}

func cmcTask() error {
	// make a HTTP request to the CoinMarketCap API
	// add header for API key
	// parse the response
	client := &http.Client{}
	req, err := http.NewRequest("GET", "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?id=4847&convert_id=1", nil)
	if err != nil {
		log.Printf("Error fetching STX price: %v\n", err)
		return err
	}

	req.Header.Set("X-CMC_PRO_API_KEY", config.CMCKey)
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	json := string(body)
	price := gjson.Get(json, "data.4847.quote.1.price")

	hubDb := sqlx.MustOpen("sqlite3", filepath.Join(config.DataDir, "hub.sqlite"))
	defer hubDb.Close()

	_, err = hubDb.Exec("INSERT INTO sats_per_stx (price) VALUES (?)",
		price.Num*100_000_000)

	return err
}

func pruneTask() error {
	hubDb := sqlx.MustOpen("sqlite3", filepath.Join(config.DataDir, "hub.sqlite"))
	defer hubDb.Close()

	tx := hubDb.MustBegin()
	defer tx.Rollback()

	tx.MustExec("DELETE FROM mempool_stats WHERE timestamp < datetime('now', '-2 days')")
	tx.MustExec("DELETE FROM dots WHERE timestamp < datetime('now', '-2 days')")
	tx.Commit()

	// Can't vacuum from within a transactions, sqlite panics.
	// See https://www.sqlite.org/lang_vacuum.html
	_, err := hubDb.Exec("VACUUM")
	return err
}
