package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	configDB             *sql.DB
	stopChan             = make(chan struct{})
	wg                   sync.WaitGroup
	configMu             sync.RWMutex
	cdcInterval          = 10 * time.Second
	verificationInterval = 5 * time.Minute
	parallelWorkers      = 5
	batchSize            = 1000
	verificationRowCount = 10
	maxRetryAttempts     = 3
	retryDelay           = 30 * time.Second

	// In-progress tracking
	syncingMappings = make(map[int]bool)
	syncingMu       sync.Mutex

	// Logging
	logFile *os.File
)

// Flow represents source-dest connection details
type Flow struct {
	FlowID       int
	FlowName     string
	SourceServer string
	SourcePort   int
	SourceUser   string
	SourcePass   string
	DestServer   string
	DestPort     int
	DestUser     string
	DestPass     string
}

// TableMapping represents a source-dest table mapping
type TableMapping struct {
	MappingID        int
	FlowID           int
	SourceDatabase   string
	SourceSchema     string
	SourceTable      string
	DestDatabase     string
	DestSchema       string
	DestTable        string
	PrimaryKeyColumn string
	IsFullSync       bool
	IsEnabled        bool
	CDCEnabled       bool
	LastCDCLSN       sql.NullString
	LastFullSyncAt   sql.NullTime
	LastCDCSyncAt    sql.NullTime
	SourceConnString string
	DestConnString   string
}

// SyncStatus represents current sync status
type SyncStatus struct {
	StatusID         int64
	MappingID        int
	SyncType         string
	Status           string
	RecordsProcessed int64
	RecordsFailed    int64
	StartedAt        time.Time
	CompletedAt      sql.NullTime
	ErrorMessage     sql.NullString
	LastProcessedPK  sql.NullString
}

func initLogging() error {
	// Try /var/log/dbaops-dataflow first, fallback to ./logs
	logDir := "/var/log/dbaops-dataflow"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		logDir = "./logs"
		if err := os.MkdirAll(logDir, 0755); err != nil {
			log.Printf("Warning: Failed to create log directory, using stdout only: %v", err)
			log.SetOutput(os.Stdout)
			log.SetFlags(log.LstdFlags | log.Lshortfile)
			return nil
		}
	}

	// Daily log file: dbaops-dataflow-YYYY-MM-DD.log
	logFileName := fmt.Sprintf("dbaops-dataflow-%s.log", time.Now().Format("2006-01-02"))
	logPath := filepath.Join(logDir, logFileName)

	// Open log file (append mode)
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Warning: Failed to open log file, using stdout only: %v", err)
		log.SetOutput(os.Stdout)
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		return nil
	}

	// Write only to file (not stdout)
	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	logFile = file
	log.Printf("Logging initialized: %s", logPath)
	return nil
}

func init() {
	// Initialize logging first
	if err := initLogging(); err != nil {
		log.Printf("Warning: Logging initialization failed: %v", err)
	}

	configServer := getEnv("CONFIG_SERVER", "localhost")
	configUser := getEnv("CONFIG_USER", "sa")
	configPass := getEnv("CONFIG_PASS", "")
	if configPass == "" {
		log.Fatal("CONFIG_PASS environment variable required (only config DB credentials needed, source/dest are in flows table)")
	}

	log.Printf("Step 1: Connecting to configuration database (server=%s, user=%s)", configServer, configUser)
	connStr := fmt.Sprintf("server=%s;user id=%s;password=%s;database=dbaops_dataflow;encrypt=disable",
		configServer, configUser, configPass)

	var err error
	configDB, err = sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatalf("Step 1 FAILED: Failed to open config database connection: %v", err)
	}

	log.Printf("Step 2: Pinging configuration database...")
	if err := configDB.Ping(); err != nil {
		log.Fatalf("Step 2 FAILED: Failed to ping config database: %v", err)
	}

	configDB.SetMaxOpenConns(50)
	configDB.SetMaxIdleConns(10)
	configDB.SetConnMaxLifetime(5 * time.Minute)

	log.Printf("Step 3: Configuration database connection established successfully")
	log.Printf("Step 4: Loading configuration from config table...")
	loadConfiguration()
	log.Printf("Step 5: Initialization complete")
}

func loadConfiguration() {
	configMu.Lock()
	defer configMu.Unlock()

	// Load CDC check interval
	if val := getConfigValue("cdc_check_interval_seconds"); val != "" {
		if seconds, err := strconv.Atoi(val); err == nil && seconds > 0 {
			cdcInterval = time.Duration(seconds) * time.Second
		}
	}

	// Load verification interval
	if val := getConfigValue("verification_interval_minutes"); val != "" {
		if minutes, err := strconv.Atoi(val); err == nil && minutes > 0 {
			verificationInterval = time.Duration(minutes) * time.Minute
		}
	}

	// Load parallel scheduler count
	if val := getConfigValue("parallel_scheduler_count"); val != "" {
		if count, err := strconv.Atoi(val); err == nil && count > 0 {
			parallelWorkers = count
		}
	}

	// Load batch size
	if val := getConfigValue("batch_size"); val != "" {
		if size, err := strconv.Atoi(val); err == nil && size > 0 {
			batchSize = size
		}
	}

	// Load max retry attempts
	if val := getConfigValue("max_retry_attempts"); val != "" {
		if attempts, err := strconv.Atoi(val); err == nil && attempts > 0 {
			maxRetryAttempts = attempts
		}
	}

	// Load retry delay
	if val := getConfigValue("retry_delay_seconds"); val != "" {
		if seconds, err := strconv.Atoi(val); err == nil && seconds > 0 {
			retryDelay = time.Duration(seconds) * time.Second
		}
	}

	// Load verification row count
	if val := getConfigValue("verification_row_count"); val != "" {
		if count, err := strconv.Atoi(val); err == nil && count > 0 {
			verificationRowCount = count
		}
	}
}

func getConfigValue(key string) string {
	var value string
	err := configDB.QueryRow("SELECT config_value FROM config WHERE config_key = @p1", key).Scan(&value)
	if err != nil {
		return ""
	}
	return value
}

func main() {
	log.Println("========================================")
	log.Println("DBAOps DataFlow Service Starting...")
	log.Println("========================================")

	log.Printf("Step 6: Starting configuration reloader goroutine...")
	wg.Add(1)
	go configReloader()

	log.Printf("Step 7: Starting sync loop goroutine (CDC interval: %v)...", cdcInterval)
	wg.Add(1)
	go syncLoop()

	log.Printf("Step 8: Starting verification loop goroutine (interval: %v)...", verificationInterval)
	wg.Add(1)
	go verificationLoop()

	log.Println("Step 9: All goroutines started. Service is running...")
	log.Println("========================================")

	select {
	case <-stopChan:
		log.Println("========================================")
		log.Println("Stopping DBAOps DataFlow...")
		log.Println("========================================")
	}

	close(stopChan)
	wg.Wait()

	configDB.Close()
	if logFile != nil {
		logFile.Close()
	}
	log.Println("Shutdown complete")
}

func configReloader() {
	defer wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			loadConfiguration()
		}
	}
}

func syncLoop() {
	defer wg.Done()
	log.Printf("[SYNC_LOOP] Sync loop started (interval: %v)", cdcInterval)

	// Panic recovery for the entire loop
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[SYNC_LOOP] PANIC in syncLoop recovered: %v", r)
			// Restart the loop after a delay
			time.Sleep(5 * time.Second)
			wg.Add(1)
			go syncLoop()
		}
	}()

	configMu.RLock()
	interval := cdcInterval
	configMu.RUnlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			log.Printf("[SYNC_LOOP] Sync loop stopped")
			return
		case <-ticker.C:
			log.Printf("[SYNC_LOOP] Tick received, processing mappings...")
			// Panic recovery for each iteration
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[SYNC_LOOP] PANIC in processAllMappings recovered: %v", r)
					}
				}()
				processAllMappings()
			}()

			configMu.RLock()
			newInterval := cdcInterval
			configMu.RUnlock()
			if newInterval != interval {
				log.Printf("[SYNC_LOOP] Interval changed from %v to %v, updating ticker...", interval, newInterval)
				ticker.Stop()
				ticker = time.NewTicker(newInterval)
				interval = newInterval
			}
		}
	}
}

func verificationLoop() {
	defer wg.Done()
	log.Printf("[VERIFICATION_LOOP] Verification loop started (interval: %v)", verificationInterval)

	// Panic recovery for the entire loop
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[VERIFICATION_LOOP] PANIC in verificationLoop recovered: %v", r)
			// Restart the loop after a delay
			time.Sleep(5 * time.Second)
			wg.Add(1)
			go verificationLoop()
		}
	}()

	configMu.RLock()
	interval := verificationInterval
	configMu.RUnlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			log.Printf("[VERIFICATION_LOOP] Verification loop stopped")
			return
		case <-ticker.C:
			log.Printf("[VERIFICATION_LOOP] Tick received, performing verifications...")
			// Panic recovery for each iteration
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[VERIFICATION_LOOP] PANIC in performAllVerifications recovered: %v", r)
					}
				}()
				performAllVerifications()
			}()

			configMu.RLock()
			newInterval := verificationInterval
			configMu.RUnlock()
			if newInterval != interval {
				log.Printf("[VERIFICATION_LOOP] Interval changed from %v to %v, updating ticker...", interval, newInterval)
				ticker.Stop()
				ticker = time.NewTicker(newInterval)
				interval = newInterval
			}
		}
	}
}

func processAllMappings() {
	log.Printf("[SYNC] Starting to process all enabled mappings...")
	mappings, err := getEnabledMappings()
	if err != nil {
		log.Printf("[SYNC] ERROR: Failed to get enabled mappings: %v", err)
		return
	}

	if len(mappings) == 0 {
		log.Printf("[SYNC] No enabled mappings found, skipping...")
		return
	}

	log.Printf("[SYNC] Found %d enabled mapping(s) to process", len(mappings))

	configMu.RLock()
	workers := parallelWorkers
	configMu.RUnlock()

	if workers > len(mappings) {
		workers = len(mappings)
	}

	log.Printf("[SYNC] Using %d parallel worker(s) to process mappings", workers)

	jobs := make(chan TableMapping, len(mappings))
	var wgWorkers sync.WaitGroup

	for i := 0; i < workers; i++ {
		wgWorkers.Add(1)
		go func(workerID int) {
			defer wgWorkers.Done()
			for mapping := range jobs {
				log.Printf("[SYNC] Worker %d: Processing mapping_id=%d (source: %s.%s.%s -> dest: %s.%s.%s)",
					workerID, mapping.MappingID,
					mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
				processMapping(mapping)
			}
		}(i)
	}

	for _, mapping := range mappings {
		jobs <- mapping
	}
	close(jobs)

	wgWorkers.Wait()
	log.Printf("[SYNC] Completed processing all mappings")
}

func processMapping(mapping TableMapping) {
	// Panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in processMapping (mapping_id=%d) recovered: %v", mapping.MappingID, r)
			flow, _ := getFlowByID(mapping.FlowID)
			writeLog(&mapping.FlowID, &mapping.MappingID, "SYNC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Panic recovered: %v",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, r),
				"ERROR", nil, nil)
		}
		syncingMu.Lock()
		delete(syncingMappings, mapping.MappingID)
		syncingMu.Unlock()
	}()

	syncingMu.Lock()
	if syncingMappings[mapping.MappingID] {
		syncingMu.Unlock()
		return
	}
	syncingMappings[mapping.MappingID] = true
	syncingMu.Unlock()

	sourceDB, err := connectToServer(mapping.SourceConnString)
	if err != nil {
		flow, _ := getFlowByID(mapping.FlowID)
		writeLog(&mapping.FlowID, &mapping.MappingID, "CONNECTION",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to connect to source server: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}
	defer sourceDB.Close()

	updateLastConnected(mapping.FlowID)

	// Check if full sync is needed - CDC must wait for full sync to complete
	if needsFullSync(mapping) {
		log.Printf("[SYNC] mapping_id=%d: Full sync requested (is_full_sync=1), performing full table copy...", mapping.MappingID)
		performFullSync(mapping, sourceDB)
		// After full sync, continue with CDC if enabled
		if mapping.CDCEnabled && mapping.IsEnabled {
			log.Printf("[SYNC] mapping_id=%d: Full sync completed, continuing with CDC sync...", mapping.MappingID)
			processCDC(mapping, sourceDB)
		}
	} else if mapping.CDCEnabled && mapping.IsEnabled {
		// Before CDC, check if full sync is requested in database
		// CDC must wait for full sync to complete
		var isFullSyncInDB bool
		err := configDB.QueryRow("SELECT is_full_sync FROM table_mappings WHERE mapping_id = @p1", mapping.MappingID).Scan(&isFullSyncInDB)
		if err != nil {
			log.Printf("[SYNC] mapping_id=%d: Error checking is_full_sync status: %v", mapping.MappingID, err)
			return
		}
		
		if isFullSyncInDB {
			log.Printf("[SYNC] mapping_id=%d: Full sync is requested (is_full_sync=1), CDC will wait for full sync to complete in next cycle", mapping.MappingID)
			flow, _ := getFlowByID(mapping.FlowID)
			writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - CDC skipped, waiting for full sync to complete",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
				"INFO", nil, nil)
			return
		}
		
		log.Printf("[SYNC] mapping_id=%d: CDC enabled, checking for CDC changes (last_lsn: %s)...", mapping.MappingID, mapping.LastCDCLSN.String)
		processCDC(mapping, sourceDB)
	} else {
		log.Printf("[SYNC] mapping_id=%d: No action needed (is_full_sync=0, cdc_enabled=0 or is_enabled=0)", mapping.MappingID)
	}
}

func performAllVerifications() {
	log.Printf("[VERIFICATION] Starting to perform verifications for all enabled mappings...")
	mappings, err := getEnabledMappings()
	if err != nil {
		log.Printf("[VERIFICATION] ERROR: Failed to get enabled mappings: %v", err)
		return
	}

	if len(mappings) == 0 {
		log.Printf("[VERIFICATION] No enabled mappings found, skipping verification...")
		return
	}

	// Filter out mappings that are currently running full sync
	filteredMappings := make([]TableMapping, 0)
	syncingMu.Lock()
	for _, mapping := range mappings {
		if syncingMappings[mapping.MappingID] {
			log.Printf("[VERIFICATION] Skipping mapping_id=%d - full sync in progress", mapping.MappingID)
			continue
		}
		filteredMappings = append(filteredMappings, mapping)
	}
	syncingMu.Unlock()

	if len(filteredMappings) == 0 {
		log.Printf("[VERIFICATION] No mappings available for verification (all are in full sync), skipping...")
		return
	}

	log.Printf("[VERIFICATION] Found %d enabled mapping(s) to verify (filtered %d in full sync)", len(filteredMappings), len(mappings)-len(filteredMappings))

	configMu.RLock()
	workers := parallelWorkers
	configMu.RUnlock()

	if workers > len(mappings) {
		workers = len(mappings)
	}

	log.Printf("[VERIFICATION] Using %d parallel worker(s) to perform verifications", workers)

	jobs := make(chan TableMapping, len(mappings))
	var wgWorkers sync.WaitGroup

	for i := 0; i < workers; i++ {
		wgWorkers.Add(1)
		go func(workerID int) {
			defer wgWorkers.Done()
			for mapping := range jobs {
				log.Printf("[VERIFICATION] Worker %d: Starting verification for mapping_id=%d (source: %s.%s.%s -> dest: %s.%s.%s)",
					workerID, mapping.MappingID,
					mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
				sourceDB, err := connectToServer(mapping.SourceConnString)
				if err != nil {
					log.Printf("[VERIFICATION] Worker %d: ERROR - Failed to connect to source for mapping_id=%d: %v", workerID, mapping.MappingID, err)
					continue
				}
				performVerification(mapping, sourceDB)
				sourceDB.Close()
				log.Printf("[VERIFICATION] Worker %d: Completed verification for mapping_id=%d", workerID, mapping.MappingID)
			}
		}(i)
	}

	for _, mapping := range filteredMappings {
		jobs <- mapping
	}
	close(jobs)

	wgWorkers.Wait()
	log.Printf("[VERIFICATION] Completed all verifications")
}

func needsFullSync(mapping TableMapping) bool {
	return mapping.IsFullSync
}

func performFullSync(mapping TableMapping, sourceDB *sql.DB) {
	// Panic recovery for full sync
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in performFullSync (mapping_id=%d) recovered: %v", mapping.MappingID, r)
			flow, _ := getFlowByID(mapping.FlowID)
			writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Panic recovered: %v",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, r),
				"ERROR", nil, nil)
		}
	}()

	// Get flow details for log messages
	flow, err := getFlowByID(mapping.FlowID)
	if err != nil {
		log.Printf("[FULL_SYNC] Failed to get flow details for mapping_id=%d: %v", mapping.MappingID, err)
		return
	}

	writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
		fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Full sync started",
			flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
			mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
		"INFO", nil, nil)

	// Get and save minimum LSN before full sync (so CDC can continue from correct point after full sync)
	if mapping.CDCEnabled {
		log.Printf("[FULL_SYNC] mapping_id=%d: CDC enabled, getting minimum LSN before full sync...", mapping.MappingID)
		minLSN := getMinLSN(mapping, sourceDB)
		if minLSN != "" {
			log.Printf("[FULL_SYNC] mapping_id=%d: Saving minimum LSN: %s", mapping.MappingID, minLSN)
			updateLastLSN(mapping.MappingID, minLSN)
		} else {
			log.Printf("[FULL_SYNC] mapping_id=%d: WARNING - Could not get minimum LSN (CDC may not be enabled on table)", mapping.MappingID)
		}
	}

	if !validateSchema(mapping, sourceDB) {
		log.Printf("[FULL_SYNC] Step 1 FAILED: Schema validation failed for mapping_id=%d", mapping.MappingID)
		writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Schema validation failed",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
			"ERROR", nil, nil)
		return
	}

	log.Printf("[FULL_SYNC] Step 1 SUCCESS: Schema validation passed for mapping_id=%d", mapping.MappingID)

	log.Printf("[FULL_SYNC] Step 2: Connecting to destination server for mapping_id=%d...", mapping.MappingID)
	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		log.Printf("[FULL_SYNC] Step 2 FAILED: Failed to connect to destination server for mapping_id=%d: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to connect to destination server: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}
	defer destDB.Close()
	log.Printf("[FULL_SYNC] Step 2 SUCCESS: Connected to destination server for mapping_id=%d", mapping.MappingID)

	log.Printf("[FULL_SYNC] Step 3: Switching to destination database [%s] for mapping_id=%d...", mapping.DestDatabase, mapping.MappingID)
	_, err = destDB.Exec(fmt.Sprintf("USE [%s]", mapping.DestDatabase))
	if err != nil {
		log.Printf("[FULL_SYNC] Step 3 FAILED: Failed to switch to destination database for mapping_id=%d: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to switch to destination database: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}
	log.Printf("[FULL_SYNC] Step 3 SUCCESS: Switched to destination database [%s] for mapping_id=%d", mapping.DestDatabase, mapping.MappingID)

	startTime := time.Now()

	log.Printf("[FULL_SYNC] Step 4: Getting identity columns for mapping_id=%d...", mapping.MappingID)
	identityColumns, err := getIdentityColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	if err != nil {
		log.Printf("[FULL_SYNC] Step 4 WARNING: Failed to get identity columns for mapping_id=%d: %v", mapping.MappingID, err)
	} else {
		log.Printf("[FULL_SYNC] Step 4 SUCCESS: Found %d identity column(s) for mapping_id=%d", len(identityColumns), mapping.MappingID)
	}

	// Only exclude identity columns from SELECT (timestamp will be included)
	excludeMap := make(map[string]bool)
	for _, col := range identityColumns {
		excludeMap[strings.ToLower(col)] = true
	}

	log.Printf("[FULL_SYNC] Step 5: Getting source table columns for mapping_id=%d...", mapping.MappingID)
	sourceCols, err := getTableColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	if err != nil {
		log.Printf("[FULL_SYNC] Step 5 FAILED: Failed to get source columns for mapping_id=%d: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to get source columns: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}
	log.Printf("[FULL_SYNC] Step 5 SUCCESS: Found %d source column(s) for mapping_id=%d", len(sourceCols), mapping.MappingID)

	columns := make([]string, 0)
	// Ensure PrimaryKeyColumn is first
	pkFound := false
	for _, col := range sourceCols {
		if strings.EqualFold(col, mapping.PrimaryKeyColumn) {
			columns = append(columns, col)
			pkFound = true
			break
		}
	}
	// Add other columns (excluding timestamp, identity, and already added PK)
	for _, col := range sourceCols {
		if !excludeMap[strings.ToLower(col)] && !strings.EqualFold(col, mapping.PrimaryKeyColumn) {
			columns = append(columns, col)
		}
	}
	if !pkFound {
		writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Primary key column %s not found in source table",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, mapping.PrimaryKeyColumn),
			"ERROR", nil, nil)
		return
	}

	if len(columns) == 0 {
		writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: No columns to sync (all are timestamp)",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
			"ERROR", nil, nil)
		return
	}

	selectQuery := fmt.Sprintf(`
		SELECT %s FROM [%s].[%s].[%s]
		ORDER BY %s
	`, strings.Join(columns, ", "),
		mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable, mapping.PrimaryKeyColumn)

	log.Printf("[FULL_SYNC] Step 6: Marking mapping_id=%d as processing (setting is_full_sync=0)...", mapping.MappingID)
	// Mark as processing immediately to prevent re-triggering
	configDB.Exec("UPDATE table_mappings SET is_full_sync = 0 WHERE mapping_id = @p1", mapping.MappingID)

	// 2. Prepare temp file (no format file needed)
	tmpFile := filepath.Join(os.TempDir(), fmt.Sprintf("full_sync_%d_%d.dat", mapping.FlowID, mapping.MappingID))
	log.Printf("[FULL_SYNC] Step 8: Preparing temporary file: %s for mapping_id=%d", tmpFile, mapping.MappingID)
	defer os.Remove(tmpFile)

	// 3. Truncate destination table
	log.Printf("[FULL_SYNC] Step 9: Truncating destination table [%s].[%s].[%s] for mapping_id=%d...",
		mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, mapping.MappingID)
	truncateQuery := fmt.Sprintf("TRUNCATE TABLE [%s].[%s].[%s]",
		mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	_, err = destDB.Exec(truncateQuery)
	if err != nil {
		log.Printf("[FULL_SYNC] Step 9 WARNING: Failed to truncate dest table for mapping_id=%d (may not exist): %v", mapping.MappingID, err)
	} else {
		log.Printf("[FULL_SYNC] Step 9 SUCCESS: Destination table truncated for mapping_id=%d", mapping.MappingID)
	}

	// 4. Source to Temp File (bcp queryout) - Native mode
	log.Printf("[FULL_SYNC] Step 10: Starting BCP export from source server for mapping_id=%d...", mapping.MappingID)
	bcpOutCmd := fmt.Sprintf(`bcp "%s" queryout "%s" -n -S "%s,%d" -U "%s" -P "%s" -d "%s" -u`,
		strings.ReplaceAll(selectQuery, "\n", " "),
		tmpFile, flow.SourceServer, flow.SourcePort, flow.SourceUser, flow.SourcePass, mapping.SourceDatabase)

	bcpOutLog := fmt.Sprintf(`bcp "%s" queryout "%s" -n -S "%s,%d" -U "%s" -P "****" -d "%s" -u`,
		strings.ReplaceAll(selectQuery, "\n", " "), tmpFile, flow.SourceServer, flow.SourcePort, flow.SourceUser, mapping.SourceDatabase)
	log.Printf("[FULL_SYNC] Step 10: BCP export command: %s", bcpOutLog)

	// Set timeout for BCP export (2 hours max)
	ctxOut, cancelOut := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancelOut()
	cmdOut := exec.CommandContext(ctxOut, "sh", "-c", bcpOutCmd)

	log.Printf("[FULL_SYNC] Step 10: Executing BCP export command for mapping_id=%d (timeout: 2 hours)...", mapping.MappingID)
	outputOut, err := cmdOut.CombinedOutput()
	outputOutStr := string(outputOut)
	if err != nil {
		if ctxOut.Err() == context.DeadlineExceeded {
			errorMsg := "BCP export timeout after 2 hours"
			log.Printf("[FULL_SYNC] Step 10 FAILED: %s for mapping_id=%d", errorMsg, mapping.MappingID)
			writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: %s",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, errorMsg),
				"ERROR", nil, nil)
		} else {
			errorMsg := fmt.Sprintf("BCP export failed: %v", err)
			log.Printf("[FULL_SYNC] Step 10 FAILED: BCP export failed for mapping_id=%d: %v, output: %s", mapping.MappingID, err, outputOutStr)
			writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: %s",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, errorMsg),
				"ERROR", nil, nil)
		}
		return
	}

	// Parse export output for row count
	exportRows := parseBCPRowCount(outputOutStr)
	log.Printf("[FULL_SYNC] Step 10 SUCCESS: Export completed - %d rows exported for mapping_id=%d", exportRows, mapping.MappingID)

	// 5. Temp File to Destination (bcp in) - Native mode, no format file
	log.Printf("[FULL_SYNC] Step 11: Starting BCP import to destination server for mapping_id=%d...", mapping.MappingID)
	bcpInCmd := fmt.Sprintf(`bcp "[%s].[%s]" in "%s" -n -E -S "%s,%d" -U "%s" -P "%s" -d "%s" -b %d -u`,
		mapping.DestSchema, mapping.DestTable,
		tmpFile, flow.DestServer, flow.DestPort, flow.DestUser, flow.DestPass, mapping.DestDatabase, batchSize)

	bcpInLog := fmt.Sprintf(`bcp "[%s].[%s]" in "%s" -n -E -S "%s,%d" -U "%s" -P "****" -d "%s" -b %d -u`,
		mapping.DestSchema, mapping.DestTable, tmpFile, flow.DestServer, flow.DestPort, flow.DestUser, mapping.DestDatabase, batchSize)
	log.Printf("[FULL_SYNC] Step 11: BCP import command: %s", bcpInLog)

	// Set timeout for BCP import (2 hours max)
	ctxIn, cancelIn := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancelIn()
	cmdIn := exec.CommandContext(ctxIn, "sh", "-c", bcpInCmd)

	log.Printf("[FULL_SYNC] Step 11: Executing BCP import command for mapping_id=%d (timeout: 2 hours, batch_size: %d)...", mapping.MappingID, batchSize)
	outputIn, err := cmdIn.CombinedOutput()
	outputInStr := string(outputIn)
	if err != nil {
		if ctxIn.Err() == context.DeadlineExceeded {
			errorMsg := "BCP import timeout after 2 hours"
			log.Printf("[FULL_SYNC] Step 11 FAILED: %s for mapping_id=%d", errorMsg, mapping.MappingID)
			writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: %s",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, errorMsg),
				"ERROR", nil, nil)
		} else {
			errorMsg := fmt.Sprintf("BCP import failed: %v", err)
			log.Printf("[FULL_SYNC] Step 11 FAILED: BCP import failed for mapping_id=%d: %v, output: %s", mapping.MappingID, err, outputInStr)
			writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: %s",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, errorMsg),
				"ERROR", nil, nil)
		}
		return
	}

	// Parse import output for row count
	importRows := parseBCPRowCount(outputInStr)
	log.Printf("[FULL_SYNC] Step 11 SUCCESS: Import completed - %d rows imported for mapping_id=%d", importRows, mapping.MappingID)
	log.Printf("[FULL_SYNC] Step 11: BCP import output: %s", outputInStr)

	// Verify row count matches
	if exportRows > 0 && importRows != exportRows {
		log.Printf("[FULL_SYNC] Step 12 WARNING: Row count mismatch for mapping_id=%d! Exported: %d, Imported: %d", mapping.MappingID, exportRows, importRows)
		writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Warning: Row count mismatch, exported %d, imported %d",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, exportRows, importRows),
			"WARNING", nil, nil)
	} else {
		log.Printf("[FULL_SYNC] Step 12 SUCCESS: Row count verified - exported: %d, imported: %d for mapping_id=%d", exportRows, importRows, mapping.MappingID)
	}

	// Update flags and log
	log.Printf("[FULL_SYNC] Step 13: Updating last_full_sync_at timestamp for mapping_id=%d...", mapping.MappingID)
	updateLastFullSync(mapping.MappingID)
	writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
		fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Full sync completed, last_full_sync_at updated",
			flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
			mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
		"SUCCESS", nil, nil)

	// Update CDC LSN after full sync (get current max LSN to continue CDC from there)
	if mapping.CDCEnabled {
		log.Printf("[FULL_SYNC] Step 14: CDC enabled, getting maximum LSN after full sync for mapping_id=%d...", mapping.MappingID)
		maxLSN := getMaxLSN(mapping, sourceDB)
		if maxLSN != "" {
			log.Printf("[FULL_SYNC] Step 14 SUCCESS: Saving maximum LSN after full sync: %s for mapping_id=%d", maxLSN, mapping.MappingID)
			updateLastLSN(mapping.MappingID, maxLSN)
		} else {
			log.Printf("[FULL_SYNC] Step 14 WARNING: Could not get maximum LSN after full sync for mapping_id=%d (check getMaxLSN logs above for details)", mapping.MappingID)
		}
	}

	duration := time.Since(startTime)
	durationMs := int64(duration.Milliseconds())
	recordsCount := importRows

	log.Printf("[FULL_SYNC] COMPLETED: Full sync finished for mapping_id=%d, duration: %v, exported: %d rows, imported: %d rows",
		mapping.MappingID, duration, exportRows, importRows)
	writeLog(&mapping.FlowID, &mapping.MappingID, "FULL_SYNC",
		fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Full sync completed, %d records transferred",
			flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
			mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, recordsCount),
		"SUCCESS", &recordsCount, &durationMs)
}

func processCDC(mapping TableMapping, sourceDB *sql.DB) {
	// Panic recovery for CDC processing
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in processCDC (mapping_id=%d) recovered: %v", mapping.MappingID, r)
			flow, _ := getFlowByID(mapping.FlowID)
			writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Panic recovered: %v",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, r),
				"ERROR", nil, nil)
		}
	}()

	// Get flow details for log messages
	flow, err := getFlowByID(mapping.FlowID)
	if err != nil {
		log.Printf("[CDC] Failed to get flow details for mapping_id=%d: %v", mapping.MappingID, err)
		return
	}

	if !isCDCEnabled(mapping, sourceDB) {
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Warning: CDC not enabled on source table",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
			"WARNING", nil, nil)
		return
	}

	writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
		fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - CDC sync started",
			flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
			mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
		"INFO", nil, nil)

	log.Printf("[CDC] mapping_id=%d: Step 1 - Getting last LSN...", mapping.MappingID)
	lastLSN := mapping.LastCDCLSN.String
	if lastLSN == "" {
		lastLSN = getMinLSN(mapping, sourceDB)
		if lastLSN == "" {
			log.Printf("[CDC] mapping_id=%d: Step 1 WARNING - No CDC data available", mapping.MappingID)
			writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - CDC sync completed, no changes found",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
				"SUCCESS", nil, nil)
			return
		}
		// Save initial LSN so we don't keep starting from the same point
		log.Printf("[CDC] mapping_id=%d: Step 1 - Saving initial LSN: %s", mapping.MappingID, lastLSN)
		updateLastLSN(mapping.MappingID, lastLSN)
	}
	log.Printf("[CDC] mapping_id=%d: Step 1 SUCCESS - Last LSN: %s", mapping.MappingID, lastLSN)

	log.Printf("[CDC] mapping_id=%d: Step 2 - Getting CDC changes since LSN %s...", mapping.MappingID, lastLSN)
	changes, err := getCDCChanges(mapping, sourceDB, lastLSN)
	if err != nil {
		log.Printf("[CDC] mapping_id=%d: Step 2 FAILED - Failed to get CDC changes: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to get CDC changes: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}

	if len(changes) == 0 {
		log.Printf("[CDC] mapping_id=%d: Step 2 SUCCESS - No changes found, CDC sync completed", mapping.MappingID)
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - CDC sync completed, no changes found",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
			"SUCCESS", nil, nil)
		return
	}
	log.Printf("[CDC] mapping_id=%d: Step 2 SUCCESS - Found %d CDC changes", mapping.MappingID, len(changes))

	// Step 3: Extract primary key values and find max LSN
	log.Printf("[CDC] mapping_id=%d: Step 3 - Extracting primary key values from CDC changes...", mapping.MappingID)
	pkValuesMap := make(map[interface{}]bool) // Use map to avoid duplicates
	pkValues := make([]interface{}, 0)
	var maxLSN string
	for _, change := range changes {
		// Track max LSN
		if lsnVal, ok := change["__$start_lsn"]; ok {
			var lsnStr string
			if lsnBytes, ok := lsnVal.([]byte); ok {
				lsnStr = hex.EncodeToString(lsnBytes)
			} else if lsnStrVal, ok := lsnVal.(string); ok {
				lsnStr = lsnStrVal
			} else {
				lsnStr = fmt.Sprintf("%v", lsnVal)
			}
			if lsnStr > maxLSN {
				maxLSN = lsnStr
			}
		}

		// Extract PK value (avoid duplicates)
		if pkVal, ok := change[mapping.PrimaryKeyColumn]; ok {
			// Use string representation for map key to handle different types
			pkKey := fmt.Sprintf("%v", pkVal)
			if !pkValuesMap[pkKey] {
				pkValuesMap[pkKey] = true
				pkValues = append(pkValues, pkVal)
			}
		}
	}

	if len(pkValues) == 0 {
		log.Printf("[CDC] mapping_id=%d: Step 3 WARNING - No primary key values found in CDC changes", mapping.MappingID)
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - CDC sync completed, no primary key values found",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
			"SUCCESS", nil, nil)
		return
	}
	log.Printf("[CDC] mapping_id=%d: Step 3 SUCCESS - Extracted %d primary key values, max LSN: %s", mapping.MappingID, len(pkValues), maxLSN)

	// Step 4: Get destination columns first (to match column order for BCP native mode)
	log.Printf("[CDC] mapping_id=%d: Step 4 - Connecting to destination and getting table columns for correct order...", mapping.MappingID)
	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		log.Printf("[CDC] mapping_id=%d: Step 4 FAILED - Failed to connect to destination: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to connect to destination: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}
	defer destDB.Close()

	_, err = destDB.Exec(fmt.Sprintf("USE [%s]", mapping.DestDatabase))
	if err != nil {
		log.Printf("[CDC] mapping_id=%d: Step 4 FAILED - Failed to switch to destination database: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to switch to destination database: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}

	// Get destination columns first (for BCP native mode, column order must match destination)
	destCols, err := getTableColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	if err != nil {
		log.Printf("[CDC] mapping_id=%d: Step 4 FAILED - Failed to get destination columns: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to get destination columns: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}

	// Get source columns
	sourceCols, err := getTableColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	if err != nil {
		log.Printf("[CDC] mapping_id=%d: Step 4 FAILED - Failed to get source columns: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to get source columns: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}

	// Build a map of source columns for quick lookup
	sourceColMap := make(map[string]string)
	for _, col := range sourceCols {
		sourceColMap[strings.ToLower(col)] = col
	}

	// Build column list in DESTINATION order (required for BCP native mode)
	// BCP import expects columns in destination table order
	// IMPORTANT: Include ALL columns (timestamp, identity, etc.) - no exclusions for BCP
	columns := make([]string, 0)
	pkFound := false

	// First ensure PrimaryKeyColumn is first
	if sourceCol, exists := sourceColMap[strings.ToLower(mapping.PrimaryKeyColumn)]; exists {
		columns = append(columns, sourceCol)
		pkFound = true
	}
	if !pkFound {
		log.Printf("[CDC] mapping_id=%d: Step 4 FAILED - Primary key column %s not found in source table", mapping.MappingID, mapping.PrimaryKeyColumn)
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Primary key column %s not found in source table",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, mapping.PrimaryKeyColumn),
			"ERROR", nil, nil)
		return
	}

	// Add ALL other columns in DESTINATION order (no exclusions - include timestamp, identity, everything)
	for _, destCol := range destCols {
		if strings.EqualFold(destCol, mapping.PrimaryKeyColumn) {
			continue // Already added
		}
		// Find matching source column
		if sourceCol, exists := sourceColMap[strings.ToLower(destCol)]; exists {
			columns = append(columns, sourceCol)
		}
	}

	log.Printf("[CDC] mapping_id=%d: Step 4 SUCCESS - Found %d columns to sync (all columns included, no exclusions)",
		mapping.MappingID, len(columns))

	// Step 5: Process PKs in batches to avoid "argument list too long" error
	// Split PKs into batches of 1000 to avoid command line length limits
	batchSizePK := 1000
	totalBatches := (len(pkValues) + batchSizePK - 1) / batchSizePK
	log.Printf("[CDC] mapping_id=%d: Step 5 - Processing %d primary keys in %d batches (batch size: %d)...",
		mapping.MappingID, len(pkValues), totalBatches, batchSizePK)

	configMu.RLock()
	batchSizeVal := batchSize
	configMu.RUnlock()

	totalExportRows := int64(0)
	totalDeleteRows := int64(0)
	totalImportRows := int64(0)

	// Process each batch - export, delete, import separately (like full sync)
	for batchNum := 0; batchNum < totalBatches; batchNum++ {
		startIdx := batchNum * batchSizePK
		endIdx := startIdx + batchSizePK
		if endIdx > len(pkValues) {
			endIdx = len(pkValues)
		}
		batchPKs := pkValues[startIdx:endIdx]

		log.Printf("[CDC] mapping_id=%d: Step 8 - Processing batch %d/%d (%d PKs)...",
			mapping.MappingID, batchNum+1, totalBatches, len(batchPKs))

		// Step 8a: Build PK IN clause for this batch
		pkInClause := buildPKInClause(batchPKs, mapping.PrimaryKeyColumn)

		// Step 8b: Delete records from destination for this batch first
		log.Printf("[CDC] mapping_id=%d: Step 8b - Deleting %d records from destination (batch %d/%d)...",
			mapping.MappingID, len(batchPKs), batchNum+1, totalBatches)
		deleteQuery := fmt.Sprintf(`
			DELETE FROM [%s].[%s].[%s]
			WHERE %s IN (%s)
		`, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable,
			mapping.PrimaryKeyColumn, pkInClause)

		log.Printf("[CDC] mapping_id=%d: Step 8b - DELETE query (batch %d/%d): %s",
			mapping.MappingID, batchNum+1, totalBatches, deleteQuery)

		result, err := destDB.Exec(deleteQuery)
		if err != nil {
			log.Printf("[CDC] mapping_id=%d: Step 8b FAILED - Failed to delete from destination (batch %d): %v",
				mapping.MappingID, batchNum+1, err)
			writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to delete from destination (batch %d): %v",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, batchNum+1, err),
				"ERROR", nil, nil)
			return
		}
		rowsAffected, _ := result.RowsAffected()
		totalDeleteRows += rowsAffected
		if rowsAffected == 0 {
			log.Printf("[CDC] mapping_id=%d: Step 8b WARNING - No records deleted (PKs may not exist in destination): %s",
				mapping.MappingID, pkInClause)
		}
		log.Printf("[CDC] mapping_id=%d: Step 8b SUCCESS - Deleted %d records from destination (batch %d/%d)",
			mapping.MappingID, rowsAffected, batchNum+1, totalBatches)

		// Step 8c: BCP export from source for this batch (with WHERE PK IN clause)
		batchTmpFile := filepath.Join(os.TempDir(), fmt.Sprintf("cdc_sync_%d_%d_batch_%d.dat", mapping.FlowID, mapping.MappingID, batchNum))
		defer os.Remove(batchTmpFile)

		selectQuery := fmt.Sprintf(`
			SELECT %s 
			FROM [%s].[%s].[%s]
			WHERE %s IN (%s)
			ORDER BY %s
		`, strings.Join(columns, ", "),
			mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
			mapping.PrimaryKeyColumn, pkInClause, mapping.PrimaryKeyColumn)

		log.Printf("[CDC] mapping_id=%d: Step 8c - BCP export query (batch %d/%d): %s",
			mapping.MappingID, batchNum+1, totalBatches, selectQuery)

		bcpOutCmd := fmt.Sprintf(`bcp "%s" queryout "%s" -n -S "%s,%d" -U "%s" -P "%s" -d "%s" -u`,
			strings.ReplaceAll(selectQuery, "\n", " "),
			batchTmpFile, flow.SourceServer, flow.SourcePort, flow.SourceUser, flow.SourcePass, mapping.SourceDatabase)

		ctxOut, cancelOut := context.WithTimeout(context.Background(), 2*time.Hour)
		cmdOut := exec.CommandContext(ctxOut, "sh", "-c", bcpOutCmd)

		outputOut, err := cmdOut.CombinedOutput()
		cancelOut()
		outputOutStr := string(outputOut)
		if err != nil {
			if ctxOut.Err() == context.DeadlineExceeded {
				errorMsg := fmt.Sprintf("BCP export timeout for batch %d", batchNum+1)
				log.Printf("[CDC] mapping_id=%d: Step 8c FAILED - %s", mapping.MappingID, errorMsg)
				writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
					fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: %s",
						flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
						mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, errorMsg),
					"ERROR", nil, nil)
				return
			}
			errorMsg := fmt.Sprintf("BCP export failed for batch %d: %v", batchNum+1, err)
			log.Printf("[CDC] mapping_id=%d: Step 8c FAILED - BCP export failed for batch %d: %v, output: %s",
				mapping.MappingID, batchNum+1, err, outputOutStr)
			writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: %s",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, errorMsg),
				"ERROR", nil, nil)
			return
		}
		batchExportRows := parseBCPRowCount(outputOutStr)
		totalExportRows += batchExportRows
		log.Printf("[CDC] mapping_id=%d: Step 8c SUCCESS - Exported %d rows from source (batch %d/%d, file size: %d bytes)",
			mapping.MappingID, batchExportRows, batchNum+1, totalBatches, getFileSize(batchTmpFile))

		// Step 8d: BCP import to destination for this batch (like full sync)
		log.Printf("[CDC] mapping_id=%d: Step 8d - Starting BCP import to destination (batch %d/%d, file: %s, size: %d bytes)...",
			mapping.MappingID, batchNum+1, totalBatches, batchTmpFile, getFileSize(batchTmpFile))

		bcpInCmd := fmt.Sprintf(`bcp "[%s].[%s]" in "%s" -n -E -S "%s,%d" -U "%s" -P "%s" -d "%s" -b %d -u`,
			mapping.DestSchema, mapping.DestTable,
			batchTmpFile, flow.DestServer, flow.DestPort, flow.DestUser, flow.DestPass, mapping.DestDatabase, batchSizeVal)

		bcpInLog := fmt.Sprintf(`bcp "[%s].[%s]" in "%s" -n -E -S "%s,%d" -U "%s" -P "****" -d "%s" -b %d -u`,
			mapping.DestSchema, mapping.DestTable, batchTmpFile, flow.DestServer, flow.DestPort, flow.DestUser, mapping.DestDatabase, batchSizeVal)
		log.Printf("[CDC] mapping_id=%d: Step 8d - BCP import command (batch %d/%d): %s",
			mapping.MappingID, batchNum+1, totalBatches, bcpInLog)

		ctxIn, cancelIn := context.WithTimeout(context.Background(), 2*time.Hour)
		cmdIn := exec.CommandContext(ctxIn, "sh", "-c", bcpInCmd)

		outputIn, err := cmdIn.CombinedOutput()
		cancelIn()
		outputInStr := string(outputIn)

		// Always log full BCP import output for debugging (only to file, not stdout)
		if logFile != nil {
			fmt.Fprintf(logFile, "[CDC] mapping_id=%d: Step 8d - BCP import output (batch %d/%d):\n%s\n",
				mapping.MappingID, batchNum+1, totalBatches, outputInStr)
		}

		if err != nil {
			if ctxIn.Err() == context.DeadlineExceeded {
				errorMsg := fmt.Sprintf("BCP import timeout for batch %d", batchNum+1)
				log.Printf("[CDC] mapping_id=%d: Step 8d FAILED - %s", mapping.MappingID, errorMsg)
				writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
					fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: %s",
						flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
						mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, errorMsg),
					"ERROR", nil, nil)
				return
			}
			errorMsg := fmt.Sprintf("BCP import failed for batch %d: %v", batchNum+1, err)
			log.Printf("[CDC] mapping_id=%d: Step 8d FAILED - BCP import failed for batch %d: %v, output: %s",
				mapping.MappingID, batchNum+1, err, outputInStr)
			writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: %s",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, errorMsg),
				"ERROR", nil, nil)
			return
		}
		batchImportRows := parseBCPRowCount(outputInStr)
		totalImportRows += batchImportRows
		if batchImportRows == 0 && batchExportRows > 0 {
			// This is a critical error - we exported rows but imported 0
			errorMsg := fmt.Sprintf("BCP import returned 0 rows but export had %d rows (batch %d/%d). This indicates a column order/type mismatch",
				batchExportRows, batchNum+1, totalBatches)
			log.Printf("[CDC] mapping_id=%d: Step 8d ERROR - %s", mapping.MappingID, errorMsg)
			writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: %s",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, errorMsg),
				"ERROR", nil, nil)
			return
		}
		if batchImportRows == 0 {
			log.Printf("[CDC] mapping_id=%d: Step 8d WARNING - BCP import returned 0 rows (batch %d/%d), output: %s",
				mapping.MappingID, batchNum+1, totalBatches, outputInStr)
		}
		log.Printf("[CDC] mapping_id=%d: Step 8d SUCCESS - Imported %d rows to destination (batch %d/%d)",
			mapping.MappingID, batchImportRows, batchNum+1, totalBatches)
	}

	log.Printf("[CDC] mapping_id=%d: Step 6 SUCCESS - Processed all batches: %d exported, %d deleted, %d imported",
		mapping.MappingID, totalExportRows, totalDeleteRows, totalImportRows)

	// Step 7: Update last LSN and sync timestamp
	if maxLSN != "" {
		log.Printf("[CDC] mapping_id=%d: Step 7 - Updating last LSN to: %s", mapping.MappingID, maxLSN)
		updateLastLSN(mapping.MappingID, maxLSN)
		log.Printf("[CDC] mapping_id=%d: Step 7 SUCCESS - Updated last LSN", mapping.MappingID)
	}

	recordsProcessed := totalImportRows

	log.Printf("[CDC] mapping_id=%d: COMPLETED - CDC sync finished: exported=%d, deleted=%d, imported=%d rows",
		mapping.MappingID, totalExportRows, totalDeleteRows, recordsProcessed)
	
	if recordsProcessed > 0 {
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - CDC sync completed, %d records transferred",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, recordsProcessed),
			"SUCCESS", &recordsProcessed, nil)
	} else {
		writeLog(&mapping.FlowID, &mapping.MappingID, "CDC",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - CDC sync completed, no changes found",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable),
			"SUCCESS", nil, nil)
	}
}

func validateSchema(mapping TableMapping, sourceDB *sql.DB) bool {
	checkTableQuery := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_CATALOG = '%s' 
		AND TABLE_SCHEMA = '%s' 
		AND TABLE_NAME = '%s'
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)

	var count int
	if err := sourceDB.QueryRow(checkTableQuery).Scan(&count); err != nil || count == 0 {
		return false
	}

	checkPKQuery := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_CATALOG = '%s' 
		AND TABLE_SCHEMA = '%s' 
		AND TABLE_NAME = '%s'
		AND COLUMN_NAME = '%s'
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable, mapping.PrimaryKeyColumn)

	if err := sourceDB.QueryRow(checkPKQuery).Scan(&count); err != nil || count == 0 {
		return false
	}

	sourceCols, err := getTableColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	if err != nil {
		return false
	}

	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		return false
	}
	defer destDB.Close()

	_, err = destDB.Exec(fmt.Sprintf("USE [%s]", mapping.DestDatabase))
	if err != nil {
		return false
	}

	destCols, err := getTableColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	if err != nil {
		return true
	}

	// Get timestamp and identity columns from both source and dest to exclude from validation
	timestampColsSource, _ := getTimestampColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	timestampColsDest, _ := getTimestampColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	identityColsSource, _ := getIdentityColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	identityColsDest, _ := getIdentityColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)

	excludeMap := make(map[string]bool)
	for _, col := range timestampColsSource {
		excludeMap[strings.ToLower(col)] = true
	}
	for _, col := range timestampColsDest {
		excludeMap[strings.ToLower(col)] = true
	}
	for _, col := range identityColsSource {
		excludeMap[strings.ToLower(col)] = true
	}
	for _, col := range identityColsDest {
		excludeMap[strings.ToLower(col)] = true
	}

	sourceColMap := make(map[string]bool)
	for _, col := range sourceCols {
		if !excludeMap[strings.ToLower(col)] {
			sourceColMap[strings.ToLower(col)] = true
		}
	}

	for _, col := range destCols {
		colLower := strings.ToLower(col)
		if excludeMap[colLower] {
			continue // Skip timestamp and identity columns in validation
		}
		if !sourceColMap[colLower] {
			return false
		}
	}

	return true
}

func getTimestampColumns(db *sql.DB, database, schema, table string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_CATALOG = '%s'
		AND TABLE_SCHEMA = '%s'
		AND TABLE_NAME = '%s'
		AND DATA_TYPE IN ('timestamp', 'rowversion')
	`, database, schema, table)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, nil
}

func getIdentityColumns(db *sql.DB, database, schema, table string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS c
		INNER JOIN sys.tables t ON t.name = c.TABLE_NAME
		INNER JOIN sys.schemas s ON s.schema_id = t.schema_id AND s.name = c.TABLE_SCHEMA
		INNER JOIN sys.columns col ON col.object_id = t.object_id AND col.name = c.COLUMN_NAME
		WHERE c.TABLE_CATALOG = '%s'
		AND c.TABLE_SCHEMA = '%s'
		AND c.TABLE_NAME = '%s'
		AND COLUMNPROPERTY(col.object_id, col.name, 'IsIdentity') = 1
	`, database, schema, table)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, nil
}

func getTableColumns(db *sql.DB, database, schema, table string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_CATALOG = '%s' 
		AND TABLE_SCHEMA = '%s' 
		AND TABLE_NAME = '%s'
		ORDER BY ORDINAL_POSITION
	`, database, schema, table)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	return columns, nil
}

func executeBatchInsert(db *sql.DB, connStr, database, schema, table string, columns []string, batch [][]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	tmpFile := filepath.Join(os.TempDir(), fmt.Sprintf("bcp_bulk_%d.dat", time.Now().UnixNano()))
	defer os.Remove(tmpFile)

	file, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	for _, row := range batch {
		values := make([]string, len(row))
		for i, val := range row {
			if val == nil {
				values[i] = ""
			} else {
				switch v := val.(type) {
				case []byte:
					values[i] = hex.EncodeToString(v)
				case time.Time:
					values[i] = v.Format("2006-01-02 15:04:05.000")
				case bool:
					if v {
						values[i] = "1"
					} else {
						values[i] = "0"
					}
				case string:
					escaped := strings.ReplaceAll(v, "\t", " ")
					escaped = strings.ReplaceAll(escaped, "\n", " ")
					escaped = strings.ReplaceAll(escaped, "\r", " ")
					values[i] = escaped
				default:
					values[i] = fmt.Sprintf("%v", v)
				}
			}
		}
		if _, err := file.WriteString(strings.Join(values, "\t") + "\n"); err != nil {
			file.Close()
			return fmt.Errorf("failed to write to temp file: %w", err)
		}
	}
	file.Close()

	server := extractServer(connStr)
	user := extractUser(connStr)
	password := extractPassword(connStr)

	bcpCmd := fmt.Sprintf(`bcp "[%s].[%s]" in "%s" -c -t"\t" -S "%s" -U "%s" -P "%s" -d "%s" -b 1000 -u`,
		schema, table, tmpFile, server, user, password, database)

	log.Printf("Executing BCP: bcp \"[%s].[%s]\" in ... -S \"%s\" -U \"%s\" -d \"%s\"", schema, table, server, user, database)

	cmd := exec.Command("sh", "-c", bcpCmd)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("BCP failed: %v, output: %s", err, string(output))
	}

	return nil
}

func extractServer(connStr string) string {
	parts := strings.Split(connStr, ";")
	for _, part := range parts {
		if strings.HasPrefix(part, "server=") {
			return strings.TrimPrefix(part, "server=")
		}
	}
	return "localhost"
}

func extractUser(connStr string) string {
	parts := strings.Split(connStr, ";")
	for _, part := range parts {
		if strings.HasPrefix(part, "user id=") {
			return strings.TrimPrefix(part, "user id=")
		}
	}
	return "sa"
}

func extractPassword(connStr string) string {
	parts := strings.Split(connStr, ";")
	for _, part := range parts {
		if strings.HasPrefix(part, "password=") {
			return strings.TrimPrefix(part, "password=")
		}
	}
	return ""
}

func applyInsert(db *sql.DB, mapping TableMapping, change map[string]interface{}) error {
	columns := make([]string, 0)
	values := make([]interface{}, 0)
	placeholders := make([]string, 0)

	for key, val := range change {
		if strings.HasPrefix(key, "__$") {
			continue
		}
		columns = append(columns, key)
		values = append(values, val)
		placeholders = append(placeholders, fmt.Sprintf("@p%d", len(placeholders)+1))
	}

	query := fmt.Sprintf(`
		INSERT INTO [%s].[%s].[%s] (%s)
		VALUES (%s)
	`, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	args := make([]interface{}, len(values))
	for i, v := range values {
		args[i] = v
	}

	_, err := db.Exec(query, args...)
	return err
}

func applyUpdate(db *sql.DB, mapping TableMapping, change map[string]interface{}) error {
	setParts := make([]string, 0)
	values := make([]interface{}, 0)
	paramIndex := 1

	for key, val := range change {
		if strings.HasPrefix(key, "__$") {
			continue
		}
		setParts = append(setParts, fmt.Sprintf("%s = @p%d", key, paramIndex))
		values = append(values, val)
		paramIndex++
	}

	pkValue, ok := change[mapping.PrimaryKeyColumn]
	if !ok {
		return fmt.Errorf("primary key column %s not found", mapping.PrimaryKeyColumn)
	}

	query := fmt.Sprintf(`
		UPDATE [%s].[%s].[%s]
		SET %s
		WHERE %s = @p%d
	`, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable,
		strings.Join(setParts, ", "),
		mapping.PrimaryKeyColumn, paramIndex)

	values = append(values, pkValue)

	args := make([]interface{}, len(values))
	for i, v := range values {
		args[i] = v
	}

	_, err := db.Exec(query, args...)
	return err
}

func applyDelete(db *sql.DB, mapping TableMapping, change map[string]interface{}) error {
	pkValue, ok := change[mapping.PrimaryKeyColumn]
	if !ok {
		return fmt.Errorf("primary key column %s not found", mapping.PrimaryKeyColumn)
	}

	query := fmt.Sprintf(`
		DELETE FROM [%s].[%s].[%s]
		WHERE %s = @p1
	`, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable,
		mapping.PrimaryKeyColumn)

	_, err := db.Exec(query, pkValue)
	return err
}

func isCDCEnabled(mapping TableMapping, db *sql.DB) bool {
	var isCDCEnabled bool
	err := db.QueryRow(fmt.Sprintf("SELECT is_cdc_enabled FROM sys.databases WHERE name = '%s'", mapping.SourceDatabase)).Scan(&isCDCEnabled)
	if err != nil {
		log.Printf("Error checking CDC on database: %v", err)
		return false
	}
	if !isCDCEnabled {
		log.Printf("CDC not enabled on database: %s", mapping.SourceDatabase)
		return false
	}

	query := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM cdc.change_tables 
		WHERE source_object_id = OBJECT_ID('[%s].[%s].[%s]')
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)

	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		log.Printf("Error checking CDC on table %s.%s.%s: %v", mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable, err)
		return false
	}
	if count == 0 {
		log.Printf("CDC not enabled on table: %s.%s.%s", mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
		return false
	}
	return true
}

func getMinLSN(mapping TableMapping, db *sql.DB) string {
	// Get CDC table name from cdc.change_tables by joining with sys.tables
	query := fmt.Sprintf(`
		SELECT t.name AS cdc_table_name
		FROM cdc.change_tables ct
		INNER JOIN sys.tables t ON t.object_id = ct.object_id
		WHERE ct.source_object_id = OBJECT_ID('[%s].[%s].[%s]')
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)

	var cdcTableName sql.NullString
	err := db.QueryRow(query).Scan(&cdcTableName)
	if err != nil {
		log.Printf("[getMinLSN] ERROR: Failed to get CDC table name for mapping_id=%d (table: %s.%s.%s): %v",
			mapping.MappingID, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable, err)
		return ""
	}
	if !cdcTableName.Valid || cdcTableName.String == "" {
		log.Printf("[getMinLSN] ERROR: CDC table name is NULL for mapping_id=%d (table: %s.%s.%s)",
			mapping.MappingID, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
		return ""
	}

	log.Printf("[getMinLSN] Found CDC table name: cdc.%s for mapping_id=%d", cdcTableName.String, mapping.MappingID)
	lsnQuery := fmt.Sprintf(`SELECT MIN(__$start_lsn) FROM cdc.[%s]`, cdcTableName.String)

	var lsn []byte
	err = db.QueryRow(lsnQuery).Scan(&lsn)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("[getMinLSN] WARNING: No LSN found in CDC table [cdc.%s] for mapping_id=%d (table may be empty)",
				cdcTableName.String, mapping.MappingID)
		} else {
			log.Printf("[getMinLSN] ERROR: Failed to get MIN LSN from CDC table [cdc.%s] for mapping_id=%d: %v",
				cdcTableName.String, mapping.MappingID, err)
		}
		return ""
	}

	return hex.EncodeToString(lsn)
}

func getMaxLSN(mapping TableMapping, db *sql.DB) string {
	// Get CDC table name from cdc.change_tables by joining with sys.tables
	query := fmt.Sprintf(`
		SELECT t.name AS cdc_table_name
		FROM cdc.change_tables ct
		INNER JOIN sys.tables t ON t.object_id = ct.object_id
		WHERE ct.source_object_id = OBJECT_ID('[%s].[%s].[%s]')
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)

	var cdcTableName sql.NullString
	err := db.QueryRow(query).Scan(&cdcTableName)
	if err != nil {
		log.Printf("[getMaxLSN] ERROR: Failed to get CDC table name for mapping_id=%d (table: %s.%s.%s): %v",
			mapping.MappingID, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable, err)
		return ""
	}
	if !cdcTableName.Valid || cdcTableName.String == "" {
		log.Printf("[getMaxLSN] ERROR: CDC table name is NULL for mapping_id=%d (table: %s.%s.%s)",
			mapping.MappingID, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
		return ""
	}

	log.Printf("[getMaxLSN] Found CDC table name: cdc.%s for mapping_id=%d", cdcTableName.String, mapping.MappingID)
	lsnQuery := fmt.Sprintf(`SELECT MAX(__$start_lsn) FROM cdc.[%s]`, cdcTableName.String)

	var lsn []byte
	err = db.QueryRow(lsnQuery).Scan(&lsn)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("[getMaxLSN] WARNING: No LSN found in CDC table [cdc.%s] for mapping_id=%d (table may be empty)",
				cdcTableName.String, mapping.MappingID)
		} else {
			log.Printf("[getMaxLSN] ERROR: Failed to get MAX LSN from CDC table [cdc.%s] for mapping_id=%d: %v",
				cdcTableName.String, mapping.MappingID, err)
		}
		return ""
	}

	return hex.EncodeToString(lsn)
}

func getCDCChanges(mapping TableMapping, db *sql.DB, lastLSN string) ([]map[string]interface{}, error) {
	// Get CDC table name from cdc.change_tables by joining with sys.tables
	query := fmt.Sprintf(`
		SELECT t.name AS cdc_table_name
		FROM cdc.change_tables ct
		INNER JOIN sys.tables t ON t.object_id = ct.object_id
		WHERE ct.source_object_id = OBJECT_ID('[%s].[%s].[%s]')
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)

	var cdcTableName sql.NullString
	err := db.QueryRow(query).Scan(&cdcTableName)
	if err != nil {
		return nil, fmt.Errorf("CDC table name not found for table: %v", err)
	}
	if !cdcTableName.Valid || cdcTableName.String == "" {
		return nil, fmt.Errorf("CDC table name is NULL for table")
	}

	lsnBytes, err := hex.DecodeString(lastLSN)
	if err != nil {
		return nil, fmt.Errorf("invalid LSN format: %v", err)
	}

	cdcQuery := fmt.Sprintf(`
		DECLARE @from_lsn BINARY(10) = @p1
		SELECT *
		FROM cdc.[%s]
		WHERE __$start_lsn > @from_lsn
		ORDER BY __$start_lsn, __$seqval
	`, cdcTableName.String)

	rows, err := db.Query(cdcQuery, lsnBytes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	changes := make([]map[string]interface{}, 0)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		change := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				change[col] = hex.EncodeToString(b)
			} else {
				change[col] = val
			}
		}

		changes = append(changes, change)
	}

	return changes, nil
}

func performVerification(mapping TableMapping, sourceDB *sql.DB) {
	// Panic recovery for verification
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[VERIFICATION] PANIC in performVerification (mapping_id=%d) recovered: %v", mapping.MappingID, r)
			flow, _ := getFlowByID(mapping.FlowID)
			writeLog(&mapping.FlowID, &mapping.MappingID, "VERIFICATION",
				fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Panic recovered: %v",
					flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
					mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, r),
				"ERROR", nil, nil)
		}
	}()

	// Get flow details for log messages
	flow, err := getFlowByID(mapping.FlowID)
	if err != nil {
		log.Printf("[VERIFICATION] Failed to get flow details for mapping_id=%d: %v", mapping.MappingID, err)
		return
	}

	log.Printf("[VERIFICATION] mapping_id=%d: Step 1 - Connecting to destination server...", mapping.MappingID)
	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		log.Printf("[VERIFICATION] mapping_id=%d: Step 1 FAILED - Failed to connect to destination: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "VERIFICATION",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to connect to destination: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}
	defer destDB.Close()
	log.Printf("[VERIFICATION] mapping_id=%d: Step 1 SUCCESS - Connected to destination server", mapping.MappingID)

	log.Printf("[VERIFICATION] mapping_id=%d: Step 2 - Switching to destination database [%s]...", mapping.MappingID, mapping.DestDatabase)
	_, err = destDB.Exec(fmt.Sprintf("USE [%s]", mapping.DestDatabase))
	if err != nil {
		log.Printf("[VERIFICATION] mapping_id=%d: Step 2 FAILED - Failed to switch to destination database: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "VERIFICATION",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to switch to destination database: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}
	log.Printf("[VERIFICATION] mapping_id=%d: Step 2 SUCCESS - Switched to destination database", mapping.MappingID)

	// Get timestamp and identity columns to exclude from verification
	timestampColsDest, _ := getTimestampColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	timestampColsSource, _ := getTimestampColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	identityColsDest, _ := getIdentityColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	identityColsSource, _ := getIdentityColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)

	excludeMap := make(map[string]bool)
	for _, col := range timestampColsDest {
		excludeMap[strings.ToLower(col)] = true
	}
	for _, col := range timestampColsSource {
		excludeMap[strings.ToLower(col)] = true
	}
	for _, col := range identityColsDest {
		excludeMap[strings.ToLower(col)] = true
	}
	for _, col := range identityColsSource {
		excludeMap[strings.ToLower(col)] = true
	}

	// Get all columns first
	destCols, err := getTableColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	if err != nil {
		return
	}

	// Build column list excluding timestamp and identity
	verifyCols := make([]string, 0)
	for _, col := range destCols {
		if !excludeMap[strings.ToLower(col)] {
			verifyCols = append(verifyCols, col)
		}
	}

	if len(verifyCols) == 0 {
		log.Printf("[VERIFICATION] mapping_id=%d: No columns to verify (all are timestamp/identity), skipping", mapping.MappingID)
		return
	}

	// Query last 10 rows from destination (excluding timestamp/identity columns)
	// Ensure primary key column is included for comparison
	verifyColsWithPK := make([]string, 0)
	pkIncluded := false
	for _, col := range verifyCols {
		if strings.EqualFold(col, mapping.PrimaryKeyColumn) {
			pkIncluded = true
		}
		verifyColsWithPK = append(verifyColsWithPK, col)
	}
	if !pkIncluded {
		verifyColsWithPK = append([]string{mapping.PrimaryKeyColumn}, verifyColsWithPK...)
	}

	configMu.RLock()
	rowCount := verificationRowCount
	configMu.RUnlock()

	log.Printf("[VERIFICATION] mapping_id=%d: Step 3 - Querying last %d rows from destination...", mapping.MappingID, rowCount)
	destQuery := fmt.Sprintf(`
		SELECT TOP %d %s
		FROM [%s].[%s].[%s]
		ORDER BY %s DESC
	`, rowCount, strings.Join(verifyColsWithPK, ", "),
		mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, mapping.PrimaryKeyColumn)

	destRows, err := destDB.Query(destQuery)
	if err != nil {
		log.Printf("[VERIFICATION] mapping_id=%d: Step 3 FAILED - Failed to query destination: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "VERIFICATION",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to query destination: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}
	defer destRows.Close()
	log.Printf("[VERIFICATION] mapping_id=%d: Step 3 SUCCESS - Queried destination table", mapping.MappingID)

	destColumns, _ := destRows.Columns()
	destData := make([]map[string]interface{}, 0)

	for destRows.Next() {
		values := make([]interface{}, len(destColumns))
		valuePtrs := make([]interface{}, len(destColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := destRows.Scan(valuePtrs...); err != nil {
			log.Printf("[VERIFICATION] mapping_id=%d: WARNING - Failed to scan dest row: %v", mapping.MappingID, err)
			continue
		}

		row := make(map[string]interface{})
		for i, col := range destColumns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = hex.EncodeToString(b)
			} else {
				row[col] = val
			}
		}
		destData = append(destData, row)
	}

	if len(destData) == 0 {
		log.Printf("[VERIFICATION] mapping_id=%d: No data found in destination table, skipping verification", mapping.MappingID)
		return
	}

	log.Printf("[VERIFICATION] mapping_id=%d: Step 4 - Found %d rows in destination, extracting primary key values...", mapping.MappingID, len(destData))
	pkValues := make([]interface{}, 0)
	for _, row := range destData {
		if pkVal, ok := row[mapping.PrimaryKeyColumn]; ok {
			pkValues = append(pkValues, pkVal)
		}
	}

	if len(pkValues) == 0 {
		log.Printf("[VERIFICATION] mapping_id=%d: No primary key values found, skipping verification", mapping.MappingID)
		return
	}

	log.Printf("[VERIFICATION] mapping_id=%d: Step 4 SUCCESS - Extracted %d primary key values", mapping.MappingID, len(pkValues))

	// Build IN clause values for the PKs we found
	// Go-MSSQL driver doesn't handle IN clause parameters well, so we use direct values
	// PK values are safe (integer or string from our own database)
	inValues := make([]string, 0)
	for _, pkVal := range pkValues {
		// Format value based on type
		switch v := pkVal.(type) {
		case string:
			// Escape single quotes for SQL
			escaped := strings.ReplaceAll(v, "'", "''")
			inValues = append(inValues, fmt.Sprintf("'%s'", escaped))
		case int, int32, int64:
			inValues = append(inValues, fmt.Sprintf("%v", v))
		case float32, float64:
			inValues = append(inValues, fmt.Sprintf("%v", v))
		default:
			// Convert to string and escape
			valStr := fmt.Sprintf("%v", v)
			escaped := strings.ReplaceAll(valStr, "'", "''")
			inValues = append(inValues, fmt.Sprintf("'%s'", escaped))
		}
	}

	if len(inValues) == 0 {
		return
	}

	// Query same rows from source using PK IN clause
	log.Printf("[VERIFICATION] mapping_id=%d: Step 5 - Querying matching rows from source using PK IN clause...", mapping.MappingID)
	sourceQuery := fmt.Sprintf(`
		SELECT %s
		FROM [%s].[%s].[%s]
		WHERE %s IN (%s)
		ORDER BY %s DESC
	`, strings.Join(verifyColsWithPK, ", "),
		mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
		mapping.PrimaryKeyColumn, strings.Join(inValues, ", "), mapping.PrimaryKeyColumn)

	sourceRows, err := sourceDB.Query(sourceQuery)
	if err != nil {
		log.Printf("[VERIFICATION] mapping_id=%d: Step 5 FAILED - Failed to query source: %v", mapping.MappingID, err)
		writeLog(&mapping.FlowID, &mapping.MappingID, "VERIFICATION",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Error: Failed to query source: %v",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, err),
			"ERROR", nil, nil)
		return
	}
	defer sourceRows.Close()
	log.Printf("[VERIFICATION] mapping_id=%d: Step 5 SUCCESS - Queried source table", mapping.MappingID)

	sourceColumns, _ := sourceRows.Columns()
	sourceData := make(map[interface{}]map[string]interface{})

	for sourceRows.Next() {
		values := make([]interface{}, len(sourceColumns))
		valuePtrs := make([]interface{}, len(sourceColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := sourceRows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		var pkVal interface{}
		for i, col := range sourceColumns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = hex.EncodeToString(b)
			} else {
				row[col] = val
			}
			if strings.EqualFold(col, mapping.PrimaryKeyColumn) {
				pkVal = row[col]
			}
		}
		if pkVal != nil {
			sourceData[pkVal] = row
		}
	}

	// Calculate combined MD5 for all rows
	log.Printf("[VERIFICATION] mapping_id=%d: Step 7 - Calculating combined MD5 hashes...", mapping.MappingID)
	destCombinedMD5 := calculateRowsMD5(destData, mapping.PrimaryKeyColumn)

	// Convert sourceData map to slice for MD5 calculation (sorted by PK)
	sourceDataSlice := make([]map[string]interface{}, 0, len(sourceData))
	for _, row := range sourceData {
		sourceDataSlice = append(sourceDataSlice, row)
	}
	sourceCombinedMD5 := calculateRowsMD5(sourceDataSlice, mapping.PrimaryKeyColumn)
	log.Printf("[VERIFICATION] mapping_id=%d: Destination Combined MD5: %s", mapping.MappingID, destCombinedMD5)
	log.Printf("[VERIFICATION] mapping_id=%d: Source Combined MD5: %s", mapping.MappingID, sourceCombinedMD5)

	mismatches := int64(0)
	compared := int64(0)

	// Compare individual rows for detailed mismatch count
	for _, destRow := range destData {
		pkVal := destRow[mapping.PrimaryKeyColumn]
		sourceRow, exists := sourceData[pkVal]

		if !exists {
			mismatches++
			continue
		}

		destMD5 := calculateRowMD5(destRow)
		sourceMD5 := calculateRowMD5(sourceRow)

		compared++
		if destMD5 != sourceMD5 {
			mismatches++
		}
	}

	statusMsg := "MD5 hashes match"
	if destCombinedMD5 != sourceCombinedMD5 {
		statusMsg = fmt.Sprintf("MD5 mismatch detected, %d mismatches out of %d compared", mismatches, compared)
		log.Printf("[VERIFICATION] mapping_id=%d: Step 6 FAILED - MD5 mismatch detected!", mapping.MappingID)
		writeLog(&mapping.FlowID, &mapping.MappingID, "VERIFICATION",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Verification failed for last %d records: %s",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, rowCount, statusMsg),
			"FAILED", &compared, nil)
	} else {
		log.Printf("[VERIFICATION] mapping_id=%d: Step 6 SUCCESS - MD5 hashes match.", mapping.MappingID)
		writeLog(&mapping.FlowID, &mapping.MappingID, "VERIFICATION",
			fmt.Sprintf("Flow '%s': Table mapping '%s' (%s.%s.%s -> %s.%s.%s) - Verification completed for last %d records: MD5 hashes match",
				flow.FlowName, mapping.SourceTable, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
				mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, rowCount),
			"SUCCESS", &compared, nil)
	}
}

func calculateRowMD5(row map[string]interface{}) string {
	// Sort keys for consistent hashing
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		val := row[k]
		sb.WriteString(k)
		sb.WriteString("=")

		// Normalize values for consistent hashing
		if val == nil {
			sb.WriteString("NULL")
		} else {
			// Convert to string consistently
			switch v := val.(type) {
			case []byte:
				// Binary data - use hex encoding
				sb.WriteString(hex.EncodeToString(v))
			case string:
				// String - use as is
				sb.WriteString(v)
			case time.Time:
				// Time - use RFC3339 format
				sb.WriteString(v.Format(time.RFC3339Nano))
			default:
				// Other types - convert to string
				sb.WriteString(fmt.Sprintf("%v", v))
			}
		}
		sb.WriteString("|")
	}

	hash := md5.Sum([]byte(sb.String()))
	return hex.EncodeToString(hash[:])
}

func calculateRowsMD5(rows []map[string]interface{}, pkColumn string) string {
	// Calculate MD5 for all rows combined (sorted by primary key for consistency)
	// First, sort rows by primary key
	sortedRows := make([]map[string]interface{}, len(rows))
	copy(sortedRows, rows)

	// Sort by primary key value
	sort.Slice(sortedRows, func(i, j int) bool {
		pkI, okI := sortedRows[i][pkColumn]
		pkJ, okJ := sortedRows[j][pkColumn]
		if !okI && !okJ {
			return false
		}
		if !okI {
			return true
		}
		if !okJ {
			return false
		}
		// Compare as strings for consistency
		return fmt.Sprintf("%v", pkI) < fmt.Sprintf("%v", pkJ)
	})

	var sb strings.Builder
	for _, row := range sortedRows {
		rowMD5 := calculateRowMD5(row)
		sb.WriteString(rowMD5)
		sb.WriteString("|")
	}

	hash := md5.Sum([]byte(sb.String()))
	return hex.EncodeToString(hash[:])
}

func getEnabledMappings() ([]TableMapping, error) {
	query := `SELECT 
		tm.mapping_id, 
		tm.flow_id,
		tm.source_database, 
		tm.source_schema, 
		tm.source_table,
		tm.dest_database, 
		tm.dest_schema, 
		tm.dest_table, 
		tm.primary_key_column,
		tm.is_full_sync,
		tm.is_enabled, 
		tm.cdc_enabled, 
		tm.last_cdc_lsn, 
		tm.last_full_sync_at,
		tm.last_cdc_sync_at,
		CONCAT('server=', f.source_server, ';port=', CAST(f.source_port AS NVARCHAR), ';user id=', f.source_user, ';password=', f.source_password, ';database=', tm.source_database, ';encrypt=disable') AS source_conn_string,
		CONCAT('server=', f.dest_server, ';port=', CAST(f.dest_port AS NVARCHAR), ';user id=', f.dest_user, ';password=', f.dest_password, ';database=', tm.dest_database, ';encrypt=disable') AS dest_conn_string
	FROM table_mappings tm
	INNER JOIN flows f ON tm.flow_id = f.flow_id
	WHERE tm.is_enabled = 1 
	AND f.is_enabled = 1`

	rows, err := configDB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mappings []TableMapping
	for rows.Next() {
		var m TableMapping
		var lastCDCLSNBinary []byte // Read as binary
		if err := rows.Scan(&m.MappingID, &m.FlowID, &m.SourceDatabase, &m.SourceSchema, &m.SourceTable,
			&m.DestDatabase, &m.DestSchema, &m.DestTable, &m.PrimaryKeyColumn,
			&m.IsFullSync, &m.IsEnabled, &m.CDCEnabled, &lastCDCLSNBinary, &m.LastFullSyncAt, &m.LastCDCSyncAt,
			&m.SourceConnString, &m.DestConnString); err != nil {
			log.Printf("[getEnabledMappings] WARNING: Failed to scan row: %v", err)
			continue
		}

		// Convert binary LSN to hex string if not null
		if lastCDCLSNBinary != nil && len(lastCDCLSNBinary) > 0 {
			m.LastCDCLSN = sql.NullString{String: hex.EncodeToString(lastCDCLSNBinary), Valid: true}
		} else {
			m.LastCDCLSN = sql.NullString{Valid: false}
		}

		mappings = append(mappings, m)
	}

	return mappings, nil
}

func connectToServer(connString string) (*sql.DB, error) {
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func updateLastConnected(flowID int) {
	configDB.Exec("UPDATE flows SET last_connected_at = GETUTCDATE() WHERE flow_id = @p1", flowID)
}

// writeLog writes a unified log entry to the logs table
func writeLog(flowID *int, mappingID *int, logType, logMessage, status string, recordsCount *int64, durationMs *int64) {
	var flowIDVal interface{}
	var mappingIDVal interface{}
	var recordsCountVal interface{}
	var durationMsVal interface{}

	if flowID != nil {
		flowIDVal = *flowID
	}
	if mappingID != nil {
		mappingIDVal = *mappingID
	}
	if recordsCount != nil {
		recordsCountVal = *recordsCount
	}
	if durationMs != nil {
		durationMsVal = *durationMs
	}

	configDB.Exec(`
		INSERT INTO logs (flow_id, mapping_id, log_type, log_message, status, records_count, duration_ms)
		VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)
	`, flowIDVal, mappingIDVal, logType, logMessage, status, recordsCountVal, durationMsVal)
}

func getFlowByID(flowID int) (Flow, error) {
	var f Flow
	err := configDB.QueryRow(`
		SELECT flow_id, flow_name, source_server, source_port, source_user, source_password,
		       dest_server, dest_port, dest_user, dest_password
		FROM flows WHERE flow_id = @p1`, flowID).Scan(
		&f.FlowID, &f.FlowName, &f.SourceServer, &f.SourcePort, &f.SourceUser, &f.SourcePass,
		&f.DestServer, &f.DestPort, &f.DestUser, &f.DestPass)
	return f, err
}


func updateLastLSN(mappingID int, lsn string) {
	// Convert hex string to binary
	lsnBytes, err := hex.DecodeString(lsn)
	if err != nil {
		log.Printf("[updateLastLSN] ERROR: Failed to decode LSN hex string '%s' for mapping_id=%d: %v", lsn, mappingID, err)
		return
	}

	// Update with binary LSN and sync timestamp
	_, err = configDB.Exec("UPDATE table_mappings SET last_cdc_lsn = @p1, last_cdc_sync_at = GETUTCDATE() WHERE mapping_id = @p2", lsnBytes, mappingID)
	if err != nil {
		log.Printf("[updateLastLSN] ERROR: Failed to update last_cdc_lsn for mapping_id=%d: %v", mappingID, err)
	} else {
		log.Printf("[updateLastLSN] SUCCESS: Updated last_cdc_lsn and last_cdc_sync_at for mapping_id=%d (LSN: %s)", mappingID, lsn)
	}
}

func createBCPFormatFile(flow Flow, mapping TableMapping, formatFile string, sourceColumns []string, timestampColumns []string) error {
	// Step 1: Generate base format file using BCP
	baseFormatFile := formatFile + ".base"
	bcpFormatCmd := fmt.Sprintf(`bcp "[%s].[%s]" format nul -n -f "%s" -S "%s,%d" -U "%s" -P "%s" -d "%s" -u`,
		mapping.DestSchema, mapping.DestTable,
		baseFormatFile, flow.DestServer, flow.DestPort, flow.DestUser, flow.DestPass, mapping.DestDatabase)

	cmd := exec.Command("sh", "-c", bcpFormatCmd)
	if output, err := cmd.CombinedOutput(); err != nil {
		os.Remove(baseFormatFile)
		return fmt.Errorf("failed to generate base format file: %v, output: %s", err, string(output))
	}
	defer os.Remove(baseFormatFile)

	// Step 2: Read base format file
	content, err := os.ReadFile(baseFormatFile)
	if err != nil {
		return fmt.Errorf("failed to read base format file: %w", err)
	}

	// Normalize line endings and remove trailing newlines
	contentStr := strings.ReplaceAll(string(content), "\r\n", "\n")
	contentStr = strings.TrimRight(contentStr, "\n\r")
	lines := strings.Split(contentStr, "\n")
	if len(lines) < 3 {
		return fmt.Errorf("invalid format file structure: expected at least 3 lines, got %d", len(lines))
	}

	// Step 3: Get destination table columns in order
	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		return fmt.Errorf("failed to connect to destination: %w", err)
	}
	defer destDB.Close()

	// Create map of timestamp columns
	timestampMap := make(map[string]bool)
	for _, col := range timestampColumns {
		timestampMap[strings.ToLower(col)] = true
	}

	// Create map of source columns (for matching)
	sourceColMap := make(map[string]int)
	for i, col := range sourceColumns {
		sourceColMap[strings.ToLower(col)] = i + 1
	}

	// Step 4: Modify format file - set Server Column Order to 0 for timestamp columns
	// Format file structure: version, field count, then one line per field
	// Field line format: Field# SQLType PrefixLength Length Terminator ServerColumnOrder ColumnName Collation
	modifiedLines := make([]string, 0, len(lines))
	modifiedLines = append(modifiedLines, lines[0]) // Version
	modifiedLines = append(modifiedLines, lines[1]) // Field count (keep original)

	for i := 2; i < len(lines); i++ {
		line := lines[i]
		if strings.TrimSpace(line) == "" {
			continue
		}

		parts := strings.Split(line, "\t")
		if len(parts) < 7 {
			modifiedLines = append(modifiedLines, line)
			continue
		}

		colName := strings.Trim(parts[6], "\"")
		colNameLower := strings.ToLower(colName)

		// If it's a timestamp column, set Server Column Order to 0 (skip)
		if timestampMap[colNameLower] {
			parts[5] = "0"
			modifiedLines = append(modifiedLines, strings.Join(parts, "\t"))
		} else if sourceColMap[colNameLower] > 0 {
			// Map to source column order (1-based)
			parts[5] = fmt.Sprintf("%d", sourceColMap[colNameLower])
			modifiedLines = append(modifiedLines, strings.Join(parts, "\t"))
		} else {
			// Column not in source SELECT, skip it
			parts[5] = "0"
			modifiedLines = append(modifiedLines, strings.Join(parts, "\t"))
		}
	}

	// Step 5: Write modified format file with proper line endings
	output := strings.Join(modifiedLines, "\n") + "\n"
	if err := os.WriteFile(formatFile, []byte(output), 0644); err != nil {
		return fmt.Errorf("failed to write format file: %w", err)
	}

	// Debug: log first few lines of format file
	if len(modifiedLines) > 0 {
		log.Printf("Format file created: %d lines, version: %s, field count: %s", len(modifiedLines), modifiedLines[0], modifiedLines[1])
	}

	return nil
}

func buildPKInClause(pkValues []interface{}, pkColumn string) string {
	// Build IN clause with proper escaping for SQL injection prevention
	// PK values are safe (from our own database), but we still escape properly
	inValues := make([]string, 0)
	for _, pkVal := range pkValues {
		switch v := pkVal.(type) {
		case string:
			// Escape single quotes for SQL
			escaped := strings.ReplaceAll(v, "'", "''")
			inValues = append(inValues, fmt.Sprintf("'%s'", escaped))
		case int, int32, int64:
			inValues = append(inValues, fmt.Sprintf("%d", v))
		case float32, float64:
			inValues = append(inValues, fmt.Sprintf("%f", v))
		case nil:
			// Skip NULL values
			continue
		default:
			// For other types, convert to string and escape
			valStr := fmt.Sprintf("%v", v)
			escaped := strings.ReplaceAll(valStr, "'", "''")
			inValues = append(inValues, fmt.Sprintf("'%s'", escaped))
		}
	}
	return strings.Join(inValues, ", ")
}

func getFileSize(filePath string) int64 {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0
	}
	return info.Size()
}

func parseBCPRowCount(output string) int64 {
	// BCP output format: "Starting copy...\n...\nX rows copied.\n..."
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "rows copied") {
			// Extract number from "X rows copied" or "X rows successfully bulk-copied"
			parts := strings.Fields(line)
			for i, part := range parts {
				if (part == "rows" || part == "row") && i > 0 {
					if count, err := strconv.ParseInt(parts[i-1], 10, 64); err == nil {
						return count
					}
				}
			}
		}
	}
	return 0
}

func updateLastFullSync(mappingID int) {
	configDB.Exec("UPDATE table_mappings SET last_full_sync_at = GETUTCDATE() WHERE mapping_id = @p1", mappingID)
}


func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func getColumnTypes(row []interface{}) []string {
	types := make([]string, len(row))
	for i, val := range row {
		if val == nil {
			types[i] = "nil"
		} else {
			types[i] = fmt.Sprintf("%T", val)
		}
	}
	return types
}

func getSampleValues(row []interface{}, count int) []interface{} {
	if len(row) < count {
		count = len(row)
	}
	sample := make([]interface{}, count)
	for i := 0; i < count; i++ {
		if row[i] == nil {
			sample[i] = nil
		} else if b, ok := row[i].([]byte); ok {
			if len(b) <= 20 {
				sample[i] = fmt.Sprintf("[]byte(len=%d)", len(b))
			} else {
				sample[i] = fmt.Sprintf("[]byte(len=%d, first10=%x)", len(b), b[:10])
			}
		} else {
			sample[i] = row[i]
		}
	}
	return sample
}
