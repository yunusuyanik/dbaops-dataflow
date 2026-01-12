package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
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
	configDB        *sql.DB
	stopChan        = make(chan struct{})
	wg              sync.WaitGroup
	configMu        sync.RWMutex
	cdcInterval     = 10 * time.Second
	verificationInterval = 5 * time.Minute
	parallelWorkers = 5
	batchSize       = 1000
	maxRetryAttempts = 3
	retryDelay      = 30 * time.Second

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
	MappingID            int
	FlowID               int
	SourceDatabase       string
	SourceSchema         string
	SourceTable          string
	DestDatabase         string
	DestSchema           string
	DestTable            string
	PrimaryKeyColumn     string
	IsFullSync           bool
	IsEnabled            bool
	CDCEnabled           bool
	LastCDCLSN           sql.NullString
	LastFullSyncAt       sql.NullTime
	SourceConnString     string
	DestConnString       string
}

// SyncStatus represents current sync status
type SyncStatus struct {
	StatusID        int64
	MappingID       int
	SyncType        string
	Status          string
	RecordsProcessed int64
	RecordsFailed   int64
	StartedAt       time.Time
	CompletedAt     sql.NullTime
	ErrorMessage    sql.NullString
	LastProcessedPK sql.NullString
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

	// Write to both stdout and file
	multiWriter := io.MultiWriter(os.Stdout, file)
	log.SetOutput(multiWriter)
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
			logError(&mapping.MappingID, &mapping.FlowID, "SYNC",
				fmt.Sprintf("Panic recovered: %v", r), nil)
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
		logError(&mapping.MappingID, &mapping.FlowID, "CONNECTION",
			fmt.Sprintf("Failed to connect to source server: %v", err), nil)
		return
	}
	defer sourceDB.Close()

	updateLastConnected(mapping.FlowID)

	if needsFullSync(mapping) {
		performFullSync(mapping, sourceDB)
	} else if mapping.CDCEnabled {
		processCDC(mapping, sourceDB)
	}
}

func performAllVerifications() {
	mappings, err := getEnabledMappings()
	if err != nil {
		log.Printf("Error getting mappings for verification: %v", err)
		return
	}

	if len(mappings) == 0 {
		return
	}

	configMu.RLock()
	workers := parallelWorkers
	configMu.RUnlock()

	if workers > len(mappings) {
		workers = len(mappings)
	}

	jobs := make(chan TableMapping, len(mappings))
	var wgWorkers sync.WaitGroup

	for i := 0; i < workers; i++ {
		wgWorkers.Add(1)
		go func() {
			defer wgWorkers.Done()
			for mapping := range jobs {
				sourceDB, err := connectToServer(mapping.SourceConnString)
				if err != nil {
					continue
				}
				performVerification(mapping, sourceDB)
				sourceDB.Close()
			}
		}()
	}

	for _, mapping := range mappings {
		jobs <- mapping
	}
	close(jobs)

	wgWorkers.Wait()
}

func needsFullSync(mapping TableMapping) bool {
	return mapping.IsFullSync
}

func performFullSync(mapping TableMapping, sourceDB *sql.DB) {
	// Panic recovery for full sync
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in performFullSync (mapping_id=%d) recovered: %v", mapping.MappingID, r)
			logError(&mapping.MappingID, &mapping.FlowID, "FULL_SYNC",
				fmt.Sprintf("Panic recovered: %v", r), nil)
		}
	}()

	statusID := startSyncStatus(mapping.MappingID, "FULL_SYNC", "RUNNING")
	if statusID == 0 {
		return
	}

	logSync(mapping.MappingID, "INFO", "Full sync started", "FULL_SYNC", 0, 0)

	if !validateSchema(mapping, sourceDB) {
		log.Printf("[FULL_SYNC] Step 1 FAILED: Schema validation failed for mapping_id=%d", mapping.MappingID)
		updateSyncStatus(statusID, "ERROR", 0, 0, "Schema validation failed")
		logError(&mapping.MappingID, &mapping.FlowID, "SCHEMA", "Schema validation failed", nil)
		return
	}

	log.Printf("[FULL_SYNC] Step 1 SUCCESS: Schema validation passed for mapping_id=%d", mapping.MappingID)
	logSync(mapping.MappingID, "INFO", "Schema validation passed", "SCHEMA_CHECK", 0, 0)

	log.Printf("[FULL_SYNC] Step 2: Connecting to destination server for mapping_id=%d...", mapping.MappingID)
	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		log.Printf("[FULL_SYNC] Step 2 FAILED: Failed to connect to destination server for mapping_id=%d: %v", mapping.MappingID, err)
		updateSyncStatus(statusID, "ERROR", 0, 0, err.Error())
		logError(&mapping.MappingID, &mapping.FlowID, "CONNECTION",
			fmt.Sprintf("Failed to connect to destination server: %v", err), nil)
		return
	}
	defer destDB.Close()
	log.Printf("[FULL_SYNC] Step 2 SUCCESS: Connected to destination server for mapping_id=%d", mapping.MappingID)

	log.Printf("[FULL_SYNC] Step 3: Switching to destination database [%s] for mapping_id=%d...", mapping.DestDatabase, mapping.MappingID)
	_, err = destDB.Exec(fmt.Sprintf("USE [%s]", mapping.DestDatabase))
	if err != nil {
		log.Printf("[FULL_SYNC] Step 3 FAILED: Failed to switch to destination database for mapping_id=%d: %v", mapping.MappingID, err)
		updateSyncStatus(statusID, "ERROR", 0, 0, err.Error())
		return
	}
	log.Printf("[FULL_SYNC] Step 3 SUCCESS: Switched to destination database [%s] for mapping_id=%d", mapping.DestDatabase, mapping.MappingID)

	startTime := time.Now()
	recordsProcessed := int64(0)
	recordsFailed := int64(0)

	log.Printf("[FULL_SYNC] Step 4: Getting identity columns for mapping_id=%d...", mapping.MappingID)
	identityColumns, err := getIdentityColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	if err != nil {
		log.Printf("[FULL_SYNC] Step 4 WARNING: Failed to get identity columns for mapping_id=%d: %v", mapping.MappingID, err)
		logSync(mapping.MappingID, "WARNING", fmt.Sprintf("Failed to get identity columns: %v", err), "FULL_SYNC", 0, 0)
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
		updateSyncStatus(statusID, "ERROR", recordsProcessed, recordsFailed, err.Error())
		logError(&mapping.MappingID, &mapping.FlowID, "SYNC",
			fmt.Sprintf("Failed to get source columns: %v", err), nil)
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
		updateSyncStatus(statusID, "ERROR", recordsProcessed, recordsFailed, fmt.Sprintf("Primary key column %s not found in source table", mapping.PrimaryKeyColumn))
		return
	}

	if len(columns) == 0 {
		updateSyncStatus(statusID, "ERROR", recordsProcessed, recordsFailed, "No columns to sync (all are timestamp)")
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

	log.Printf("[FULL_SYNC] Step 7: Getting flow details (flow_id=%d) for BCP operations...", mapping.FlowID)
	// 1. Get flow details for BCP
	flow, err := getFlowByID(mapping.FlowID)
	if err != nil {
		log.Printf("[FULL_SYNC] Step 7 FAILED: Failed to get flow details for mapping_id=%d: %v", mapping.MappingID, err)
		updateSyncStatus(statusID, "ERROR", 0, 0, fmt.Sprintf("Failed to get flow details: %v", err))
		return
	}
	log.Printf("[FULL_SYNC] Step 7 SUCCESS: Got flow details (source: %s:%d, dest: %s:%d) for mapping_id=%d",
		flow.SourceServer, flow.SourcePort, flow.DestServer, flow.DestPort, mapping.MappingID)

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
		logSync(mapping.MappingID, "WARNING",
			fmt.Sprintf("Failed to truncate dest table (may not exist): %v", err), "FULL_SYNC", 0, 0)
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
			updateSyncStatus(statusID, "ERROR", 0, 0, errorMsg)
			logSync(mapping.MappingID, "ERROR", errorMsg, "FULL_SYNC", 0, 0)
		} else {
			log.Printf("[FULL_SYNC] Step 10 FAILED: BCP export failed for mapping_id=%d: %v, output: %s", mapping.MappingID, err, outputOutStr)
			updateSyncStatus(statusID, "ERROR", 0, 0, fmt.Sprintf("BCP export failed: %v, output: %s", err, outputOutStr))
			logSync(mapping.MappingID, "ERROR", fmt.Sprintf("BCP export failed: %v, output: %s", err, outputOutStr), "FULL_SYNC", 0, 0)
		}
		return
	}
	
	// Parse export output for row count
	exportRows := parseBCPRowCount(outputOutStr)
	log.Printf("[FULL_SYNC] Step 10 SUCCESS: Export completed - %d rows exported for mapping_id=%d", exportRows, mapping.MappingID)
	logSync(mapping.MappingID, "INFO", fmt.Sprintf("BCP export completed: %d rows", exportRows), "FULL_SYNC", exportRows, 0)

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
			updateSyncStatus(statusID, "ERROR", 0, 0, errorMsg)
			logSync(mapping.MappingID, "ERROR", errorMsg, "FULL_SYNC", 0, 0)
		} else {
			errorMsg := fmt.Sprintf("BCP import failed: %v, output: %s", err, outputInStr)
			log.Printf("[FULL_SYNC] Step 11 FAILED: BCP import failed for mapping_id=%d: %v, output: %s", mapping.MappingID, err, outputInStr)
			updateSyncStatus(statusID, "ERROR", 0, 0, errorMsg)
			logSync(mapping.MappingID, "ERROR", errorMsg, "FULL_SYNC", 0, 0)
		}
		return
	}
	
	// Parse import output for row count
	importRows := parseBCPRowCount(outputInStr)
	log.Printf("[FULL_SYNC] Step 11 SUCCESS: Import completed - %d rows imported for mapping_id=%d", importRows, mapping.MappingID)
	log.Printf("[FULL_SYNC] Step 11: BCP import output: %s", outputInStr)
	logSync(mapping.MappingID, "INFO", fmt.Sprintf("BCP import completed: %d rows", importRows), "FULL_SYNC", importRows, 0)
	
	// Verify row count matches
	if exportRows > 0 && importRows != exportRows {
		log.Printf("[FULL_SYNC] Step 12 WARNING: Row count mismatch for mapping_id=%d! Exported: %d, Imported: %d", mapping.MappingID, exportRows, importRows)
		logSync(mapping.MappingID, "WARNING", fmt.Sprintf("Row count mismatch: exported %d, imported %d", exportRows, importRows), "FULL_SYNC", 0, 0)
	} else {
		log.Printf("[FULL_SYNC] Step 12 SUCCESS: Row count verified - exported: %d, imported: %d for mapping_id=%d", exportRows, importRows, mapping.MappingID)
	}

	// Update flags
	log.Printf("[FULL_SYNC] Step 13: Updating last_full_sync_at timestamp for mapping_id=%d...", mapping.MappingID)
	updateLastFullSync(mapping.MappingID)

	duration := time.Since(startTime)
	updateSyncStatus(statusID, "COMPLETED", 0, 0, "")

	log.Printf("[FULL_SYNC] COMPLETED: Full sync finished for mapping_id=%d, duration: %v, exported: %d rows, imported: %d rows",
		mapping.MappingID, duration, exportRows, importRows)
	logSync(mapping.MappingID, "INFO", 
		fmt.Sprintf("Full sync completed via BCP-to-BCP, duration: %v", duration),
		"FULL_SYNC", 0, int(duration.Milliseconds()))
}

func processCDC(mapping TableMapping, sourceDB *sql.DB) {
	// Panic recovery for CDC processing
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in processCDC (mapping_id=%d) recovered: %v", mapping.MappingID, r)
			logError(&mapping.MappingID, &mapping.FlowID, "CDC",
				fmt.Sprintf("Panic recovered: %v", r), nil)
		}
	}()

	if !isCDCEnabled(mapping, sourceDB) {
		logSync(mapping.MappingID, "WARNING", "CDC not enabled on source table", "CDC", 0, 0)
		return
	}

	statusID := startSyncStatus(mapping.MappingID, "CDC", "RUNNING")
	if statusID == 0 {
		return
	}

	lastLSN := mapping.LastCDCLSN.String
	if lastLSN == "" {
		lastLSN = getMinLSN(mapping, sourceDB)
		if lastLSN == "" {
			updateSyncStatus(statusID, "COMPLETED", 0, 0, "No CDC data available")
			return
		}
	}

	changes, err := getCDCChanges(mapping, sourceDB, lastLSN)
	if err != nil {
		updateSyncStatus(statusID, "ERROR", 0, 0, err.Error())
		logError(&mapping.MappingID, &mapping.FlowID, "CDC",
			fmt.Sprintf("Failed to get CDC changes: %v", err), nil)
		return
	}

	if len(changes) == 0 {
		updateSyncStatus(statusID, "COMPLETED", 0, 0, "")
		return
	}

	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		updateSyncStatus(statusID, "ERROR", 0, 0, err.Error())
		return
	}
	defer destDB.Close()

	_, err = destDB.Exec(fmt.Sprintf("USE [%s]", mapping.DestDatabase))
	if err != nil {
		updateSyncStatus(statusID, "ERROR", 0, 0, err.Error())
		return
	}

	recordsProcessed := int64(0)
	recordsFailed := int64(0)
	var newLSN string

		for _, change := range changes {
		if lsnVal, ok := change["__$start_lsn"]; ok {
			if lsnBytes, ok := lsnVal.([]byte); ok {
				newLSN = hex.EncodeToString(lsnBytes)
			} else if lsnStr, ok := lsnVal.(string); ok {
				newLSN = lsnStr
			} else {
				newLSN = fmt.Sprintf("%v", lsnVal)
			}
		}
		
		var operation int
		if opVal, ok := change["__$operation"]; ok {
			switch v := opVal.(type) {
			case int:
				operation = v
			case int64:
				operation = int(v)
			case string:
				if opInt, err := strconv.Atoi(v); err == nil {
					operation = opInt
				}
			}
		}

		switch operation {
		case 1:
			err = applyDelete(destDB, mapping, change)
		case 2:
			err = applyInsert(destDB, mapping, change)
		case 3:
		case 4:
			err = applyUpdate(destDB, mapping, change)
		default:
			logSync(mapping.MappingID, "WARNING", fmt.Sprintf("Unknown CDC operation: %d", operation), "CDC", 0, 0)
			continue
		}

		if err != nil {
			recordsFailed++
			logSync(mapping.MappingID, "ERROR", fmt.Sprintf("Failed to apply change: %v", err), "CDC", 0, 0)
		} else {
			recordsProcessed++
		}
	}

	if newLSN != "" {
		updateLastLSN(mapping.MappingID, newLSN)
	}

	updateSyncStatus(statusID, "COMPLETED", recordsProcessed, recordsFailed, "")
	
	if recordsProcessed > 0 {
		logSync(mapping.MappingID, "INFO", 
			fmt.Sprintf("CDC sync completed: %d processed, %d failed", recordsProcessed, recordsFailed),
			"CDC", recordsProcessed, 0)
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
	var captureInstance sql.NullString
	query := fmt.Sprintf(`
		SELECT capture_instance 
		FROM cdc.change_tables 
		WHERE source_object_id = OBJECT_ID('[%s].[%s].[%s]')
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	
	if err := db.QueryRow(query).Scan(&captureInstance); err != nil || !captureInstance.Valid {
		return ""
	}

	lsnQuery := fmt.Sprintf(`SELECT MIN(__$start_lsn) FROM cdc.[%s]`, captureInstance.String)

	var lsn []byte
	if err := db.QueryRow(lsnQuery).Scan(&lsn); err != nil {
		return ""
	}
	
	return hex.EncodeToString(lsn)
}

func getCDCChanges(mapping TableMapping, db *sql.DB, lastLSN string) ([]map[string]interface{}, error) {
	var captureInstance sql.NullString
	query := fmt.Sprintf(`
		SELECT capture_instance 
		FROM cdc.change_tables 
		WHERE source_object_id = OBJECT_ID('[%s].[%s].[%s]')
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	
	if err := db.QueryRow(query).Scan(&captureInstance); err != nil || !captureInstance.Valid {
		return nil, fmt.Errorf("CDC capture instance not found for table")
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
	`, captureInstance.String)

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
			log.Printf("PANIC in performVerification (mapping_id=%d) recovered: %v", mapping.MappingID, r)
			logError(&mapping.MappingID, nil, "VERIFICATION",
				fmt.Sprintf("Panic recovered: %v", r), nil)
		}
	}()

	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		return
	}
	defer destDB.Close()

	_, err = destDB.Exec(fmt.Sprintf("USE [%s]", mapping.DestDatabase))
	if err != nil {
		return
	}

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
		return
	}

	query := fmt.Sprintf(`
		SELECT TOP 10000 %s
		FROM [%s].[%s].[%s]
		ORDER BY %s DESC
	`, strings.Join(verifyCols, ", "),
		mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, mapping.PrimaryKeyColumn)

	destRows, err := destDB.Query(query)
	if err != nil {
		logError(&mapping.MappingID, nil, "VERIFICATION", fmt.Sprintf("Failed to query dest: %v", err), nil)
		return
	}
	defer destRows.Close()

	destColumns, _ := destRows.Columns()
	destData := make([]map[string]interface{}, 0)

	for destRows.Next() {
		values := make([]interface{}, len(destColumns))
		valuePtrs := make([]interface{}, len(destColumns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := destRows.Scan(valuePtrs...); err != nil {
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
		return
	}

	pkValues := make([]interface{}, 0)
	for _, row := range destData {
		if pkVal, ok := row[mapping.PrimaryKeyColumn]; ok {
			pkValues = append(pkValues, pkVal)
		}
	}

	if len(pkValues) == 0 {
		return
	}

	// Build IN clause values (limit to 1000 for safety)
	// Go-MSSQL driver doesn't handle IN clause parameters well, so we use direct values
	// PK values are safe (integer or string from our own database)
	inValues := make([]string, 0)
	for i, pkVal := range pkValues {
		if i >= 1000 {
			break
		}
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

	sourceQuery := fmt.Sprintf(`
		SELECT %s
		FROM [%s].[%s].[%s]
		WHERE %s IN (%s)
		ORDER BY %s
	`, strings.Join(verifyCols, ", "),
		mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
		mapping.PrimaryKeyColumn, strings.Join(inValues, ", "),
		mapping.PrimaryKeyColumn)

	sourceRows, err := sourceDB.Query(sourceQuery)
	if err != nil {
		logError(&mapping.MappingID, nil, "VERIFICATION", fmt.Sprintf("Failed to query source: %v", err), nil)
		return
	}
	defer sourceRows.Close()

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

	mismatches := int64(0)
	compared := int64(0)

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

	status := "PASSED"
	if mismatches > 0 {
		status = "FAILED"
	}

	logVerification(mapping.MappingID, "MD5_COMPARISON", "", "", 
		int64(len(destData)), int64(len(sourceData)), compared, mismatches, status, "")

	if mismatches > 0 {
		logError(&mapping.MappingID, nil, "VERIFICATION", 
			fmt.Sprintf("MD5 verification failed: %d mismatches out of %d compared", mismatches, compared), nil)
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
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(fmt.Sprintf("%v", row[k]))
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
		if err := rows.Scan(&m.MappingID, &m.FlowID, &m.SourceDatabase, &m.SourceSchema, &m.SourceTable,
			&m.DestDatabase, &m.DestSchema, &m.DestTable, &m.PrimaryKeyColumn,
			&m.IsFullSync, &m.IsEnabled, &m.CDCEnabled, &m.LastCDCLSN, &m.LastFullSyncAt,
			&m.SourceConnString, &m.DestConnString); err != nil {
			continue
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

func startSyncStatus(mappingID int, syncType, status string) int64 {
	var statusID int64
	err := configDB.QueryRow(`
		INSERT INTO sync_status (mapping_id, sync_type, status, records_processed, records_failed, started_at)
		OUTPUT INSERTED.status_id
		VALUES (@p1, @p2, @p3, 0, 0, GETUTCDATE())
	`, mappingID, syncType, status).Scan(&statusID)
	
	if err != nil {
		log.Printf("Failed to create sync status: %v", err)
		return 0
	}
	return statusID
}

func updateSyncStatus(statusID int64, status string, processed, failed int64, errorMsg string) {
	if errorMsg != "" {
		configDB.Exec(`
			UPDATE sync_status 
			SET status = @p1, records_processed = @p2, records_failed = @p3, 
			    completed_at = GETUTCDATE(), error_message = @p4
			WHERE status_id = @p5
		`, status, processed, failed, errorMsg, statusID)
	} else {
		configDB.Exec(`
			UPDATE sync_status 
			SET status = @p1, records_processed = @p2, records_failed = @p3, completed_at = GETUTCDATE()
			WHERE status_id = @p4
		`, status, processed, failed, statusID)
	}
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

func updateLastProcessedPK(statusID int64, pkValue string) {
	configDB.Exec("UPDATE sync_status SET last_processed_pk = @p1 WHERE status_id = @p2", pkValue, statusID)
}

func updateLastLSN(mappingID int, lsn string) {
	configDB.Exec("UPDATE table_mappings SET last_cdc_lsn = @p1 WHERE mapping_id = @p2", lsn, mappingID)
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

func logSync(mappingID int, level, message, syncType string, recordsCount int64, execTimeMs int) {
	configDB.Exec(`
		INSERT INTO sync_logs (mapping_id, log_level, log_message, sync_type, records_count, execution_time_ms)
		VALUES (@p1, @p2, @p3, @p4, @p5, @p6)
	`, mappingID, level, message, syncType, recordsCount, execTimeMs)
}

func logError(mappingID *int, flowID *int, errorType, message string, details interface{}) {
	var detailsStr string
	if details != nil {
		detailsStr = fmt.Sprintf("%v", details)
	}

	configDB.Exec(`
		INSERT INTO error_logs (mapping_id, flow_id, error_type, error_message, error_details)
		VALUES (@p1, @p2, @p3, @p4, @p5)
	`, mappingID, flowID, errorType, message, detailsStr)
}

func logVerification(mappingID int, vType, sourceMD5, destMD5 string, 
	sourceCount, destCount, compared, mismatches int64, status, details string) {
	configDB.Exec(`
		INSERT INTO verification_logs 
		(mapping_id, verification_type, source_md5, dest_md5, source_row_count, dest_row_count,
		 records_compared, mismatches_found, verification_status, verification_details)
		VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10)
	`, mappingID, vType, sourceMD5, destMD5, sourceCount, destCount, compared, mismatches, status, details)
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
