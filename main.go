package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

const (
	// Config
	destConnectionString = "server=%s;user id=%s;password=%s;database=dbaops_dataflow;encrypt=disable"
	checkInterval        = 10 * time.Second // CDC check interval
	verificationInterval = 5 * time.Minute  // MD5 verification interval
	maxRetryAttempts    = 3
	retryDelay          = 30 * time.Second
)

var (
	destDB   *sql.DB
	stopChan = make(chan struct{})
	wg       sync.WaitGroup
)

// SourceServer represents a source server configuration
type SourceServer struct {
	ServerID      int
	ServerName    string
	ConnString    string
	IsEnabled     bool
	LastConnected *time.Time
}

// TableMapping represents a source-dest table mapping
type TableMapping struct {
	MappingID            int
	ServerID             int
	SourceDatabase       string
	SourceSchema         string
	SourceTable          string
	DestDatabase         string
	DestSchema           string
	DestTable            string
	PrimaryKeyColumn     string
	FullSyncTriggerCol   sql.NullString
	IsEnabled            bool
	CDCEnabled           bool
	LastCDCLSN           sql.NullString
	LastFullSyncAt       sql.NullTime
}

// SyncStatus represents current sync status
type SyncStatus struct {
	StatusID        int64
	MappingID      int
	SyncType       string
	Status         string
	RecordsProcessed int64
	RecordsFailed   int64
	StartedAt      time.Time
	CompletedAt    sql.NullTime
	ErrorMessage   sql.NullString
	LastProcessedPK sql.NullString
}

func init() {
	// Get dest connection from env
	destServer := getEnv("DEST_SERVER", "localhost")
	destUser := getEnv("DEST_USER", "sa")
	destPass := getEnv("DEST_PASS", "")
	
	if destPass == "" {
		log.Fatal("DEST_PASS environment variable required")
	}

	connStr := fmt.Sprintf(destConnectionString, destServer, destUser, destPass)
	var err error
	destDB, err = sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to dest database: %v", err)
	}

	if err := destDB.Ping(); err != nil {
		log.Fatalf("Failed to ping dest database: %v", err)
	}

	destDB.SetMaxOpenConns(50)
	destDB.SetMaxIdleConns(10)
	destDB.SetConnMaxLifetime(5 * time.Minute)

	log.Println("Connected to destination database")
}

func main() {
	log.Println("DBAOps DataFlow starting...")

	// Start main sync loop
	wg.Add(1)
	go syncLoop()

	// Start verification loop
	wg.Add(1)
	go verificationLoop()

	// Wait for stop signal (Ctrl+C or SIGTERM)
	// In production, you'd handle signals properly
	select {
	case <-stopChan:
		log.Println("Stopping DBAOps DataFlow...")
	}

	// Graceful shutdown
	close(stopChan)
	wg.Wait()
	
	destDB.Close()
	log.Println("Shutdown complete")
}

func syncLoop() {
	defer wg.Done()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			processAllServers()
		}
	}
}

func verificationLoop() {
	defer wg.Done()

	ticker := time.NewTicker(verificationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			performAllVerifications()
		}
	}
}

func performAllVerifications() {
	servers, err := getEnabledServers()
	if err != nil {
		log.Printf("Error getting servers for verification: %v", err)
		return
	}

	for _, server := range servers {
		if !server.IsEnabled {
			continue
		}

		mappings, err := getTableMappings(server.ServerID)
		if err != nil {
			continue
		}

		sourceDB, err := connectToSource(server.ConnString)
		if err != nil {
			continue
		}

		for _, mapping := range mappings {
			if !mapping.IsEnabled {
				continue
			}

			wg.Add(1)
			go func(m TableMapping, s SourceServer, db *sql.DB) {
				defer wg.Done()
				defer db.Close()
				performVerification(m, db)
			}(mapping, server, sourceDB)
		}
	}
}

func processAllServers() {
	servers, err := getEnabledServers()
	if err != nil {
		log.Printf("Error getting servers: %v", err)
		return
	}

	for _, server := range servers {
		if !server.IsEnabled {
			continue
		}

		wg.Add(1)
		go func(s SourceServer) {
			defer wg.Done()
			processServer(s)
		}(server)
	}
}

func processServer(server SourceServer) {
	// Update last connected
	updateLastConnected(server.ServerID)

	// Get enabled table mappings for this server
	mappings, err := getTableMappings(server.ServerID)
	if err != nil {
		logError(nil, &server.ServerID, "CONNECTION", fmt.Sprintf("Failed to get table mappings: %v", err), nil)
		return
	}

	// Connect to source server
	sourceDB, err := connectToSource(server.ConnString)
	if err != nil {
		logError(nil, &server.ServerID, "CONNECTION", fmt.Sprintf("Failed to connect to source: %v", err), nil)
		return
	}
	defer sourceDB.Close()

	// Process each table mapping
	for _, mapping := range mappings {
		if !mapping.IsEnabled {
			continue
		}

		// Check if full sync is needed
		if needsFullSync(mapping, sourceDB) {
			wg.Add(1)
			go func(m TableMapping) {
				defer wg.Done()
				performFullSync(m, sourceDB, server)
			}(mapping)
		} else if mapping.CDCEnabled {
			// Process CDC changes
			wg.Add(1)
			go func(m TableMapping) {
				defer wg.Done()
				processCDC(m, sourceDB, server)
			}(mapping)
		}
	}
}

func needsFullSync(mapping TableMapping, sourceDB *sql.DB) bool {
	if !mapping.FullSyncTriggerCol.Valid {
		return false
	}

	// Check if trigger column is set to 1
	query := fmt.Sprintf(`
		SELECT TOP 1 CAST(%s AS INT) 
		FROM [%s].[%s].[%s]
		WHERE %s = 1
	`, mapping.FullSyncTriggerCol.String,
		mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
		mapping.FullSyncTriggerCol.String)

	var triggerValue int
	err := sourceDB.QueryRow(query).Scan(&triggerValue)
	if err != nil {
		return false
	}

	return triggerValue == 1
}

func performFullSync(mapping TableMapping, sourceDB *sql.DB, server SourceServer) {
	statusID := startSyncStatus(mapping.MappingID, "FULL_SYNC", "RUNNING")
	if statusID == 0 {
		return
	}

	logSync(mapping.MappingID, "INFO", "Full sync started", "FULL_SYNC", 0, 0)

	// Schema validation
	if !validateSchema(mapping, sourceDB) {
		updateSyncStatus(statusID, "ERROR", 0, 0, "Schema validation failed")
		logError(&mapping.MappingID, &server.ServerID, "SCHEMA", "Schema validation failed", nil)
		return
	}

	logSync(mapping.MappingID, "INFO", "Schema validation passed", "SCHEMA_CHECK", 0, 0)

	// Get source connection string for dest operations
	destConnStr := getDestConnectionString(mapping.DestDatabase)

	// Get all data from source
	startTime := time.Now()
	recordsProcessed := int64(0)
	recordsFailed := int64(0)

	// Build SELECT query
	selectQuery := fmt.Sprintf(`
		SELECT * FROM [%s].[%s].[%s]
		ORDER BY %s
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable, mapping.PrimaryKeyColumn)

	rows, err := sourceDB.Query(selectQuery)
	if err != nil {
		updateSyncStatus(statusID, "ERROR", recordsProcessed, recordsFailed, err.Error())
		logError(&mapping.MappingID, &server.ServerID, "SYNC", fmt.Sprintf("Failed to query source: %v", err), nil)
		return
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		updateSyncStatus(statusID, "ERROR", recordsProcessed, recordsFailed, err.Error())
		return
	}

	// Connect to dest database
	destDBForTable, err := sql.Open("sqlserver", destConnStr)
	if err != nil {
		updateSyncStatus(statusID, "ERROR", recordsProcessed, recordsFailed, err.Error())
		return
	}
	defer destDBForTable.Close()

	// Truncate dest table (full sync)
	truncateQuery := fmt.Sprintf("TRUNCATE TABLE [%s].[%s].[%s]", mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	_, err = destDBForTable.Exec(truncateQuery)
	if err != nil {
		logSync(mapping.MappingID, "WARNING", fmt.Sprintf("Failed to truncate dest table (may not exist): %v", err), "FULL_SYNC", 0, 0)
		// Continue anyway, will try to insert
	}

	// Build INSERT query
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("@p%d", i+1)
	}
	insertQuery := fmt.Sprintf(`
		INSERT INTO [%s].[%s].[%s] (%s)
		VALUES (%s)
	`, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	// Batch insert
	batchSize := 1000
	batch := make([][]interface{}, 0, batchSize)
	var lastPKValue interface{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			recordsFailed++
			logSync(mapping.MappingID, "ERROR", fmt.Sprintf("Failed to scan row: %v", err), "FULL_SYNC", 0, 0)
			continue
		}

		// Get PK value for tracking
		for i, col := range columns {
			if strings.EqualFold(col, mapping.PrimaryKeyColumn) {
				lastPKValue = values[i]
				break
			}
		}

		batch = append(batch, values)
		recordsProcessed++

		if len(batch) >= batchSize {
			if err := executeBatchInsert(destDBForTable, insertQuery, columns, batch); err != nil {
				recordsFailed += int64(len(batch))
				logSync(mapping.MappingID, "ERROR", fmt.Sprintf("Batch insert failed: %v", err), "FULL_SYNC", int64(len(batch)), 0)
			}
			batch = batch[:0]
		}
	}

	// Insert remaining batch
	if len(batch) > 0 {
		if err := executeBatchInsert(destDBForTable, insertQuery, columns, batch); err != nil {
			recordsFailed += int64(len(batch))
			logSync(mapping.MappingID, "ERROR", fmt.Sprintf("Final batch insert failed: %v", err), "FULL_SYNC", int64(len(batch)), 0)
		}
	}

	// Reset trigger column to 0
	if mapping.FullSyncTriggerCol.Valid {
		resetQuery := fmt.Sprintf(`
			UPDATE [%s].[%s].[%s]
			SET %s = 0
			WHERE %s = 1
		`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
			mapping.FullSyncTriggerCol.String, mapping.FullSyncTriggerCol.String)
		sourceDB.Exec(resetQuery) // Ignore errors, source is read-only intent
	}

	// Update last full sync time
	updateLastFullSync(mapping.MappingID)

	// Update sync status
	duration := time.Since(startTime)
	updateSyncStatus(statusID, "COMPLETED", recordsProcessed, recordsFailed, "")
	
	var lastPKStr string
	if lastPKValue != nil {
		lastPKStr = fmt.Sprintf("%v", lastPKValue)
	}
	updateLastProcessedPK(statusID, lastPKStr)

	logSync(mapping.MappingID, "INFO", 
		fmt.Sprintf("Full sync completed: %d processed, %d failed, duration: %v", 
			recordsProcessed, recordsFailed, duration),
		"FULL_SYNC", recordsProcessed, int(duration.Milliseconds()))
}

func processCDC(mapping TableMapping, sourceDB *sql.DB, server SourceServer) {
	// Check if CDC is enabled on source table
	if !isCDCEnabled(mapping, sourceDB) {
		logSync(mapping.MappingID, "WARNING", "CDC not enabled on source table", "CDC", 0, 0)
		return
	}

	statusID := startSyncStatus(mapping.MappingID, "CDC", "RUNNING")
	if statusID == 0 {
		return
	}

	// Get last processed LSN
	lastLSN := mapping.LastCDCLSN.String
	if lastLSN == "" {
		// Get minimum LSN
		lastLSN = getMinLSN(mapping, sourceDB)
		if lastLSN == "" {
			updateSyncStatus(statusID, "COMPLETED", 0, 0, "No CDC data available")
			return
		}
	}

	// Get changes since last LSN
	changes, err := getCDCChanges(mapping, sourceDB, lastLSN)
	if err != nil {
		updateSyncStatus(statusID, "ERROR", 0, 0, err.Error())
		logError(&mapping.MappingID, &server.ServerID, "CDC", fmt.Sprintf("Failed to get CDC changes: %v", err), nil)
		return
	}

	if len(changes) == 0 {
		updateSyncStatus(statusID, "COMPLETED", 0, 0, "")
		return
	}

	// Apply changes to dest
	recordsProcessed := int64(0)
	recordsFailed := int64(0)
	var newLSN string

	destConnStr := getDestConnectionString(mapping.DestDatabase)
	destDBForTable, err := sql.Open("sqlserver", destConnStr)
	if err != nil {
		updateSyncStatus(statusID, "ERROR", 0, 0, err.Error())
		return
	}
	defer destDBForTable.Close()

	for _, change := range changes {
		// Extract LSN (binary) and convert to hex string
		if lsnVal, ok := change["__$start_lsn"]; ok {
			if lsnBytes, ok := lsnVal.([]byte); ok {
				newLSN = hex.EncodeToString(lsnBytes)
			} else {
				newLSN = fmt.Sprintf("%v", lsnVal)
			}
		}

		operation := change["__$operation"].(int)
		switch operation {
		case 1: // Delete
			err = applyDelete(destDBForTable, mapping, change)
		case 2: // Insert
			err = applyInsert(destDBForTable, mapping, change)
		case 3: // Update (before)
			// Skip, will handle in update (after)
		case 4: // Update (after)
			err = applyUpdate(destDBForTable, mapping, change)
		}

		if err != nil {
			recordsFailed++
			logSync(mapping.MappingID, "ERROR", fmt.Sprintf("Failed to apply change: %v", err), "CDC", 0, 0)
		} else {
			recordsProcessed++
		}
	}

	// Update last LSN
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
	// Check if source table exists
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

	// Check if PK column exists
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

	// Get source columns
	sourceCols, err := getTableColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	if err != nil {
		return false
	}

	// Get dest columns (if table exists)
	destConnStr := getDestConnectionString(mapping.DestDatabase)
	destDBForTable, err := sql.Open("sqlserver", destConnStr)
	if err != nil {
		return false
	}
	defer destDBForTable.Close()

	destCols, err := getTableColumns(destDBForTable, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	if err != nil {
		// Table might not exist, that's OK for full sync
		return true
	}

	// Compare column names (case-insensitive)
	sourceColMap := make(map[string]bool)
	for _, col := range sourceCols {
		sourceColMap[strings.ToLower(col)] = true
	}

	for _, col := range destCols {
		if !sourceColMap[strings.ToLower(col)] {
			return false
		}
	}

	return true
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

func executeBatchInsert(db *sql.DB, query string, columns []string, batch [][]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range batch {
		args := make([]interface{}, len(row))
		for i, val := range row {
			args[i] = val
		}
		if _, err := stmt.Exec(args...); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func applyInsert(db *sql.DB, mapping TableMapping, change map[string]interface{}) error {
	// Extract column names (excluding CDC metadata columns)
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
	// Build SET clause
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

	// Build WHERE clause for PK
	pkValue, ok := change[mapping.PrimaryKeyColumn]
	if !ok {
		return fmt.Errorf("primary key column %s not found in change", mapping.PrimaryKeyColumn)
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
		return fmt.Errorf("primary key column %s not found in change", mapping.PrimaryKeyColumn)
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
	// Check if CDC is enabled on database
	var isCDCEnabled int
	db.QueryRow(fmt.Sprintf("SELECT is_cdc_enabled FROM sys.databases WHERE name = '%s'", mapping.SourceDatabase)).Scan(&isCDCEnabled)
	if isCDCEnabled == 0 {
		return false
	}

	// Check if CDC is enabled on table
	query := fmt.Sprintf(`
		SELECT COUNT(*) 
		FROM cdc.change_tables 
		WHERE source_object_id = OBJECT_ID('[%s].[%s].[%s]')
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)

	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		return false
	}
	return count > 0
}

func getMinLSN(mapping TableMapping, db *sql.DB) string {
	// Get capture instance name first
	var captureInstance sql.NullString
	query := fmt.Sprintf(`
		SELECT capture_instance 
		FROM cdc.change_tables 
		WHERE source_object_id = OBJECT_ID('[%s].[%s].[%s]')
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	
	if err := db.QueryRow(query).Scan(&captureInstance); err != nil || !captureInstance.Valid {
		return ""
	}

	// Get minimum LSN from CDC tables
	lsnQuery := fmt.Sprintf(`
		SELECT MIN(__$start_lsn)
		FROM cdc.[%s]
	`, captureInstance.String)

	var lsn []byte
	if err := db.QueryRow(lsnQuery).Scan(&lsn); err != nil {
		return ""
	}
	
	// Convert binary LSN to hex string for storage
	return hex.EncodeToString(lsn)
}

func getCDCChanges(mapping TableMapping, db *sql.DB, lastLSN string) ([]map[string]interface{}, error) {
	// Use CDC tables (cdc.capture_instance_CT)
	// First, get the capture instance name
	var captureInstance sql.NullString
	query := fmt.Sprintf(`
		SELECT capture_instance 
		FROM cdc.change_tables 
		WHERE source_object_id = OBJECT_ID('[%s].[%s].[%s]')
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	
	if err := db.QueryRow(query).Scan(&captureInstance); err != nil || !captureInstance.Valid {
		return nil, fmt.Errorf("CDC capture instance not found for table")
	}

	// Convert hex string back to binary for LSN comparison
	lsnBytes, err := hex.DecodeString(lastLSN)
	if err != nil {
		return nil, fmt.Errorf("invalid LSN format: %v", err)
	}

	// Query CDC changes - use parameterized query for LSN
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
				change[col] = hex.EncodeToString(b) // LSN is binary, convert to hex string
			} else {
				change[col] = val
			}
		}

		changes = append(changes, change)
	}

	return changes, nil
}

func performVerification(mapping TableMapping, sourceDB *sql.DB) {
	// Get last inserted PK from dest
	destConnStr := getDestConnectionString(mapping.DestDatabase)
	destDBForTable, err := sql.Open("sqlserver", destConnStr)
	if err != nil {
		return
	}
	defer destDBForTable.Close()

	// Get last 10k records from dest (or less if not available)
	query := fmt.Sprintf(`
		SELECT TOP 10000 *
		FROM [%s].[%s].[%s]
		ORDER BY %s DESC
	`, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, mapping.PrimaryKeyColumn)

	destRows, err := destDBForTable.Query(query)
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
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		destData = append(destData, row)
	}

	if len(destData) == 0 {
		return
	}

	// Get corresponding records from source
	pkValues := make([]interface{}, 0)
	for _, row := range destData {
		if pkVal, ok := row[mapping.PrimaryKeyColumn]; ok {
			pkValues = append(pkValues, pkVal)
		}
	}

	if len(pkValues) == 0 {
		return
	}

	// Build IN clause (limited to avoid query size issues)
	pkPlaceholders := make([]string, 0)
	pkArgs := make([]interface{}, 0)
	for i, pkVal := range pkValues {
		if i >= 1000 { // Limit to 1000 for safety
			break
		}
		pkPlaceholders = append(pkPlaceholders, fmt.Sprintf("@pk%d", i+1))
		pkArgs = append(pkArgs, pkVal)
	}

	sourceQuery := fmt.Sprintf(`
		SELECT *
		FROM [%s].[%s].[%s]
		WHERE %s IN (%s)
		ORDER BY %s
	`, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable,
		mapping.PrimaryKeyColumn, strings.Join(pkPlaceholders, ", "),
		mapping.PrimaryKeyColumn)

	sourceRows, err := sourceDB.Query(sourceQuery, pkArgs...)
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
				row[col] = string(b)
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

	// Compare MD5 hashes
	mismatches := int64(0)
	compared := int64(0)

	for _, destRow := range destData {
		pkVal := destRow[mapping.PrimaryKeyColumn]
		sourceRow, exists := sourceData[pkVal]

		if !exists {
			mismatches++
			continue
		}

		// Calculate MD5 for both rows
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

	// Log verification result
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

	// Build string representation
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

// Database helper functions

func getEnabledServers() ([]SourceServer, error) {
	query := `SELECT server_id, server_name, connection_string, is_enabled, last_connected_at 
	          FROM source_servers WHERE is_enabled = 1`
	
	rows, err := destDB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var servers []SourceServer
	for rows.Next() {
		var s SourceServer
		var lastConn sql.NullTime
		if err := rows.Scan(&s.ServerID, &s.ServerName, &s.ConnString, &s.IsEnabled, &lastConn); err != nil {
			continue
		}
		if lastConn.Valid {
			s.LastConnected = &lastConn.Time
		}
		servers = append(servers, s)
	}

	return servers, nil
}

func getTableMappings(serverID int) ([]TableMapping, error) {
	query := `SELECT mapping_id, server_id, source_database, source_schema, source_table,
	                 dest_database, dest_schema, dest_table, primary_key_column,
	                 full_sync_trigger_column, is_enabled, cdc_enabled, last_cdc_lsn, last_full_sync_at
	          FROM table_mappings 
	          WHERE server_id = @p1 AND is_enabled = 1`
	
	rows, err := destDB.Query(query, serverID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var mappings []TableMapping
	for rows.Next() {
		var m TableMapping
		if err := rows.Scan(&m.MappingID, &m.ServerID, &m.SourceDatabase, &m.SourceSchema, &m.SourceTable,
			&m.DestDatabase, &m.DestSchema, &m.DestTable, &m.PrimaryKeyColumn,
			&m.FullSyncTriggerCol, &m.IsEnabled, &m.CDCEnabled, &m.LastCDCLSN, &m.LastFullSyncAt); err != nil {
			continue
		}
		mappings = append(mappings, m)
	}

	return mappings, nil
}

func connectToSource(connString string) (*sql.DB, error) {
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func getDestConnectionString(database string) string {
	destServer := getEnv("DEST_SERVER", "localhost")
	destUser := getEnv("DEST_USER", "sa")
	destPass := getEnv("DEST_PASS", "")
	return fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s;encrypt=disable", 
		destServer, destUser, destPass, database)
}

func updateLastConnected(serverID int) {
	destDB.Exec("UPDATE source_servers SET last_connected_at = GETUTCDATE() WHERE server_id = @p1", serverID)
}

func startSyncStatus(mappingID int, syncType, status string) int64 {
	var statusID int64
	err := destDB.QueryRow(`
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
		destDB.Exec(`
			UPDATE sync_status 
			SET status = @p1, records_processed = @p2, records_failed = @p3, 
			    completed_at = GETUTCDATE(), error_message = @p4
			WHERE status_id = @p5
		`, status, processed, failed, errorMsg, statusID)
	} else {
		destDB.Exec(`
			UPDATE sync_status 
			SET status = @p1, records_processed = @p2, records_failed = @p3, completed_at = GETUTCDATE()
			WHERE status_id = @p4
		`, status, processed, failed, statusID)
	}
}

func updateLastProcessedPK(statusID int64, pkValue string) {
	destDB.Exec("UPDATE sync_status SET last_processed_pk = @p1 WHERE status_id = @p2", pkValue, statusID)
}

func updateLastLSN(mappingID int, lsn string) {
	destDB.Exec("UPDATE table_mappings SET last_cdc_lsn = @p1 WHERE mapping_id = @p2", lsn, mappingID)
}

func updateLastFullSync(mappingID int) {
	destDB.Exec("UPDATE table_mappings SET last_full_sync_at = GETUTCDATE() WHERE mapping_id = @p1", mappingID)
}

func logSync(mappingID int, level, message, syncType string, recordsCount int64, execTimeMs int) {
	destDB.Exec(`
		INSERT INTO sync_logs (mapping_id, log_level, log_message, sync_type, records_count, execution_time_ms)
		VALUES (@p1, @p2, @p3, @p4, @p5, @p6)
	`, mappingID, level, message, syncType, recordsCount, execTimeMs)
}

func logError(mappingID *int, serverID *int, errorType, message string, details interface{}) {
	var detailsStr string
	if details != nil {
		detailsStr = fmt.Sprintf("%v", details)
	}

	destDB.Exec(`
		INSERT INTO error_logs (mapping_id, server_id, error_type, error_message, error_details)
		VALUES (@p1, @p2, @p3, @p4, @p5)
	`, mappingID, serverID, errorType, message, detailsStr)
}

func logVerification(mappingID int, vType, sourceMD5, destMD5 string, 
	sourceCount, destCount, compared, mismatches int64, status, details string) {
	destDB.Exec(`
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

