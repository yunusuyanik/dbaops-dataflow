package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
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
)

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

func init() {
	configServer := getEnv("CONFIG_SERVER", "localhost")
	configUser := getEnv("CONFIG_USER", "sa")
	configPass := getEnv("CONFIG_PASS", "")
	if configPass == "" {
		log.Fatal("CONFIG_PASS environment variable required (only config DB credentials needed, source/dest are in flows table)")
	}

	connStr := fmt.Sprintf("server=%s;user id=%s;password=%s;database=dbaops_dataflow;encrypt=disable",
		configServer, configUser, configPass)
	
	var err error
	configDB, err = sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to config database: %v", err)
	}

	if err := configDB.Ping(); err != nil {
		log.Fatalf("Failed to ping config database: %v", err)
	}

	configDB.SetMaxOpenConns(50)
	configDB.SetMaxIdleConns(10)
	configDB.SetConnMaxLifetime(5 * time.Minute)

	log.Println("Connected to configuration database")
	loadConfiguration()
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
	log.Println("DBAOps DataFlow starting...")

	wg.Add(1)
	go configReloader()

	wg.Add(1)
	go syncLoop()

	wg.Add(1)
	go verificationLoop()

	select {
	case <-stopChan:
		log.Println("Stopping DBAOps DataFlow...")
	}

	close(stopChan)
	wg.Wait()
	
	configDB.Close()
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

	configMu.RLock()
	interval := cdcInterval
	configMu.RUnlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			processAllMappings()
			
			configMu.RLock()
			newInterval := cdcInterval
			configMu.RUnlock()
			if newInterval != interval {
				ticker.Stop()
				ticker = time.NewTicker(newInterval)
				interval = newInterval
			}
		}
	}
}

func verificationLoop() {
	defer wg.Done()

	configMu.RLock()
	interval := verificationInterval
	configMu.RUnlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			performAllVerifications()
			
			configMu.RLock()
			newInterval := verificationInterval
			configMu.RUnlock()
			if newInterval != interval {
				ticker.Stop()
				ticker = time.NewTicker(newInterval)
				interval = newInterval
			}
		}
	}
}

func processAllMappings() {
	mappings, err := getEnabledMappings()
	if err != nil {
		log.Printf("Error getting mappings: %v", err)
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
				processMapping(mapping)
			}
		}()
	}

	for _, mapping := range mappings {
		jobs <- mapping
	}
	close(jobs)

	wgWorkers.Wait()
}

func processMapping(mapping TableMapping) {
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
	statusID := startSyncStatus(mapping.MappingID, "FULL_SYNC", "RUNNING")
	if statusID == 0 {
		return
	}

	logSync(mapping.MappingID, "INFO", "Full sync started", "FULL_SYNC", 0, 0)

	if !validateSchema(mapping, sourceDB) {
		updateSyncStatus(statusID, "ERROR", 0, 0, "Schema validation failed")
		logError(&mapping.MappingID, &mapping.FlowID, "SCHEMA", "Schema validation failed", nil)
		return
	}

	logSync(mapping.MappingID, "INFO", "Schema validation passed", "SCHEMA_CHECK", 0, 0)

	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		updateSyncStatus(statusID, "ERROR", 0, 0, err.Error())
		logError(&mapping.MappingID, &mapping.FlowID, "CONNECTION",
			fmt.Sprintf("Failed to connect to destination server: %v", err), nil)
		return
	}
	defer destDB.Close()

	_, err = destDB.Exec(fmt.Sprintf("USE [%s]", mapping.DestDatabase))
	if err != nil {
		updateSyncStatus(statusID, "ERROR", 0, 0, err.Error())
		return
	}

	startTime := time.Now()
	recordsProcessed := int64(0)
	recordsFailed := int64(0)

	timestampColumnsDest, err := getTimestampColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	if err != nil {
		logSync(mapping.MappingID, "WARNING", fmt.Sprintf("Failed to get timestamp columns from dest: %v", err), "FULL_SYNC", 0, 0)
	}

	timestampColumnsSource, err := getTimestampColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	if err != nil {
		logSync(mapping.MappingID, "WARNING", fmt.Sprintf("Failed to get timestamp columns from source: %v", err), "FULL_SYNC", 0, 0)
	}

	identityColumns, err := getIdentityColumns(destDB, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	if err != nil {
		logSync(mapping.MappingID, "WARNING", fmt.Sprintf("Failed to get identity columns: %v", err), "FULL_SYNC", 0, 0)
	}

	excludeMap := make(map[string]bool)
	for _, col := range timestampColumnsDest {
		excludeMap[strings.ToLower(col)] = true
	}
	for _, col := range timestampColumnsSource {
		excludeMap[strings.ToLower(col)] = true
	}
	for _, col := range identityColumns {
		excludeMap[strings.ToLower(col)] = true
	}

	sourceCols, err := getTableColumns(sourceDB, mapping.SourceDatabase, mapping.SourceSchema, mapping.SourceTable)
	if err != nil {
		updateSyncStatus(statusID, "ERROR", recordsProcessed, recordsFailed, err.Error())
		logError(&mapping.MappingID, &mapping.FlowID, "SYNC",
			fmt.Sprintf("Failed to get source columns: %v", err), nil)
		return
	}

	columns := make([]string, 0)
	for _, col := range sourceCols {
		if !excludeMap[strings.ToLower(col)] {
			columns = append(columns, col)
		}
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

	rows, err := sourceDB.Query(selectQuery)
	if err != nil {
		updateSyncStatus(statusID, "ERROR", recordsProcessed, recordsFailed, err.Error())
		logError(&mapping.MappingID, &mapping.FlowID, "SYNC",
			fmt.Sprintf("Failed to query source: %v", err), nil)
		return
	}
	defer rows.Close()

	truncateQuery := fmt.Sprintf("TRUNCATE TABLE [%s].[%s].[%s]",
		mapping.DestDatabase, mapping.DestSchema, mapping.DestTable)
	_, err = destDB.Exec(truncateQuery)
	if err != nil {
		logSync(mapping.MappingID, "WARNING",
			fmt.Sprintf("Failed to truncate dest table (may not exist): %v", err), "FULL_SYNC", 0, 0)
	}

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

	configMu.RLock()
	batchSizeLocal := batchSize
	configMu.RUnlock()

	batch := make([][]interface{}, 0, batchSizeLocal)
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

		for i, col := range columns {
			if strings.EqualFold(col, mapping.PrimaryKeyColumn) {
				lastPKValue = values[i]
				break
			}
		}

		batch = append(batch, values)
		recordsProcessed++

		if len(batch) >= batchSizeLocal {
			if err := executeBatchInsert(destDB, insertQuery, columns, batch); err != nil {
				recordsFailed += int64(len(batch))
				logSync(mapping.MappingID, "ERROR",
					fmt.Sprintf("Batch insert failed: %v", err), "FULL_SYNC", int64(len(batch)), 0)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := executeBatchInsert(destDB, insertQuery, columns, batch); err != nil {
			recordsFailed += int64(len(batch))
			logSync(mapping.MappingID, "ERROR",
				fmt.Sprintf("Final batch insert failed: %v", err), "FULL_SYNC", int64(len(batch)), 0)
		}
	}

	configDB.Exec("UPDATE table_mappings SET is_full_sync = 0 WHERE mapping_id = @p1", mapping.MappingID)

	updateLastFullSync(mapping.MappingID)

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

func processCDC(mapping TableMapping, sourceDB *sql.DB) {
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

func executeBatchInsert(db *sql.DB, query string, columns []string, batch [][]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, row := range batch {
		args := make([]interface{}, len(row))
		for i, val := range row {
			if val == nil {
				args[i] = nil
			} else {
				switch v := val.(type) {
				case []byte:
					args[i] = v
				default:
					args[i] = val
				}
			}
		}
		if _, err := tx.Exec(query, args...); err != nil {
			return fmt.Errorf("row insert failed: %w", err)
		}
	}

	return tx.Commit()
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
	destDB, err := connectToServer(mapping.DestConnString)
	if err != nil {
		return
	}
	defer destDB.Close()

	_, err = destDB.Exec(fmt.Sprintf("USE [%s]", mapping.DestDatabase))
	if err != nil {
		return
	}

	query := fmt.Sprintf(`
		SELECT TOP 10000 *
		FROM [%s].[%s].[%s]
		ORDER BY %s DESC
	`, mapping.DestDatabase, mapping.DestSchema, mapping.DestTable, mapping.PrimaryKeyColumn)

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

	pkValues := make([]interface{}, 0)
	for _, row := range destData {
		if pkVal, ok := row[mapping.PrimaryKeyColumn]; ok {
			pkValues = append(pkValues, pkVal)
		}
	}

	if len(pkValues) == 0 {
		return
	}

	pkPlaceholders := make([]string, 0)
	pkArgs := make([]interface{}, 0)
	for i, pkVal := range pkValues {
		if i >= 1000 {
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
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}

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

func updateLastProcessedPK(statusID int64, pkValue string) {
	configDB.Exec("UPDATE sync_status SET last_processed_pk = @p1 WHERE status_id = @p2", pkValue, statusID)
}

func updateLastLSN(mappingID int, lsn string) {
	configDB.Exec("UPDATE table_mappings SET last_cdc_lsn = @p1 WHERE mapping_id = @p2", lsn, mappingID)
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
