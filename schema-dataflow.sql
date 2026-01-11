-- ============================================
-- DBAOps DataFlow Database Schema
-- ============================================
-- This schema can be installed on any SQL Server
-- Supports multiple source and destination servers

USE master;
GO

-- Create database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'dbaops_dataflow')
BEGIN
    CREATE DATABASE dbaops_dataflow;
END
GO

USE dbaops_dataflow;
GO

-- ============================================
-- CONFIGURATION TABLE
-- ============================================
-- Global configuration settings
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'config')
BEGIN
    CREATE TABLE config (
        config_key NVARCHAR(100) PRIMARY KEY,
        config_value NVARCHAR(500) NOT NULL,
        description NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
    
    -- Default configuration values
    INSERT INTO config (config_key, config_value, description) VALUES
    ('cdc_check_interval_seconds', '10', 'Interval in seconds for checking CDC changes'),
    ('verification_interval_minutes', '5', 'Interval in minutes for MD5 verification'),
    ('parallel_scheduler_count', '5', 'Number of parallel goroutines for processing tables'),
    ('batch_size', '1000', 'Number of records per batch insert'),
    ('max_retry_attempts', '3', 'Maximum retry attempts for failed operations'),
    ('retry_delay_seconds', '30', 'Delay in seconds between retry attempts');
END
GO

-- ============================================
-- SERVER CONNECTIONS TABLE
-- ============================================
-- Stores connection information for all servers (source and destination)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'server_connections')
BEGIN
    CREATE TABLE server_connections (
        server_id INT IDENTITY(1,1) PRIMARY KEY,
        server_name NVARCHAR(255) NOT NULL UNIQUE,
        server_type NVARCHAR(50) NOT NULL, -- 'SOURCE' or 'DEST'
        connection_string NVARCHAR(1000) NOT NULL,
        is_enabled BIT NOT NULL DEFAULT 1,
        created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        last_connected_at DATETIME2 NULL,
        notes NVARCHAR(MAX) NULL,
        CHECK (server_type IN ('SOURCE', 'DEST'))
    );
    
    CREATE INDEX IX_server_connections_type ON server_connections(server_type, is_enabled);
    CREATE INDEX IX_server_connections_enabled ON server_connections(is_enabled);
END
GO

-- ============================================
-- TABLE MAPPINGS TABLE
-- ============================================
-- Source-destination table mappings
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'table_mappings')
BEGIN
    CREATE TABLE table_mappings (
        mapping_id INT IDENTITY(1,1) PRIMARY KEY,
        source_server_id INT NOT NULL,
        source_database NVARCHAR(255) NOT NULL,
        source_schema NVARCHAR(255) NOT NULL DEFAULT 'dbo',
        source_table NVARCHAR(255) NOT NULL,
        dest_server_id INT NOT NULL,
        dest_database NVARCHAR(255) NOT NULL,
        dest_schema NVARCHAR(255) NOT NULL DEFAULT 'dbo',
        dest_table NVARCHAR(255) NOT NULL,
        primary_key_column NVARCHAR(255) NOT NULL,
        full_sync_trigger_column NVARCHAR(255) NULL, -- Full sync is performed when this column is set to 1
        is_enabled BIT NOT NULL DEFAULT 1,
        cdc_enabled BIT NOT NULL DEFAULT 0,
        last_cdc_lsn BINARY(10) NULL,
        last_full_sync_at DATETIME2 NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        FOREIGN KEY (source_server_id) REFERENCES server_connections(server_id) ON DELETE CASCADE,
        FOREIGN KEY (dest_server_id) REFERENCES server_connections(server_id) ON DELETE CASCADE,
        UNIQUE(source_server_id, source_database, source_schema, source_table, dest_server_id, dest_database, dest_schema, dest_table)
    );
    
    CREATE INDEX IX_table_mappings_source_server ON table_mappings(source_server_id, is_enabled);
    CREATE INDEX IX_table_mappings_dest_server ON table_mappings(dest_server_id, is_enabled);
    CREATE INDEX IX_table_mappings_enabled ON table_mappings(is_enabled);
END
GO

-- ============================================
-- SYNC STATUS TABLE
-- ============================================
-- Sync status for each table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sync_status')
BEGIN
    CREATE TABLE sync_status (
        status_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        mapping_id INT NOT NULL,
        sync_type NVARCHAR(50) NOT NULL, -- 'CDC' or 'FULL_SYNC'
        status NVARCHAR(50) NOT NULL, -- 'RUNNING', 'COMPLETED', 'ERROR', 'PAUSED'
        records_processed BIGINT NOT NULL DEFAULT 0,
        records_failed BIGINT NOT NULL DEFAULT 0,
        started_at DATETIME2 NOT NULL,
        completed_at DATETIME2 NULL,
        error_message NVARCHAR(MAX) NULL,
        last_processed_pk NVARCHAR(500) NULL, -- Last processed primary key value
        FOREIGN KEY (mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE CASCADE
    );
    
    CREATE INDEX IX_sync_status_mapping ON sync_status(mapping_id, started_at DESC);
    CREATE INDEX IX_sync_status_type ON sync_status(sync_type, status);
END
GO

-- ============================================
-- SYNC LOGS TABLE
-- ============================================
-- Detailed log for each stage
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sync_logs')
BEGIN
    CREATE TABLE sync_logs (
        log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        mapping_id INT NOT NULL,
        log_level NVARCHAR(20) NOT NULL, -- 'INFO', 'WARNING', 'ERROR'
        log_message NVARCHAR(MAX) NOT NULL,
        sync_type NVARCHAR(50) NULL, -- 'CDC', 'FULL_SYNC', 'VERIFICATION', 'SCHEMA_CHECK'
        records_count BIGINT NULL,
        execution_time_ms INT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        FOREIGN KEY (mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE CASCADE
    );
    
    CREATE INDEX IX_sync_logs_mapping ON sync_logs(mapping_id, created_at DESC);
    CREATE INDEX IX_sync_logs_level ON sync_logs(log_level, created_at DESC);
END
GO

-- ============================================
-- ERROR LOGS TABLE
-- ============================================
-- Error logs (if one table fails, others continue)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'error_logs')
BEGIN
    CREATE TABLE error_logs (
        error_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        mapping_id INT NULL, -- Can be NULL (server-level error)
        server_id INT NULL,
        error_type NVARCHAR(50) NOT NULL, -- 'CONNECTION', 'SYNC', 'SCHEMA', 'VERIFICATION', 'OTHER'
        error_message NVARCHAR(MAX) NOT NULL,
        error_details NVARCHAR(MAX) NULL,
        stack_trace NVARCHAR(MAX) NULL,
        retry_count INT NOT NULL DEFAULT 0,
        is_resolved BIT NOT NULL DEFAULT 0,
        created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        resolved_at DATETIME2 NULL,
        FOREIGN KEY (mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE SET NULL,
        FOREIGN KEY (server_id) REFERENCES server_connections(server_id) ON DELETE SET NULL
    );
    
    CREATE INDEX IX_error_logs_mapping ON error_logs(mapping_id, created_at DESC);
    CREATE INDEX IX_error_logs_server ON error_logs(server_id, created_at DESC);
    CREATE INDEX IX_error_logs_resolved ON error_logs(is_resolved, created_at DESC);
END
GO

-- ============================================
-- VERIFICATION LOGS TABLE
-- ============================================
-- MD5 comparison results
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'verification_logs')
BEGIN
    CREATE TABLE verification_logs (
        verification_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        mapping_id INT NOT NULL,
        verification_type NVARCHAR(50) NOT NULL, -- 'MD5_COMPARISON', 'ROW_COUNT', 'SCHEMA'
        source_md5 NVARCHAR(32) NULL,
        dest_md5 NVARCHAR(32) NULL,
        source_row_count BIGINT NULL,
        dest_row_count BIGINT NULL,
        records_compared BIGINT NOT NULL DEFAULT 0,
        mismatches_found BIGINT NOT NULL DEFAULT 0,
        verification_status NVARCHAR(50) NOT NULL, -- 'PASSED', 'FAILED', 'PARTIAL'
        verification_details NVARCHAR(MAX) NULL,
        verified_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        FOREIGN KEY (mapping_id) REFERENCES table_mappings(mapping_id) ON DELETE CASCADE
    );
    
    CREATE INDEX IX_verification_logs_mapping ON verification_logs(mapping_id, verified_at DESC);
    CREATE INDEX IX_verification_logs_status ON verification_logs(verification_status, verified_at DESC);
END
GO

-- ============================================
-- HELPER VIEWS
-- ============================================

-- Summary of active syncs
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_sync_summary')
    DROP VIEW v_sync_summary;
GO

CREATE VIEW v_sync_summary AS
SELECT 
    sc_source.server_name AS source_server,
    sc_dest.server_name AS dest_server,
    tm.source_database,
    tm.source_table,
    tm.dest_table,
    ss.status,
    ss.sync_type,
    ss.records_processed,
    ss.records_failed,
    ss.started_at,
    ss.completed_at,
    DATEDIFF(SECOND, ss.started_at, ISNULL(ss.completed_at, GETUTCDATE())) AS duration_seconds
FROM sync_status ss
INNER JOIN table_mappings tm ON ss.mapping_id = tm.mapping_id
INNER JOIN server_connections sc_source ON tm.source_server_id = sc_source.server_id
INNER JOIN server_connections sc_dest ON tm.dest_server_id = sc_dest.server_id
WHERE ss.status IN ('RUNNING', 'ERROR')
GO

-- Recent errors
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_recent_errors')
    DROP VIEW v_recent_errors;
GO

CREATE VIEW v_recent_errors AS
SELECT TOP 100
    el.error_id,
    sc.server_name,
    tm.source_table,
    tm.dest_table,
    el.error_type,
    el.error_message,
    el.retry_count,
    el.is_resolved,
    el.created_at
FROM error_logs el
LEFT JOIN server_connections sc ON el.server_id = sc.server_id
LEFT JOIN table_mappings tm ON el.mapping_id = tm.mapping_id
WHERE el.is_resolved = 0
ORDER BY el.created_at DESC
GO

PRINT 'DBAOps DataFlow schema created successfully!';
GO
