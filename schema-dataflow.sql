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
-- FLOWS TABLE
-- ============================================
-- Stores source and destination connection information in a single row
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'flows')
BEGIN
    CREATE TABLE flows (
        flow_id INT IDENTITY(1,1) PRIMARY KEY,
        flow_name NVARCHAR(255) NOT NULL UNIQUE,
        source_server NVARCHAR(255) NOT NULL,
        source_port INT NOT NULL DEFAULT 1433,
        source_user NVARCHAR(255) NOT NULL,
        source_password NVARCHAR(255) NOT NULL,
        dest_server NVARCHAR(255) NOT NULL,
        dest_port INT NOT NULL DEFAULT 1433,
        dest_user NVARCHAR(255) NOT NULL,
        dest_password NVARCHAR(255) NOT NULL,
        is_enabled BIT NOT NULL DEFAULT 1,
        created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        last_connected_at DATETIME2 NULL,
        notes NVARCHAR(MAX) NULL
    );
    
    CREATE INDEX IX_flows_enabled ON flows(is_enabled);
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
        flow_id INT NOT NULL,
        source_database NVARCHAR(255) NOT NULL,
        source_schema NVARCHAR(255) NOT NULL DEFAULT 'dbo',
        source_table NVARCHAR(255) NOT NULL,
        dest_database NVARCHAR(255) NOT NULL,
        dest_schema NVARCHAR(255) NOT NULL DEFAULT 'dbo',
        dest_table NVARCHAR(255) NOT NULL,
        primary_key_column NVARCHAR(255) NOT NULL,
        is_full_sync BIT NOT NULL DEFAULT 0,
        is_enabled BIT NOT NULL DEFAULT 1,
        cdc_enabled BIT NOT NULL DEFAULT 0,
        last_cdc_lsn BINARY(10) NULL,
        last_cdc_sync_at DATETIME2 NULL,
        last_full_sync_at DATETIME2 NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
        updated_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
    
    CREATE INDEX IX_table_mappings_flow ON table_mappings(flow_id, is_enabled);
    CREATE INDEX IX_table_mappings_enabled ON table_mappings(is_enabled);
    CREATE INDEX IX_table_mappings_source ON table_mappings(source_database, source_schema, source_table);
    CREATE INDEX IX_table_mappings_dest ON table_mappings(dest_database, dest_schema, dest_table);
END
GO

-- ============================================
-- LOGS TABLE
-- ============================================
-- Unified log table for all operations
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'logs')
BEGIN
    CREATE TABLE logs (
        log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        flow_id INT NULL,
        mapping_id INT NULL,
        log_type NVARCHAR(50) NOT NULL,
        log_message NVARCHAR(MAX) NOT NULL,
        status NVARCHAR(20) NOT NULL,
        records_count BIGINT NULL,
        duration_ms INT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()
    );
    
    CREATE INDEX IX_logs_flow ON logs(flow_id, created_at DESC);
    CREATE INDEX IX_logs_mapping ON logs(mapping_id, created_at DESC);
    CREATE INDEX IX_logs_type ON logs(log_type, created_at DESC);
    CREATE INDEX IX_logs_status ON logs(status, created_at DESC);
    CREATE INDEX IX_logs_created ON logs(created_at DESC);
END
GO

-- ============================================
-- HELPER VIEWS
-- ============================================

-- Recent logs with flow and table mapping details
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_recent_logs')
    DROP VIEW v_recent_logs;
GO

CREATE VIEW v_recent_logs AS
SELECT TOP 1000
    l.log_id,
    f.flow_name,
    tm.source_database,
    tm.source_schema,
    tm.source_table,
    tm.dest_database,
    tm.dest_schema,
    tm.dest_table,
    l.log_type,
    l.log_message,
    l.status,
    l.records_count,
    l.duration_ms,
    l.created_at
FROM logs l
LEFT JOIN table_mappings tm ON l.mapping_id = tm.mapping_id
LEFT JOIN flows f ON l.flow_id = f.flow_id OR (tm.flow_id IS NOT NULL AND l.flow_id = tm.flow_id)
ORDER BY l.created_at DESC
GO

-- Recent errors
IF EXISTS (SELECT * FROM sys.views WHERE name = 'v_recent_errors')
    DROP VIEW v_recent_errors;
GO

CREATE VIEW v_recent_errors AS
SELECT TOP 100
    l.log_id,
    f.flow_name,
    tm.source_table,
    tm.dest_table,
    l.log_type,
    l.log_message,
    l.created_at
FROM logs l
LEFT JOIN table_mappings tm ON l.mapping_id = tm.mapping_id
LEFT JOIN flows f ON l.flow_id = f.flow_id OR (tm.flow_id IS NOT NULL AND l.flow_id = tm.flow_id)
WHERE l.status = 'ERROR'
ORDER BY l.created_at DESC
GO

PRINT 'DBAOps DataFlow schema created successfully!';
GO
