-- ============================================
-- EXAMPLE INSERT STATEMENTS
-- ============================================

USE dbaops_dataflow;
GO

-- 1. Add a Flow (Source and Destination SQL Server connection info)
INSERT INTO flows (
    flow_name,
    source_server,
    source_port,
    source_user,
    source_password,
    dest_server,
    dest_port,
    dest_user,
    dest_password,
    is_enabled,
    notes
)
VALUES (
    'ProductionToWarehouse',
    'prod-sql-01.company.local',
    1433,
    'sa',
    'SourcePassword123!',
    'warehouse-sql-01.company.local',
    1433,
    'sa',
    'DestPassword456!',
    1,
    'Production database to data warehouse sync'
);
GO

-- 2. Add another Flow (multiple source servers example)
INSERT INTO flows (
    flow_name,
    source_server,
    source_port,
    source_user,
    source_password,
    dest_server,
    dest_port,
    dest_user,
    dest_password,
    is_enabled
)
VALUES (
    'SecondaryToWarehouse',
    '192.168.1.100',
    1433,
    'dbuser',
    'AnotherPassword789!',
    'warehouse-sql-01.company.local',
    1433,
    'sa',
    'DestPassword456!',
    1
);
GO

-- 3. Add Table Mapping for Users table
INSERT INTO table_mappings (
    flow_id,
    source_database,
    source_schema,
    source_table,
    dest_database,
    dest_schema,
    dest_table,
    primary_key_column,
    full_sync_trigger_column,
    is_enabled,
    cdc_enabled
)
VALUES (
    1,  -- flow_id from flows table
    'ProductionDB',
    'dbo',
    'Users',
    'WarehouseDB',
    'dbo',
    'Users',
    'UserID',
    'FullSyncTrigger',  -- Optional: column that triggers full sync
    1,
    1   -- CDC enabled
);
GO

-- 4. Add Table Mapping for Orders table (without full sync trigger)
INSERT INTO table_mappings (
    flow_id,
    source_database,
    source_schema,
    source_table,
    dest_database,
    dest_schema,
    dest_table,
    primary_key_column,
    full_sync_trigger_column,
    is_enabled,
    cdc_enabled
)
VALUES (
    1,
    'ProductionDB',
    'dbo',
    'Orders',
    'WarehouseDB',
    'dbo',
    'Orders',
    'OrderID',
    NULL,  -- No full sync trigger column
    1,
    1
);
GO

-- 5. Add Table Mapping from second flow
INSERT INTO table_mappings (
    flow_id,
    source_database,
    source_schema,
    source_table,
    dest_database,
    dest_schema,
    dest_table,
    primary_key_column,
    full_sync_trigger_column,
    is_enabled,
    cdc_enabled
)
VALUES (
    2,  -- flow_id from second flow
    'SecondaryDB',
    'dbo',
    'Products',
    'WarehouseDB',
    'dbo',
    'Products',
    'ProductID',
    'SyncTrigger',
    1,
    1
);
GO

-- ============================================
-- ENABLE CDC ON SOURCE SQL SERVER
-- ============================================
-- Run these on the SOURCE SQL Server instance

-- Enable CDC at database level
USE ProductionDB;
GO
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on Users table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Users',
    @role_name = N'cdc_admin',
    @capture_instance = N'dbo_Users';
GO

-- Enable CDC on Orders table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Orders',
    @role_name = N'cdc_admin',
    @capture_instance = N'dbo_Orders';
GO

-- Verify CDC is enabled
SELECT 
    name AS database_name,
    is_cdc_enabled
FROM sys.databases
WHERE name = 'ProductionDB';
-- Should return is_cdc_enabled = 1

SELECT 
    capture_instance,
    source_schema,
    source_table
FROM cdc.change_tables;
-- Should show dbo_Users and dbo_Orders

-- ============================================
-- TRIGGER FULL SYNC
-- ============================================
-- Run this on the SOURCE SQL Server to trigger full sync

USE ProductionDB;
GO

-- Trigger full sync for Users table
UPDATE [dbo].[Users]
SET FullSyncTrigger = 1
WHERE UserID = 1;  -- or WHERE 1=1 for entire table
GO

-- ============================================
-- MONITORING QUERIES
-- ============================================

USE dbaops_dataflow;
GO

-- View all flows
SELECT * FROM flows;

-- View all table mappings
SELECT 
    tm.mapping_id,
    f.flow_name,
    tm.source_database,
    tm.source_table,
    tm.dest_database,
    tm.dest_table,
    tm.is_enabled,
    tm.cdc_enabled
FROM table_mappings tm
INNER JOIN flows f ON tm.flow_id = f.flow_id;

-- View active syncs
SELECT * FROM v_sync_summary;

-- View recent errors
SELECT * FROM v_recent_errors;

-- View sync logs
SELECT TOP 100
    log_level,
    log_message,
    sync_type,
    created_at
FROM sync_logs
ORDER BY created_at DESC;