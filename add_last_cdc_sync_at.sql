-- Add last_cdc_sync_at column to table_mappings
USE dbaops_dataflow;
GO

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('table_mappings') AND name = 'last_cdc_sync_at')
BEGIN
    ALTER TABLE table_mappings
    ADD last_cdc_sync_at DATETIME2 NULL;
    
    PRINT 'Column last_cdc_sync_at added to table_mappings';
END
ELSE
BEGIN
    PRINT 'Column last_cdc_sync_at already exists';
END
GO
