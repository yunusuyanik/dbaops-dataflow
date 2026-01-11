# DBAOps DataFlow

Production-grade, minimal CDC (Change Data Capture) and Full Sync service. Transfers data from multiple source servers to destination server.

## Features

✅ **Multi-Source Support**: Can receive data from multiple source servers  
✅ **CDC (Change Data Capture)**: Incremental sync via SQL Server CDC  
✅ **Full Sync**: Trigger full sync by setting trigger column to 1  
✅ **Schema Validation**: Automatic schema check before full sync  
✅ **MD5 Verification**: Compare last 10k records with source using MD5  
✅ **Error Resilience**: If one table fails, others continue  
✅ **Comprehensive Logging**: Detailed logs for every stage  
✅ **Server/Table Control**: Stop at server or table level  
✅ **Read-Only Source**: No write/delete operations on source  

## Prerequisites

- Go 1.21 or higher
- SQL Server (for config database - can be any server)
- SQL Server with CDC enabled (source servers)
- Network access between config, source, and destination servers

## Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/yunusuyanik/dbaops-dataflow.git
cd dbaops-dataflow
```

### Step 2: Install Dependencies

```bash
go mod download
```

### Step 3: Setup Database Schema

Run the schema file on **any SQL Server** (can be source, destination, or a separate config server):

```bash
sqlcmd -S CONFIG_SERVER -U sa -P PASSWORD -i schema-dataflow.sql
```

Or using SQL Server Management Studio:
1. Open `schema-dataflow.sql`
2. Connect to your server (any server works)
3. Execute the script

This will create:
- `dbaops_dataflow` database
- `config` table (for intervals and parallel count)
- `server_connections` table (for source and destination servers)
- `table_mappings` table (with source and dest server references)
- All required tables (sync_status, sync_logs, error_logs, etc.)
- Helper views for monitoring

### Step 4: Configure Environment Variables

Set the following environment variables for the **config database** connection:

```bash
export CONFIG_SERVER=localhost
export CONFIG_USER=sa
export CONFIG_PASS=your-password
```

For Windows (PowerShell):
```powershell
$env:CONFIG_SERVER="localhost"
$env:CONFIG_USER="sa"
$env:CONFIG_PASS="your-password"
```

For Windows (CMD):
```cmd
set CONFIG_SERVER=localhost
set CONFIG_USER=sa
set CONFIG_PASS=your-password
```

### Step 5: Build the Application

```bash
go build -o dbaops-dataflow
```

This will create the `dbaops-dataflow` executable in the current directory.

### Step 6: Run the Service

```bash
./dbaops-dataflow
```

For Windows:
```cmd
dbaops-dataflow.exe
```

The service will start and begin checking for sync operations every 10 seconds.

## Usage

### Configure Settings

Update configuration values in the `config` table:

```sql
USE dbaops_dataflow;
GO

-- Update CDC check interval (in seconds)
UPDATE config SET config_value = '10' WHERE config_key = 'cdc_check_interval_seconds';

-- Update verification interval (in minutes)
UPDATE config SET config_value = '5' WHERE config_key = 'verification_interval_minutes';

-- Update parallel scheduler count (number of concurrent workers)
UPDATE config SET config_value = '5' WHERE config_key = 'parallel_scheduler_count';

-- Update batch size
UPDATE config SET config_value = '1000' WHERE config_key = 'batch_size';
```

### Add Server Connections

Add both source and destination servers:

```sql
USE dbaops_dataflow;
GO

-- Add source server
INSERT INTO server_connections (server_name, server_type, connection_string, is_enabled)
VALUES (
    'SourceServer1',
    'SOURCE',
    'server=source1.example.com;user id=sa;password=pass;database=master;encrypt=disable',
    1
);

-- Add destination server
INSERT INTO server_connections (server_name, server_type, connection_string, is_enabled)
VALUES (
    'DestServer1',
    'DEST',
    'server=dest1.example.com;user id=sa;password=pass;database=master;encrypt=disable',
    1
);
```

### Add Table Mapping

Create a mapping between source and destination tables (both servers must exist):

```sql
INSERT INTO table_mappings (
    source_server_id,
    source_database, source_schema, source_table,
    dest_server_id,
    dest_database, dest_schema, dest_table,
    primary_key_column,
    full_sync_trigger_column,
    is_enabled,
    cdc_enabled
)
VALUES (
    1,  -- source_server_id (from server_connections)
    'SourceDB', 'dbo', 'Users',
    2,  -- dest_server_id (from server_connections)
    'DestDB', 'dbo', 'Users',
    'UserID',
    'FullSyncTrigger',
    1,
    1
);
```

**Parameters:**
- `source_server_id`: ID from `server_connections` table (type='SOURCE')
- `dest_server_id`: ID from `server_connections` table (type='DEST')
- `source_database/schema/table`: Source table location
- `dest_database/schema/table`: Destination table location
- `primary_key_column`: Primary key column name
- `full_sync_trigger_column`: Column name that triggers full sync when set to 1 (can be NULL)
- `is_enabled`: 1 to enable, 0 to disable
- `cdc_enabled`: 1 if CDC is enabled on source table

### Enable CDC on Source Server

Before using CDC, enable it on the source database and table:

```sql
-- Connect to source server
USE SourceDB;
GO

-- Enable CDC at database level
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC at table level
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Users',
    @role_name = N'cdc_admin',
    @capture_instance = N'dbo_Users';
GO
```

### Trigger Full Sync

To perform a full sync, set the trigger column to 1 in the source table:

```sql
-- On source server
UPDATE [SourceDB].[dbo].[Users]
SET FullSyncTrigger = 1
WHERE UserID = 1;  -- or for entire table: WHERE 1=1
```

The service will automatically:
1. Perform schema validation
2. Truncate destination table
3. Fetch all data from source
4. Insert into destination
5. Reset trigger column to 0
6. Continue with CDC

### Stop Server/Table Sync

To temporarily stop syncing:

```sql
-- Stop server connection
UPDATE server_connections SET is_enabled = 0 WHERE server_id = 1;

-- Stop specific table mapping
UPDATE table_mappings SET is_enabled = 0 WHERE mapping_id = 1;

-- Re-enable
UPDATE server_connections SET is_enabled = 1 WHERE server_id = 1;
UPDATE table_mappings SET is_enabled = 1 WHERE mapping_id = 1;
```

## Database Tables

### config
Stores global configuration (intervals, parallel count, batch size, etc.)

### server_connections
Stores connection information for all servers (both SOURCE and DEST types)

### table_mappings
Defines source-destination table mappings with references to server_connections

### sync_status
Tracks the status of each sync operation (CDC or FULL_SYNC).

### sync_logs
Detailed log records for all operations (INFO, WARNING, ERROR).

### error_logs
Error logs. If one table fails, others continue syncing.

### verification_logs
MD5 comparison results from verification process.

## Monitoring

### View Active Syncs

```sql
SELECT * FROM v_sync_summary;
```

### View Recent Errors

```sql
SELECT * FROM v_recent_errors;
```

### View Verification Results

```sql
SELECT * FROM verification_logs
WHERE verification_status = 'FAILED'
ORDER BY verified_at DESC;
```

### Check Sync Status

```sql
SELECT 
    tm.source_table,
    tm.dest_table,
    ss.status,
    ss.sync_type,
    ss.records_processed,
    ss.records_failed,
    ss.started_at,
    ss.completed_at
FROM sync_status ss
INNER JOIN table_mappings tm ON ss.mapping_id = tm.mapping_id
WHERE ss.status IN ('RUNNING', 'ERROR')
ORDER BY ss.started_at DESC;
```

## MD5 Verification

The service automatically performs MD5 verification every 5 minutes (default):

1. Gets last 10,000 records from destination table (ORDER BY PK DESC)
2. Fetches same records from source
3. Calculates MD5 hash for each record
4. Compares hashes and writes results to `verification_logs` table
5. Logs mismatches to `error_logs` table

## Error Management

- **Isolated Failures**: If one table fails, sync continues for other tables
- **Error Logging**: All errors are logged to `error_logs` table
- **Automatic Retry**: Service retries failed operations in the next interval
- **Retry Limits**: Maximum 3 attempts with 30 second delay

## Security

⚠️ **IMPORTANT**: 
- Only READ operations are performed on source server
- Only the full sync trigger column can be updated (and this is optional)
- Source server credentials are stored in `source_servers` table - secure this database appropriately
- Use encrypted connections when possible

## Performance

- **Batch Size**: Configurable via `config` table (default: 1,000)
- **CDC Check Interval**: Configurable via `config` table (default: 10 seconds)
- **Verification Interval**: Configurable via `config` table (default: 5 minutes)
- **Parallel Workers**: Configurable via `config` table (default: 5 concurrent workers)
- **Connection Pooling**: Maximum 50 open connections
- **Dynamic Configuration**: Config reloads every minute without restart

## Configuration

All configuration is stored in the `config` table and reloaded every minute:

```sql
-- View current configuration
SELECT * FROM config;

-- Update CDC check interval (seconds)
UPDATE config SET config_value = '15' WHERE config_key = 'cdc_check_interval_seconds';

-- Update parallel worker count
UPDATE config SET config_value = '10' WHERE config_key = 'parallel_scheduler_count';

-- Update verification interval (minutes)
UPDATE config SET config_value = '10' WHERE config_key = 'verification_interval_minutes';
```

Changes take effect within 1 minute without restarting the service.

## Logging

All logs are stored in destination database tables:
- `sync_logs`: Detailed operation logs with timestamps
- `error_logs`: Error logs with retry counts
- `verification_logs`: MD5 verification results

Logs are also printed to stdout for real-time monitoring.

## Example Scenario

1. **Initial Setup**: 
   - Create source and destination tables with matching schemas
   - Run schema setup on destination server

2. **Enable CDC**: 
   - Enable CDC on source database and table

3. **Configure Mapping**: 
   - Add source and destination servers to `server_connections` table
   - Add table mapping to `table_mappings` table (with source_server_id and dest_server_id)

4. **First Full Sync**: 
   - Set trigger column to 1 in source table
   - Service performs full sync automatically

5. **CDC Continues**: 
   - Service automatically continues with CDC after full sync
   - Changes are synced incrementally

6. **Verification**: 
   - MD5 verification runs every 5 minutes
   - Mismatches are logged for investigation

## Troubleshooting

### CDC not working
- Verify CDC is enabled on source database: `SELECT is_cdc_enabled FROM sys.databases WHERE name = 'YourDB'`
- Check if CDC is enabled on table: `SELECT * FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('YourTable')`
- Ensure `cdc_enabled = 1` in `table_mappings` table
- Check error messages in `sync_logs` table

### Full sync not triggered
- Verify trigger column is set to 1: `SELECT FullSyncTrigger FROM SourceTable WHERE FullSyncTrigger = 1`
- Check if `full_sync_trigger_column` is correct in `table_mappings` table
- Check status in `sync_status` table for error messages
- Ensure `is_enabled = 1` for the mapping

### Schema validation failed
- Verify source and destination table columns match
- Check column names are case-sensitive
- See detailed error message in `sync_logs` table
- Ensure primary key column exists in both tables

### Connection errors
- Verify network connectivity between config, source, and destination servers
- Check connection strings in `server_connections` table
- Verify SQL Server authentication credentials
- Check firewall rules
- Ensure both source and dest servers are enabled in `server_connections`

### Performance issues
- Monitor `sync_status` table for slow operations
- Check `sync_logs` for execution times
- Consider increasing batch size for large tables
- Verify network latency between servers

## Building from Source

### Requirements
- Go 1.21 or higher
- Git

### Build Steps

```bash
# Clone repository
git clone https://github.com/yunusuyanik/dbaops-dataflow.git
cd dbaops-dataflow

# Download dependencies
go mod download

# Build
go build -o dbaops-dataflow

# Run
./dbaops-dataflow
```

### Cross-Platform Build

```bash
# Linux
GOOS=linux GOARCH=amd64 go build -o dbaops-dataflow-linux

# Windows
GOOS=windows GOARCH=amd64 go build -o dbaops-dataflow.exe

# macOS
GOOS=darwin GOARCH=amd64 go build -o dbaops-dataflow-macos
```

## License

GPL-3.0

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please open an issue on GitHub.
