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
- SQL Server (destination server)
- SQL Server with CDC enabled (source servers)
- Network access between destination and source servers

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

Run the schema file on the destination SQL Server:

```bash
sqlcmd -S DEST_SERVER -U sa -P PASSWORD -i schema-dataflow.sql
```

Or using SQL Server Management Studio:
1. Open `schema-dataflow.sql`
2. Connect to your destination server
3. Execute the script

This will create:
- `dbaops_dataflow` database
- All required tables (source_servers, table_mappings, sync_status, etc.)
- Helper views for monitoring

### Step 4: Configure Environment Variables

Set the following environment variables:

```bash
export DEST_SERVER=localhost
export DEST_USER=sa
export DEST_PASS=your-password
```

For Windows (PowerShell):
```powershell
$env:DEST_SERVER="localhost"
$env:DEST_USER="sa"
$env:DEST_PASS="your-password"
```

For Windows (CMD):
```cmd
set DEST_SERVER=localhost
set DEST_USER=sa
set DEST_PASS=your-password
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

### Add Source Server

Connect to the destination database and insert a source server:

```sql
USE dbaops_dataflow;
GO

INSERT INTO source_servers (server_name, connection_string, is_enabled)
VALUES (
    'SourceServer1',
    'server=source1.example.com;user id=sa;password=pass;database=master;encrypt=disable',
    1
);
```

### Add Table Mapping

Create a mapping between source and destination tables:

```sql
INSERT INTO table_mappings (
    server_id,
    source_database, source_schema, source_table,
    dest_database, dest_schema, dest_table,
    primary_key_column,
    full_sync_trigger_column,
    is_enabled,
    cdc_enabled
)
VALUES (
    1,
    'SourceDB', 'dbo', 'Users',
    'DestDB', 'dbo', 'Users',
    'UserID',
    'FullSyncTrigger',
    1,
    1
);
```

**Parameters:**
- `server_id`: ID from `source_servers` table
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
-- Stop entire server
UPDATE source_servers SET is_enabled = 0 WHERE server_id = 1;

-- Stop specific table
UPDATE table_mappings SET is_enabled = 0 WHERE mapping_id = 1;

-- Re-enable
UPDATE source_servers SET is_enabled = 1 WHERE server_id = 1;
UPDATE table_mappings SET is_enabled = 1 WHERE mapping_id = 1;
```

## Database Tables

### source_servers
Stores source server connection information.

### table_mappings
Defines source-destination table mappings and sync configuration.

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

- **Batch Size**: 1,000 records per batch
- **CDC Check Interval**: 10 seconds (default)
- **Verification Interval**: 5 minutes (default)
- **Connection Pooling**: Maximum 50 open connections
- **Concurrent Processing**: Multiple tables sync in parallel

## Configuration

You can modify intervals by changing constants in `main.go`:

```go
const (
    checkInterval        = 10 * time.Second
    verificationInterval = 5 * time.Minute
    maxRetryAttempts    = 3
    retryDelay          = 30 * time.Second
)
```

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
   - Add source server to `source_servers` table
   - Add table mapping to `table_mappings` table

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
- Verify network connectivity between destination and source servers
- Check connection strings in `source_servers` table
- Verify SQL Server authentication credentials
- Check firewall rules

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
