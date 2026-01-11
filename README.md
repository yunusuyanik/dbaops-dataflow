# DBAOps DataFlow

Minimal CDC (Change Data Capture) and Full Sync service for SQL Server. Transfers data from multiple source SQL Server instances to a destination SQL Server instance.

## Features

- Multi-source support
- CDC incremental sync (SQL Server CDC required)
- Full sync on demand
- Schema validation
- MD5 verification (last 10k records)
- Error resilience (one table failure doesn't stop others)
- Comprehensive logging

## Prerequisites

- Go 1.21+
- SQL Server (config, source, destination)
- CDC enabled on source databases
- Network access between servers

## Installation

```bash
git clone https://github.com/yunusuyanik/dbaops-dataflow.git
cd dbaops-dataflow
go mod download
go build -o dbaops-dataflow
```

## Setup

### 1. Create Schema

Run `schema-dataflow.sql` on any SQL Server instance:

```bash
sqlcmd -S SERVER -U sa -P PASSWORD -i schema-dataflow.sql
```

### 2. Configure Environment

```bash
export CONFIG_SERVER=localhost
export CONFIG_USER=sa
export CONFIG_PASS=your-password
```

### 3. Run Service

```bash
./dbaops-dataflow
```

## Usage

### Add Flow

```sql
USE dbaops_dataflow;
GO

INSERT INTO flows (
    flow_name, source_server, source_port, source_user, source_password,
    dest_server, dest_port, dest_user, dest_password, is_enabled
)
VALUES (
    'ProdToWarehouse',
    'source-server.com', 1433, 'sa', 'password',
    'dest-server.com', 1433, 'sa', 'password',
    1
);
```

### Add Table Mapping

```sql
INSERT INTO table_mappings (
    flow_id, source_database, source_schema, source_table,
    dest_database, dest_schema, dest_table,
    primary_key_column, is_enabled, cdc_enabled
)
VALUES (
    1, 'SourceDB', 'dbo', 'Users',
    'DestDB', 'dbo', 'Users',
    'UserID', 1, 1
);
```

### Enable CDC on Source

```sql
USE SourceDB;
GO
EXEC sys.sp_cdc_enable_db;
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Users',
    @role_name = NULL;
GO
```

### Trigger Full Sync

```sql
UPDATE table_mappings SET is_full_sync = 1 WHERE mapping_id = 1;
```

Service automatically:
1. Validates schema
2. Truncates destination
3. Syncs all data
4. Sets `is_full_sync = 0`
5. Continues with CDC

### Stop Sync

```sql
-- Stop flow
UPDATE flows SET is_enabled = 0 WHERE flow_id = 1;

-- Stop table
UPDATE table_mappings SET is_enabled = 0 WHERE mapping_id = 1;
```

## Configuration

Update `config` table:

```sql
UPDATE config SET config_value = '10' WHERE config_key = 'cdc_check_interval_seconds';
UPDATE config SET config_value = '5' WHERE config_key = 'parallel_scheduler_count';
UPDATE config SET config_value = '1000' WHERE config_key = 'batch_size';
```

## Monitoring

```sql
-- Active syncs
SELECT * FROM v_sync_summary;

-- Recent errors
SELECT * FROM v_recent_errors;

-- Sync logs
SELECT TOP 100 * FROM sync_logs ORDER BY created_at DESC;
```

## Troubleshooting

**CDC not working:**
- Verify CDC enabled: `SELECT is_cdc_enabled FROM sys.databases WHERE name = 'DB'`
- Check table: `SELECT * FROM cdc.change_tables`
- Ensure `cdc_enabled = 1` in `table_mappings`

**Full sync not triggered:**
- Check `is_full_sync = 1` in `table_mappings`
- Check `sync_status` table for errors

**Connection errors:**
- Verify network connectivity
- Check credentials in `flows` table
- Test: `sqlcmd -S server -U user -P password`

## License

GPL-3.0
