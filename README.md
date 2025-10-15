# MCP Server for Databend

[![PyPI - Version](https://img.shields.io/pypi/v/mcp-databend)](https://pypi.org/project/mcp-databend)

An MCP server for Databend database interactions.

## What You Can Do

### Database Operations
- **execute_sql** - Execute SQL queries with timeout protection and safe mode security
- **show_databases** - List all databases
- **show_tables** - List tables in a database (with optional filter)
- **describe_table** - Get table schema information

### Stage Management
- **show_stages** - List all available Databend stages
- **list_stage_files** - List files in a specific stage (supports @stage_name format)
- **create_stage** - Create a new stage with connection support

### Connection Management
- **show_connections** - List all available Databend connections

## Security Features

### MCP Safe Mode (Enabled by Default)

This server includes built-in security protection that blocks potentially dangerous SQL operations:

- **Blocked Operations**: `DROP`, `DELETE`, `TRUNCATE`, `ALTER`, `UPDATE`, `REVOKE`

**Safe Mode Configuration:**
```json
{
  "env": {
    "DATABEND_DSN": "your-connection-string-here",
    "SAFE_MODE": "true"
  }
}
```

To disable safe mode (not recommended for production):
```json
{
  "env": {
    "DATABEND_DSN": "your-connection-string-here",
    "SAFE_MODE": "false"
  }
}
```

## How to Use

### Step 1: Get Databend Connection

**Recommended**: Sign up for [Databend Cloud](https://app.databend.com) (free tier available)

Get your connection string from [Databend documentation](https://docs.databend.com/developer/drivers/#connection-string-dsn).

| Deployment | Connection String Example |
|------------|---------------------------|
| **Databend Cloud** | `databend://user:pwd@host:443/database?warehouse=wh` |
| **Self-hosted** | `databend://user:pwd@localhost:8000/database?sslmode=disable` |

Or use local Databend by setting `LOCAL_MODE=true`, the metadata is stored in `.databend` directory:


### Step 2: Install

```bash
uv tool install mcp-databend
```

### Step 3: Configure Your MCP Client

#### Option A: Claude Code (CLI)

- For Databend server:
```bash
claude mcp add mcp-databend --env DATABEND_DSN='your-connection-string-here' -- uv tool run mcp-databend
```

- For local Databend:
```bash
claude mcp add mcp-databend --env LOCAL_MODE=true -- uv tool run mcp-databend
```

#### Option B: MCP Configuration (JSON)

Add to your MCP client configuration (e.g., Claude Desktop, Windsurf):

```json
{
  "mcpServers": {
    "mcp-databend": {
      "command": "uv",
      "args": ["tool", "run", "mcp-databend"],
      "env": {
        "DATABEND_DSN": "your-connection-string-here",
        "SAFE_MODE": "true"
      }
    }
  }
}
```

#### Supported Clients

- **Claude Code** (CLI)
- **Windsurf** / **Claude Desktop** / **Continue.dev** / **Cursor IDE**

#### Options variables

- `DATABEND_DSN`: Databend connection string
- `LOCAL_MODE`: Set to `true` to use local Databend
- `SAFE_MODE`: Set to `false` to disable safe mode
- `DATABEND_QUERY_TIMEOUT`: Query execution timeout in seconds (default: `300`)
- `DATABEND_MCP_SERVER_TRANSPORT`: Default to `stdio`, set to `http` or `sse` to enable HTTP/SSE transport
- `DATABEND_MCP_BIND_HOST`: Default to `127.0.0.1`, set to bind host for HTTP/SSE transport
- `DATABEND_MCP_BIND_PORT`: Default to `8001`, set to bind port for HTTP/SSE transport

### Step 4: Start Using

Once configured, you can ask your AI assistant to:

**Database Operations:**
- "Show me all databases"
- "List tables in the sales database"
- "Describe the users table structure"
- "Run this SQL query: SELECT * FROM products LIMIT 10"

**Stage Management:**
- "Show me all stages"
- "List files in @my_stage"
- "Create a stage named my_s3_stage with URL s3://my-bucket using connection my_connection"

**Connection Management:**
- "Show all connections"

## Development

```bash
# Clone and setup
git clone https://github.com/databendlabs/mcp-databend
cd mcp-databend
uv sync

# Run locally
uv run python -m mcp_databend.main

# Use modelcontextprotocol/inspector to debug
npx @modelcontextprotocol/inspector -e LOCAL_MODE=1 uv run python -m mcp_databend.main
```
