# MCP Server for Databend

[![PyPI - Version](https://img.shields.io/pypi/v/mcp-databend)](https://pypi.org/project/mcp-databend)

An MCP server for Databend database interactions.

## What You Can Do

- **execute_sql** - Execute SQL queries with timeout protection
- **show_databases** - List all databases
- **show_tables** - List tables in a database (with optional filter)
- **describe_table** - Get table schema information

## How to Use

### Step 1: Get Databend Connection

**Recommended**: Sign up for [Databend Cloud](https://app.databend.com) (free tier available)

Get your connection string from [Databend documentation](https://docs.databend.com/developer/drivers/#connection-string-dsn).

| Deployment | Connection String Example |
|------------|---------------------------|
| **Databend Cloud** | `databend://cloudapp:pass@host:443/database?warehouse=wh` |
| **Self-hosted** | `databend://user:pass@localhost:8000/database?sslmode=disable` |

### Step 2: Install

```bash
uv tool install mcp-databend
```

### Step 3: Configure Your MCP Client

#### MCP Configuration

Add to your MCP client configuration (e.g., Claude Desktop, Windsurf):

```json
{
  "mcpServers": {
    "mcp-databend": {
      "command": "uv",
      "args": ["tool", "run", "mcp-databend"],
      "env": {
        "DATABEND_DSN": "your-connection-string-here"
      }
    }
  }
}
```

#### Supported Clients

- **Windsurf** / **Claude Desktop** / **Continue.dev** / **Cursor IDE**

### Step 4: Start Using

Once configured, you can ask your AI assistant to:
- "Show me all databases"
- "List tables in the sales database"
- "Describe the users table structure"
- "Run this SQL query: SELECT * FROM products LIMIT 10"

## Development

```bash
# Clone and setup
git clone https://github.com/databendlabs/mcp-databend
cd mcp-databend
uv sync

# Run locally
uv run python -m mcp_databend.main
```
