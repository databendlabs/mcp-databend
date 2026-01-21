# MCP Server for Databend

[![PyPI - Version](https://img.shields.io/pypi/v/mcp-databend)](https://pypi.org/project/mcp-databend)

**Connect AI assistants to your Databend database safely.** This MCP server enables Claude, Cursor, and other AI tools to query and analyze your data without risking production changes.

Works with Claude Desktop, Claude Code, Cursor, Windsurf, Codex, Gemini CLI, and any MCP-compatible agent. Learn more at [databend.com/mcp](https://www.databend.com/mcp/).

## What is MCP?

[Model Context Protocol (MCP)](https://modelcontextprotocol.io) lets AI assistants securely access external data sources. This server gives AI read access to your entire database, while restricting writes to isolated sandbox areas.

**Use Cases:**
- Ask AI to analyze sales data: "Show me top customers this quarter"
- Generate reports: "Create a summary table of user signups by region"
- Debug queries: "Why is this query slow?"
- Explore schema: "What tables contain customer information?"

AI can freely experiment in its sandbox without affecting production data.

## Why Safe?

**Session Sandbox Isolation** - Each AI session gets a unique prefix (`mcp_sandbox_{session_id}_`):
- ✅ AI can freely create/modify/delete objects with its prefix
- ✅ AI can read all data (SELECT, SHOW, DESCRIBE)
- ❌ AI cannot modify production data

**Example:**
```sql
-- ✅ AI can do this safely in sandbox
CREATE DATABASE mcp_sandbox_abc123_analysis
CREATE OR REPLACE TABLE mcp_sandbox_abc123_analysis.results (id INT)
INSERT INTO mcp_sandbox_abc123_analysis.results VALUES (1)
UPDATE mcp_sandbox_abc123_analysis.results SET id=2
DROP TABLE mcp_sandbox_abc123_analysis.results
SELECT * FROM production.sales  -- Read-only access

-- ❌ AI cannot do this
DROP TABLE production.users
UPDATE production.orders SET status='cancelled'
CREATE OR REPLACE TABLE production.temp (id INT)
```

**Defense in Depth:**
1. Database user permissions (primary)
2. MCP sandbox validation (additional layer)
3. SQL standardization to prevent bypass attempts

## Quick Start

### Install
```bash
uv tool install mcp-databend
```

### Configure

**Claude Code:**
```bash
claude mcp add mcp-databend --env DATABEND_DSN='databend://user:pwd@host:443/db?warehouse=wh' -- uv tool run mcp-databend
```

**Other MCP Clients:**
```json
{
  "mcpServers": {
    "mcp-databend": {
      "command": "uv",
      "args": ["tool", "run", "mcp-databend"],
      "env": {
        "DATABEND_DSN": "databend://user:pwd@host:443/db?warehouse=wh"
      }
    }
  }
}
```

Get your connection string from [Databend Cloud](https://app.databend.com) (free tier) or [docs](https://docs.databend.com/developer/drivers/#connection-string-dsn).

**Local Mode:** Set `LOCAL_MODE=true` to use embedded Databend (stores in `.databend/`).

## Available Tools

| Tool | Description |
|------|-------------|
| `execute_sql` | Execute SQL with sandbox validation |
| `execute_multi_sql` | Execute multiple SQL statements |
| `show_databases` | List all databases |
| `show_tables` | List tables in database |
| `describe_table` | Get table schema |
| `get_session_sandbox_prefix` | Get current session prefix |
| `list_session_sandbox_databases` | List sandbox databases |
| `create_session_sandbox_database` | Create sandbox database |
| `show_stages` / `list_stage_files` / `create_stage` | Stage management |
| `show_connections` | List connections |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABEND_DSN` | - | Connection string |
| `LOCAL_MODE` | `false` | Use local Databend |
| `DATABEND_QUERY_TIMEOUT` | `300` | Query timeout (seconds) |
| `DATABEND_MCP_SERVER_TRANSPORT` | `stdio` | Transport: `stdio`, `http`, `sse` |
| `DATABEND_MCP_BIND_HOST` | `127.0.0.1` | Bind host for HTTP/SSE |
| `DATABEND_MCP_BIND_PORT` | `8001` | Bind port for HTTP/SSE |

## Development

```bash
git clone https://github.com/databendlabs/mcp-databend
cd mcp-databend
uv sync

# Run locally
uv run python -m mcp_databend.main

# Debug
npx @modelcontextprotocol/inspector -e LOCAL_MODE=1 uv run python -m mcp_databend.main

# Test
uv run pytest
```
