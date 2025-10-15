import json
import os
import logging
import sys
import re
from fastmcp import FastMCP
from fastmcp.tools import Tool
import concurrent.futures
from dotenv import load_dotenv
import pyarrow as pa
import atexit
from typing import Optional
from .env import get_config, TransportType

# Constants
SERVER_NAME = "mcp-databend"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(SERVER_NAME)

# Initialize thread pool and cleanup
QUERY_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=10)
atexit.register(lambda: QUERY_EXECUTOR.shutdown(wait=True))

# Load environment variables
load_dotenv()

# Initialize MCP server
mcp = FastMCP(SERVER_NAME)

# Global Databend client singleton
_databend_client = None


def get_global_databend_client():
    """Get global Databend client instance (deprecated, use create_databend_client)."""
    global _databend_client
    if _databend_client is None:
        _databend_client = create_databend_client()
    return _databend_client


def is_sql_safe(sql: str) -> tuple[bool, str]:
    """
    Check if SQL query is safe to execute in safe mode.

    Args:
        sql: SQL query string to check

    Returns:
        Tuple of (is_safe, reason) where is_safe is boolean and reason is error message if unsafe
    """
    sql_upper = sql.upper().strip()

    # List of dangerous operations to block in safe mode
    dangerous_patterns = [
        (r"\bDROP\s+", "DROP operations are not allowed in MCP safe mode"),
        (r"\bDELETE\s+", "DELETE operations are not allowed in MCP safe mode"),
        (r"\bTRUNCATE\s+", "TRUNCATE operations are not allowed in MCP safe mode"),
        (r"\bALTER\s+", "ALTER operations are not allowed in MCP safe mode"),
        (r"\bUPDATE\s+", "UPDATE operations are not allowed in MCP safe mode"),
        (r"\bREVOKE\s+", "REVOKE operations are not allowed in MCP safe mode"),
    ]

    # Check each dangerous pattern
    for pattern, reason in dangerous_patterns:
        if re.search(pattern, sql_upper, re.IGNORECASE | re.DOTALL):
            return False, reason

    return True, ""


def create_databend_client():
    """Create and return a Databend client instance."""
    config = get_config()

    if config.local_mode:
        logger.info("Creating Databend client local")
        # Use local in-memory Databend
        import databend
        return databend.SessionContext()
    else:
        logger.info("Creating Databend client dsn")
        # Use remote Databend server
        from databend_driver import BlockingDatabendClient

        return BlockingDatabendClient(config.dsn)


def execute_databend_query(sql: str) -> list[dict] | dict:
    """
    Execute a SQL query against Databend and return results.

    Args:
        sql: SQL query string to execute

    Returns:
        List of dictionaries containing query results or error dictionary
    """
    client = get_global_databend_client()
    config = get_config()

    try:
        if config.local_mode:
            # Handle local in-memory Databend
            result = client.sql(sql)
            df = result.to_py_arrow()
            return recordbatches_to_dicts(df)
        else:
            # Handle remote Databend server
            conn = client.get_conn()
            cursor = conn.query_iter(sql)
            column_names = [field.name for field in cursor.schema().fields()]
            results = []

            for row in cursor:
                row_data = dict(zip(column_names, list(row.values())))
                results.append(row_data)

            logger.info(f"Query executed successfully, returned {len(results)} rows")
            return results

    except Exception as err:
        error_msg = f"Error executing query: {str(err)}"
        logger.error(error_msg)
        return {"error": error_msg}

def recordbatches_to_dicts(batches: list[pa.RecordBatch]) -> list[dict]:
    results = []
    for batch in batches:
        columns = batch.schema.names
        columns_data = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
        for row in zip(*columns_data):
            results.append(dict(zip(columns, row)))
    return results


def _execute_sql(sql: str) -> dict:
    logger.info(f"Executing SQL query: {sql}")

    # Check safe mode configuration
    config = get_config()
    if config.safe_mode:
        is_safe, reason = is_sql_safe(sql)
        if not is_safe:
            error_msg = f"Query blocked by MCP safe mode: {reason}. Current mode: SAFE_MODE=true. To disable, set SAFE_MODE=false"
            logger.warning(f"{error_msg} - Query: {sql}")
            return {"status": "error", "message": error_msg}
    else:
        logger.info("MCP safe mode is disabled (SAFE_MODE=false)")

    try:
        # Submit query to thread pool
        future = QUERY_EXECUTOR.submit(execute_databend_query, sql)
        query_timeout = config.query_timeout
        try:
            # Wait for query to complete with timeout
            result = future.result(timeout=query_timeout)

            if isinstance(result, dict) and "error" in result:
                error_msg = f"Query execution failed: {result['error']}"
                logger.warning(error_msg)
                return {"status": "error", "message": error_msg}

            # Ensure we always return a dict structure for fastmcp compatibility
            if isinstance(result, list):
                return {"status": "success", "data": result, "row_count": len(result)}
            else:
                return {"status": "success", "data": result}

        except concurrent.futures.TimeoutError:
            error_msg = f"Query timed out after {query_timeout} seconds"
            logger.warning(f"{error_msg}: {sql}")
            future.cancel()
            return {"status": "error", "message": error_msg}

    except Exception as e:
        error_msg = f"Unexpected error in query execution: {str(e)}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}


def execute_multi_sql(sqls: list[str]) -> list[dict]:
    """
    Execute multiple SQL queries against Databend database with MCP safe mode protection.

    Safe mode (enabled by default) blocks dangerous operations like DROP, DELETE,
    TRUNCATE, ALTER, UPDATE, and REVOKE. Set SAFE_MODE=false to disable.

    Args:
        sqls: List of SQL query strings to execute

    Returns:
        List of dictionaries containing either query results or error information
    """
    results = []
    for sql in sqls:
        results.append(execute_sql(sql))
    return results

def execute_sql(sql: str) -> dict:
    """
    Execute SQL query against Databend database with MCP safe mode protection.

    Safe mode (enabled by default) blocks dangerous operations like DROP, DELETE,
    TRUNCATE, ALTER, UPDATE, and REVOKE. Set SAFE_MODE=false to disable.

    Args:
        sql: SQL query string to execute

    Returns:
        Dictionary containing either query results or error information
    """
    return _execute_sql(sql)


def show_databases():
    """List available Databend databases (safe operation, not affected by MCP safe mode)"""
    logger.info("Listing all databases")
    return _execute_sql("SHOW DATABASES")


def show_tables(database: Optional[str] = None, filter: Optional[str] = None):
    """
    List available Databend tables in a database (safe operation, not affected by MCP safe mode)
    Args:
        database: The database name
        filter: The filter string, eg: "name like 'test%'"

    Returns:
        Dictionary containing either query results or error information
    """
    logger.info(f"Listing tables in database '{database}'")
    sql = f"SHOW TABLES"
    if database is not None:
        sql += f" FROM {database}"
    if filter is not None:
        sql += f" WHERE {filter}"
    return _execute_sql(sql)

def show_functions(filter: Optional[str] = None):
    """List available Databend functions (safe operation, not affected by MCP safe mode)
    Args:
        filter: The filter string, eg: "name like 'add%'"
    Returns:
        Dictionary containing either query results or error information
    """
    logger.info("Listing all functions")
    sql = "SHOW FUNCTIONS"
    if filter is not None:
        sql += f" WHERE {filter}"
    return _execute_sql(sql)

def describe_table(table: str, database: Optional[str] = None):
    """
    Describe a Databend table (safe operation, not affected by MCP safe mode)
    Args:
        table: The table name
        database: The database name

    Returns:
        Dictionary containing either query results or error information
    """
    table = table.strip()
    if database is not None:
        table = f"{database}.{table}"
    logger.info(f"Describing table '{table}'")
    sql = f"DESCRIBE TABLE {table}"
    return _execute_sql(sql)


def show_stages():
    """List available Databend stages (safe operation, not affected by MCP safe mode)"""
    logger.info("Listing all stages")
    return _execute_sql("SHOW STAGES")


def list_stage_files(stage_name: str, path: Optional[str] = None):
    """
    List files in a Databend stage (safe operation, not affected by MCP safe mode)
    Args:
        stage_name: The stage name (with @ prefix)
        path: Optional path within the stage

    Returns:
        Dictionary containing either query results or error information
    """
    if not stage_name.startswith("@"):
        stage_name = f"@{stage_name}"

    if path:
        stage_path = f"{stage_name}/{path.strip('/')}"
    else:
        stage_path = stage_name

    logger.info(f"Listing files in stage '{stage_path}'")
    sql = f"LIST {stage_path}"
    return _execute_sql(sql)


def show_connections():
    """List available Databend connections (safe operation, not affected by MCP safe mode)"""
    logger.info("Listing all connections")
    return _execute_sql("SHOW CONNECTIONS")


def create_stage(
    name: str, url: str, connection_name: Optional[str] = None
) -> dict:
    """
    Create a Databend stage with connection
    Args:
        name: The stage name
        url: The stage URL (e.g., 's3://bucket-name')
        connection_name: Optional connection name to use

    Returns:
        Dictionary containing either query results or error information
    """
    logger.info(f"Creating stage '{name}' with URL '{url}'")

    sql_parts = [f"CREATE STAGE {name}", f"URL = '{url}'"]

    if connection_name:
        sql_parts.append(f"CONNECTION = (CONNECTION_NAME = '{connection_name}')")

    sql = " ".join(sql_parts)
    return _execute_sql(sql)


# Register all tools
mcp.add_tool(Tool.from_function(execute_sql))
mcp.add_tool(Tool.from_function(execute_multi_sql))
mcp.add_tool(Tool.from_function(show_databases))
mcp.add_tool(Tool.from_function(show_tables))
mcp.add_tool(Tool.from_function(show_functions))
mcp.add_tool(Tool.from_function(describe_table))
mcp.add_tool(Tool.from_function(show_stages))
mcp.add_tool(Tool.from_function(list_stage_files))
mcp.add_tool(Tool.from_function(show_connections))
mcp.add_tool(Tool.from_function(create_stage))


def main():
    """Main entry point for the MCP server."""
    try:
        config = get_config()
        transport = config.mcp_server_transport

        logger.info(f"Starting Databend MCP Server with transport: {transport}")

        # For HTTP and SSE transports, we need to specify host and port
        http_transports = [TransportType.HTTP.value, TransportType.SSE.value]
        if transport in http_transports:
            # Use the configured bind host (defaults to 127.0.0.1, can be set to 0.0.0.0)
            # and bind port (defaults to 8001)
            mcp.run(transport=transport, host=config.mcp_bind_host, port=config.mcp_bind_port)
        else:
            # For stdio transport, no host or port is needed
            mcp.run(transport=transport)
    except KeyboardInterrupt:
        logger.info("Shutting down server by user request")
    except Exception as e:
        logger.error(f"Server startup failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
