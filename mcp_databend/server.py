import json
import os
import logging
import sys
from mcp.server.fastmcp import FastMCP
import concurrent.futures
from dotenv import load_dotenv
import atexit
from typing import Optional
from databend_env import get_config

# Constants
SERVER_NAME = "mcp-databend"
DEFAULT_TIMEOUT = 60  # seconds

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


def create_databend_client():
    """Create and return a Databend client instance."""
    config = get_config()
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
    client = create_databend_client()
    conn = client.get_conn()

    try:
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


def _execute_sql(sql: str) -> dict:
    logger.info(f"Executing SQL query: {sql}")

    try:
        # Submit query to thread pool
        future = QUERY_EXECUTOR.submit(execute_databend_query, sql)
        try:
            # Wait for query to complete with timeout
            result = future.result(timeout=DEFAULT_TIMEOUT)

            if isinstance(result, dict) and "error" in result:
                error_msg = f"Query execution failed: {result['error']}"
                logger.warning(error_msg)
                return {"status": "error", "message": error_msg}

            return result

        except concurrent.futures.TimeoutError:
            error_msg = f"Query timed out after {DEFAULT_TIMEOUT} seconds"
            logger.warning(f"{error_msg}: {sql}")
            future.cancel()
            return {"status": "error", "message": error_msg}

    except Exception as e:
        error_msg = f"Unexpected error in query execution: {str(e)}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}

@mcp.tool()
async def execute_sql(sql: str) -> dict:
    """
    Execute SQL query against Databend database.

    Args:
        sql: SQL query string to execute

    Returns:
        Dictionary containing either query results or error information
    """
    return _execute_sql(sql)


@mcp.tool()
def show_databases():
    """List available Databend databases"""
    logger.info("Listing all databases")
    return _execute_sql("SHOW DATABASES")


@mcp.tool()
def show_tables(database: Optional[str] = None, filter: Optional[str] = None):
    """
    List available Databend tables in a database
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
        sql += f" where '{filter}'"
    return _execute_sql(sql)


@mcp.tool()
def describe_table(table: str, database: Optional[str] = None):
    """
    Describe a Databend table
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
    return execute_sql(sql)


def main():
    """Main entry point for the MCP server."""
    try:
        logger.info("Starting Databend MCP Server...")
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Shutting down server by user request")
    except Exception as e:
        logger.error(f"Server startup failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
