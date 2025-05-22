import json
import os
import httpx
import logging
import sys
from mcp.server.fastmcp import FastMCP
import concurrent.futures
from dotenv import load_dotenv
import atexit
from databend_env import get_config


MCP_SERVER_NAME = "mcp-databend"
QUERY_TIMEOUT = 30
# Configure logging to write to stderr
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(MCP_SERVER_NAME)

QUERY_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=10)
atexit.register(lambda: QUERY_EXECUTOR.shutdown(wait=True))
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP(MCP_SERVER_NAME)


def create_client():
    config = get_config()
    from databend_driver import BlockingDatabendClient
    client = BlockingDatabendClient(config.dsn)
    return client

def execute_query(query: str):
    client = create_client()
    conn = client.get_conn()
    try:
        cursor = conn.query_iter(query)
        names = [field.name for field in cursor.schema().fields()]
        rows = []
        for row in cursor:
            values = row.values()
            row_dict = dict(zip(names, list(values)))
            rows.append(row_dict)
        logger.info(f"Query returned {len(rows)} rows")
        return rows
    except Exception as err:
        logger.error(f"Error executing query: {err}")
        return {"error": str(err)}

@mcp.tool()
async def execute_sql(
    sql: str
) -> str:
    """Execute sql from databend

    Args:
        sql: sql to execute
    """
    logger.info("Executing sql: %s", sql)
    try:
        future = QUERY_EXECUTOR.submit(execute_query, sql)
        try:
            result = future.result(timeout=QUERY_TIMEOUT)
            if isinstance(result, dict) and "error" in result:
                logger.warning(f"Query failed: {result['error']}")
                return {
                    "status": "error",
                    "message": f"Query failed: {result['error']}",
                }
            return result
        except concurrent.futures.TimeoutError:
            logger.warning(
                f"Query timed out after {QUERY_TIMEOUT} seconds: {sql}"
            )
            future.cancel()
            return {
                "status": "error",
                "message": f"Query timed out after {QUERY_TIMEOUT} seconds",
            }
    except Exception as e:
        logger.error(f"Unexpected error in execute_sql: {str(e)}")
        return {"status": "error", "message": f"Unexpected error: {str(e)}"}


if __name__ == "__main__":
    # Log server startup
    logger.info("Starting databend MCP Server...")
    # Initialize and run the server
    try:
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Shutting down server by KeyboardInterrupt")
