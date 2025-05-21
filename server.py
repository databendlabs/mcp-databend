import json
import os
import httpx
import logging
import sys
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

# Configure logging to write to stderr
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("databend-mcp")

# Initialize FastMCP server
mcp = FastMCP("databend-mcp")

@mcp.tool()
async def execute_sql(
    sql: str
) -> str:
    """Execute sql from databend

    Args:
        sql: sql to execute
    """
    f"sql is {sql}"


if __name__ == "__main__":
    # Log server startup
    logger.info("Starting databend MCP Server...")

    # Initialize and run the server
    mcp.run(transport="stdio")

    # This line won't be reached during normal operation
    logger.info("Server stopped")
