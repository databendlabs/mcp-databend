"""Main entry point for the MCP Databend server."""

import sys
import logging
from .server import mcp, logger


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
