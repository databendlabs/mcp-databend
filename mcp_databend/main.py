"""Main entry point for the MCP Databend server."""

import sys
import logging
from .server import mcp, logger
from .env import get_config, TransportType


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
