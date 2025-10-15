"""Environment configuration for the MCP Databend server."""

from dataclasses import dataclass
import os
from enum import Enum


class TransportType(Enum):
    """Transport types for MCP server."""
    STDIO = "stdio"
    HTTP = "http"
    SSE = "sse"

    @classmethod
    def values(cls):
        """Get all transport type values."""
        return [member.value for member in cls]


@dataclass
class DatabendConfig:
    """Configuration for Databend connection settings.

    Required environment variables:
        DATABEND_DSN: The dsn connect string (defaults to: "databend://default:@127.0.0.1:8000/?sslmode=disable")
        SAFE_MODE: Enable safe mode to restrict dangerous SQL operations (defaults to: "true")
        LOCAL_MODE: Enable local mode to use in-memory Databend (defaults to: "false")
        DATABEND_QUERY_TIMEOUT: Query execution timeout in seconds (defaults to: "300")
    """

    def __init__(self):
        """Initialize the configuration from environment variables."""
        pass

    @property
    def dsn(self) -> str:
        """Get the Databend dsn connection string."""
        return os.environ.get(
            "DATABEND_DSN", "databend://default:@127.0.0.1:8000/?sslmode=disable"
        )

    @property
    def safe_mode(self) -> bool:
        """Get the safe mode setting."""
        return os.environ.get("SAFE_MODE", "true").lower() in ("true", "1", "yes", "on")

    @property
    def local_mode(self) -> bool:
        """Get the local mode setting."""
        return os.environ.get("LOCAL_MODE", "false").lower() in (
            "true",
            "1",
            "yes",
            "on",
        )

    @property
    def mcp_server_transport(self) -> str:
        """Get the MCP server transport method.

        Valid options: "stdio", "http", "sse"
        Default: "stdio"
        """
        transport = os.getenv("DATABEND_MCP_SERVER_TRANSPORT", TransportType.STDIO.value).lower()

        # Validate transport type
        if transport not in TransportType.values():
            valid_options = ", ".join(f'"{t}"' for t in TransportType.values())
            raise ValueError(f"Invalid transport '{transport}'. Valid options: {valid_options}")
        return transport

    @property
    def mcp_bind_host(self) -> str:
        """Get the MCP server bind host for HTTP/SSE transports.

        Default: "127.0.0.1"
        """
        return os.getenv("DATABEND_MCP_BIND_HOST", "127.0.0.1")

    @property
    def mcp_bind_port(self) -> int:
        """Get the MCP server bind port for HTTP/SSE transports.

        Default: 8001
        """
        port_str = os.getenv("DATABEND_MCP_BIND_PORT", "8001")
        try:
            port = int(port_str)
            if port < 1 or port > 65535:
                raise ValueError(f"Port must be between 1 and 65535, got {port}")
            return port
        except ValueError as e:
            if "invalid literal" in str(e):
                raise ValueError(f"Invalid port value '{port_str}'. Must be a valid integer.")
            raise

    @property
    def query_timeout(self) -> int:
        """Get the query execution timeout in seconds.

        Default: 300
        """
        timeout_str = os.getenv("DATABEND_QUERY_TIMEOUT", "300")
        try:
            timeout = int(timeout_str)
            if timeout < 1:
                raise ValueError(f"Query timeout must be greater than 0, got {timeout}")
            return timeout
        except ValueError as e:
            if "invalid literal" in str(e):
                raise ValueError(f"Invalid query timeout value '{timeout_str}'. Must be a valid integer.")
            raise


# Global instance placeholder for the singleton pattern
_CONFIG_INSTANCE = None


def get_config():
    """
    Gets the singleton instance of DatabendConfig.
    Instantiates it on the first call.
    """
    global _CONFIG_INSTANCE
    if _CONFIG_INSTANCE is None:
        _CONFIG_INSTANCE = DatabendConfig()
    return _CONFIG_INSTANCE
