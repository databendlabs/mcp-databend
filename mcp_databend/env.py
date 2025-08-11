"""Environment configuration for the MCP Databend server."""

from dataclasses import dataclass
import os


@dataclass
class DatabendConfig:
    """Configuration for Databend connection settings.

    Required environment variables:
        DATABEND_DSN: The dsn connect string (defaults to: "databend://default:@127.0.0.1:8000/?sslmode=disable")
        SAFE_MODE: Enable safe mode to restrict dangerous SQL operations (defaults to: "true")
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
