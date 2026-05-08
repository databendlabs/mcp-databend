"""Tests for environment configuration."""

from mcp_databend.env import DatabendConfig


def test_safe_mode_defaults_to_enabled(monkeypatch):
    monkeypatch.delenv("DATABEND_MCP_SAFE_MODE", raising=False)

    assert DatabendConfig().safe_mode is True


def test_safe_mode_falsey_values(monkeypatch):
    for value in ["false", "0", "no", "off"]:
        monkeypatch.setenv("DATABEND_MCP_SAFE_MODE", value)

        assert DatabendConfig().safe_mode is False


def test_safe_mode_rejects_invalid_value(monkeypatch):
    monkeypatch.setenv("DATABEND_MCP_SAFE_MODE", "flase")

    try:
        DatabendConfig().safe_mode
    except ValueError as err:
        assert "DATABEND_MCP_SAFE_MODE" in str(err)
    else:
        raise AssertionError("Expected ValueError")


def test_safe_mode_truthy_values(monkeypatch):
    for value in ["true", "1", "yes", "on"]:
        monkeypatch.setenv("DATABEND_MCP_SAFE_MODE", value)

        assert DatabendConfig().safe_mode is True
