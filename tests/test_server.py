"""Tests for server safe mode behavior."""

from dataclasses import dataclass

from mcp_databend import server


@dataclass
class FakeConfig:
    safe_mode: bool
    query_timeout: int = 1


def test_execute_sql_blocks_non_sandbox_write_in_safe_mode(monkeypatch):
    monkeypatch.setattr(server, "get_config", lambda: FakeConfig(safe_mode=True))
    monkeypatch.setattr(server, "execute_databend_query", lambda sql: [])

    result = server._execute_sql("DROP DATABASE production")

    assert result["status"] == "error"
    assert "must start with" in result["message"]


def test_execute_sql_skips_sandbox_validation_when_safe_mode_disabled(monkeypatch):
    executed_sql = []

    monkeypatch.setattr(server, "get_config", lambda: FakeConfig(safe_mode=False))
    monkeypatch.setattr(
        server,
        "execute_databend_query",
        lambda sql: executed_sql.append(sql) or [],
    )

    result = server._execute_sql("DROP DATABASE production")

    assert result == {"status": "success", "data": [], "row_count": 0}
    assert executed_sql == ["DROP DATABASE production"]
