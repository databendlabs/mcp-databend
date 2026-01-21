"""Tests for SQL safety module."""

import pytest
from mcp_databend.safety import (
    check_sql_safety,
    get_session_prefix,
    SESSION_ID,
    SANDBOX_PREFIX,
    SafetyResult,
)


class TestReadOnlyQueries:
    """Read-only queries should always be allowed."""

    @pytest.mark.parametrize("sql", [
        "SELECT * FROM production.users",
        "SHOW DATABASES",
        "SHOW TABLES FROM mydb",
        "DESCRIBE TABLE mydb.users",
        "DESC mydb.orders",
        "EXPLAIN SELECT * FROM t",
        "LIST @mystage",
        "WITH cte AS (SELECT 1) SELECT * FROM cte",
        "SELECT * FROM mcp_sandbox_other_session.table1",  # Can read other sessions
    ])
    def test_read_queries_allowed(self, sql):
        result = check_sql_safety(sql)
        assert result.allowed is True


class TestCurrentSessionSandbox:
    """Write operations on current session sandbox should be allowed."""

    def test_create_database(self):
        sql = f"CREATE DATABASE {get_session_prefix()}mydb"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_create_table(self):
        sql = f"CREATE TABLE {get_session_prefix()}mydb.users (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_insert(self):
        sql = f"INSERT INTO {get_session_prefix()}mydb.users VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_update(self):
        sql = f"UPDATE {get_session_prefix()}mydb.users SET name = 'x'"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_delete(self):
        sql = f"DELETE FROM {get_session_prefix()}mydb.users WHERE id = 1"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_drop_table(self):
        sql = f"DROP TABLE {get_session_prefix()}mydb.users"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_drop_database(self):
        sql = f"DROP DATABASE {get_session_prefix()}mydb"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_truncate(self):
        sql = f"TRUNCATE TABLE {get_session_prefix()}mydb.users"
        result = check_sql_safety(sql)
        assert result.allowed is True


class TestBlockedOperations:
    """Operations that should be blocked."""

    def test_create_or_replace_sandbox_allowed(self):
        sql = f"CREATE OR REPLACE TABLE {get_session_prefix()}mydb.t (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_create_or_replace_view_sandbox_allowed(self):
        sql = f"CREATE OR REPLACE VIEW {get_session_prefix()}mydb.v AS SELECT 1"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_create_or_replace_non_sandbox_blocked(self):
        sql = "CREATE OR REPLACE TABLE production.users (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is False
        assert "CREATE OR REPLACE" in result.reason

    def test_replace_into_sandbox_allowed(self):
        sql = f"REPLACE INTO {get_session_prefix()}mydb.users VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_replace_into_non_sandbox_blocked(self):
        sql = "REPLACE INTO production.users VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is False
        assert "REPLACE INTO" in result.reason

    def test_non_sandbox_create_table_blocked(self):
        sql = "CREATE TABLE production.test (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is False
        assert "production" in result.reason.lower()

    def test_non_sandbox_insert_blocked(self):
        sql = "INSERT INTO mydb.users VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_non_sandbox_drop_blocked(self):
        sql = "DROP DATABASE important_db"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_non_sandbox_update_blocked(self):
        sql = "UPDATE production.users SET admin = true"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_non_sandbox_delete_blocked(self):
        sql = "DELETE FROM production.logs"
        result = check_sql_safety(sql)
        assert result.allowed is False


class TestSessionIsolation:
    """Session isolation tests."""

    def test_session_prefix_format(self):
        prefix = get_session_prefix()
        assert prefix.startswith(SANDBOX_PREFIX)
        assert prefix.endswith("_")
        assert SESSION_ID in prefix

    def test_different_session_write_blocked(self):
        other_session_prefix = f"{SANDBOX_PREFIX}xyz12345_"
        sql = f"INSERT INTO {other_session_prefix}db.table VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_drop_other_session_sandbox_blocked(self):
        other_session_prefix = f"{SANDBOX_PREFIX}other123_"
        sql = f"DROP DATABASE {other_session_prefix}mydb"
        result = check_sql_safety(sql)
        assert result.allowed is False


class TestNestedSQL:
    """Nested SQL and referenced object checks."""

    def test_task_with_sandbox_sql_allowed(self):
        """TASK with nested SQL writing to sandbox should be allowed."""
        sql = f"CREATE TASK {get_session_prefix()}t AS INSERT INTO {get_session_prefix()}db.t VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_task_with_non_sandbox_sql_blocked(self):
        """TASK with nested SQL writing to non-sandbox should be blocked."""
        sql = f"CREATE TASK {get_session_prefix()}t AS INSERT INTO production.users VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is False
        assert "Referenced object" in result.reason

    def test_pipe_with_non_sandbox_blocked(self):
        """PIPE with COPY INTO non-sandbox should be blocked."""
        sql = f"CREATE PIPE {get_session_prefix()}p AS COPY INTO production.t FROM @stage"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_pipe_with_sandbox_allowed(self):
        """PIPE with COPY INTO sandbox should be allowed."""
        sql = f"CREATE PIPE {get_session_prefix()}p AS COPY INTO {get_session_prefix()}db.t FROM @stage"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_stream_on_sandbox_table_allowed(self):
        """STREAM on sandbox table should be allowed."""
        sql = f"CREATE STREAM {get_session_prefix()}s ON TABLE {get_session_prefix()}db.orders"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_stream_on_non_sandbox_table_blocked(self):
        """STREAM on non-sandbox table should be blocked."""
        sql = f"CREATE STREAM {get_session_prefix()}s ON TABLE production.orders"
        result = check_sql_safety(sql)
        assert result.allowed is False
        assert "Referenced object" in result.reason

    def test_view_can_reference_any_table(self):
        """VIEW can reference any table (read-only)."""
        # VIEW itself must be in sandbox, but SELECT FROM can read anything
        sql = f"CREATE VIEW {get_session_prefix()}v AS SELECT * FROM production.users"
        result = check_sql_safety(sql)
        assert result.allowed is True


class TestGrantRevoke:
    """GRANT/REVOKE privilege operation tests."""

    def test_grant_on_non_sandbox_db_blocked(self):
        """GRANT ON non-sandbox database should be blocked."""
        sql = "GRANT SELECT ON DATABASE production TO user1"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_grant_on_sandbox_db_to_non_sandbox_user_blocked(self):
        """GRANT sandbox db TO non-sandbox user should be blocked."""
        sql = f"GRANT SELECT ON DATABASE {get_session_prefix()}db TO root"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_grant_role_non_sandbox_blocked(self):
        """GRANT non-sandbox ROLE should be blocked."""
        sql = "GRANT ROLE admin TO user1"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_grant_all_sandbox_allowed(self):
        """GRANT with all sandbox objects should be allowed."""
        prefix = get_session_prefix()
        sql = f"GRANT SELECT ON DATABASE {prefix}db TO {prefix}user"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_revoke_from_non_sandbox_blocked(self):
        """REVOKE FROM non-sandbox user should be blocked."""
        sql = f"REVOKE SELECT ON DATABASE {get_session_prefix()}db FROM admin"
        result = check_sql_safety(sql)
        assert result.allowed is False


class TestAllDatabendObjects:
    """Test various Databend object types."""

    def test_create_stage_in_sandbox(self):
        sql = f"CREATE STAGE {get_session_prefix()}mystage"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_create_stage_outside_sandbox_blocked(self):
        sql = "CREATE STAGE production_stage"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_create_function_in_sandbox(self):
        sql = f"CREATE FUNCTION {get_session_prefix()}myfunc AS (x) -> x + 1"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_create_user_in_sandbox(self):
        sql = f"CREATE USER {get_session_prefix()}testuser IDENTIFIED BY 'pass'"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_create_user_outside_sandbox_blocked(self):
        sql = "CREATE USER admin IDENTIFIED BY 'pass'"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_alter_table_in_sandbox(self):
        sql = f"ALTER TABLE {get_session_prefix()}db.t ADD COLUMN c INT"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_alter_table_outside_sandbox_blocked(self):
        sql = "ALTER TABLE production.users ADD COLUMN c INT"
        result = check_sql_safety(sql)
        assert result.allowed is False


class TestEdgeCases:
    """Edge cases and special scenarios."""

    def test_empty_sql(self):
        result = check_sql_safety("")
        assert isinstance(result, SafetyResult)

    def test_whitespace_sql(self):
        result = check_sql_safety("   ")
        assert isinstance(result, SafetyResult)

    def test_multiline_sql(self):
        sql = f"""
        CREATE TABLE {get_session_prefix()}mydb.users (
            id INT,
            name VARCHAR(100)
        )
        """
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_multiline_non_sandbox_blocked(self):
        sql = """
        CREATE TABLE production.users (
            id INT,
            name VARCHAR(100)
        )
        """
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_sql_with_comments(self):
        sql = f"-- This creates a table\nCREATE TABLE {get_session_prefix()}db.users (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_case_insensitive_keywords(self):
        sql = f"create table {get_session_prefix()}db.test (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_case_insensitive_sandbox_prefix(self):
        # Sandbox prefix matching should be case insensitive
        sql = f"CREATE TABLE {get_session_prefix().upper()}DB.test (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_optimize_table_in_sandbox(self):
        sql = f"OPTIMIZE TABLE {get_session_prefix()}db.users"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_optimize_table_outside_sandbox_blocked(self):
        sql = "OPTIMIZE TABLE production.users"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_vacuum_table_in_sandbox(self):
        sql = f"VACUUM TABLE {get_session_prefix()}db.logs"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_vacuum_table_outside_sandbox_blocked(self):
        sql = "VACUUM TABLE production.logs"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_copy_into_sandbox(self):
        sql = f"COPY INTO {get_session_prefix()}db.t FROM @stage/data.csv"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_copy_into_non_sandbox_blocked(self):
        sql = "COPY INTO production.t FROM @stage/data.csv"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_merge_into_sandbox(self):
        sql = f"MERGE INTO {get_session_prefix()}db.target USING source ON target.id = source.id"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_merge_into_non_sandbox_blocked(self):
        sql = "MERGE INTO production.target USING source ON target.id = source.id"
        result = check_sql_safety(sql)
        assert result.allowed is False


class TestDatabaseTableFormat:
    """Test db.table format handling."""

    def test_insert_db_table_sandbox(self):
        sql = f"INSERT INTO {get_session_prefix()}db.users VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_insert_db_table_non_sandbox_blocked(self):
        sql = "INSERT INTO production.users VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is False
        assert "production" in result.reason

    def test_update_db_table_sandbox(self):
        sql = f"UPDATE {get_session_prefix()}db.users SET name='x'"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_update_db_table_non_sandbox_blocked(self):
        sql = "UPDATE production.users SET name='x'"
        result = check_sql_safety(sql)
        assert result.allowed is False

    def test_insert_with_join_non_sandbox_blocked(self):
        sql = f"INSERT INTO {get_session_prefix()}db.t SELECT * FROM production.users"
        result = check_sql_safety(sql)
        assert result.allowed is False
        assert "non-sandbox" in result.reason

    def test_create_table_db_table_format(self):
        sql = f"CREATE TABLE {get_session_prefix()}db.users (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_create_table_db_table_non_sandbox_blocked(self):
        sql = "CREATE TABLE production.users (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is False


class TestSQLStandardization:
    """Test SQL standardization handles malformed SQL."""

    def test_create_or_replace_with_comments(self):
        sql = f"CREATE/*comment*/OR/*comment*/REPLACE TABLE {get_session_prefix()}t (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_create_or_replace_with_newlines(self):
        sql = f"CREATE\n  OR\n  REPLACE TABLE {get_session_prefix()}t (id INT)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_insert_with_excessive_whitespace(self):
        sql = f"INSERT    INTO    {get_session_prefix()}db.t    VALUES    (1)"
        result = check_sql_safety(sql)
        assert result.allowed is True

    def test_insert_non_sandbox_with_comments(self):
        sql = "INSERT/*comment*/INTO production.users VALUES (1)"
        result = check_sql_safety(sql)
        assert result.allowed is False


