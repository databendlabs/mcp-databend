"""SQL Safety module - Code-enforced constraints for AI SQL operations.

Session-level isolation ensures each MCP session can only modify objects
with its unique sandbox prefix: mcp_sandbox_{session_id}_

DEFENSE STRATEGY: Database-level permissions + MCP validation
1. Database user should have LIMITED permissions (only on sandbox databases)
2. MCP validates SQL as additional safety layer
3. Default DENY - only explicitly allowed patterns pass

ARCHITECTURE: Modular rule-based system
- Each rule is independent and can be modified/added/removed easily
- Rules are checked in order, first match wins
"""

import re
import secrets
from dataclasses import dataclass
from typing import Callable

# Session ID: generated at server startup, constant for process lifetime
SESSION_ID = secrets.token_hex(4)
SANDBOX_PREFIX = "mcp_sandbox_"


@dataclass
class SafetyResult:
    """Result of safety check."""
    allowed: bool
    reason: str = ""


def get_session_prefix() -> str:
    """Return current session's sandbox prefix."""
    return f"{SANDBOX_PREFIX}{SESSION_ID}_"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _normalize_sql(sql: str) -> str:
    """Remove comments and strings to prevent injection."""
    sql = re.sub(r'--[^\n]*', ' ', sql)
    sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
    sql = re.sub(r"'(?:[^'\\]|\\.)*'", ' ', sql)
    sql = re.sub(r'"(?:[^"\\]|\\.)*"', ' ', sql)
    sql = re.sub(r'`[^`]*`', ' ', sql)
    return sql


def _standardize_sql(sql: str) -> str:
    """Standardize SQL format for reliable pattern matching."""
    # Remove comments
    sql = re.sub(r'--[^\n]*', ' ', sql)
    sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.DOTALL)
    # Normalize whitespace (including newlines) to single space
    sql = re.sub(r'\s+', ' ', sql)
    return sql.strip()


def _extract_identifier(identifier: str, session_prefix: str) -> tuple[str, bool]:
    """Extract identifier and check if it's sandbox.

    Handles: table, db.table, `db`.`table`
    Returns: (name, is_sandbox)
    """
    identifier = identifier.strip('`\'"')

    # Handle db.table format - check database name
    if '.' in identifier:
        db_name = identifier.split('.')[0].strip('`\'"')
        return db_name, db_name.lower().startswith(session_prefix.lower())

    return identifier, identifier.lower().startswith(session_prefix.lower())


def _contains_non_sandbox_reference(sql: str, session_prefix: str) -> bool:
    """Check if SQL contains FROM/JOIN/INTO references to non-sandbox objects."""
    sql_clean = _normalize_sql(sql)

    # Support db.table format
    ref_patterns = [
        r'\bFROM\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)',
        r'\bJOIN\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)',
        r'\bINTO\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)',
        r'\bUPDATE\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)',
    ]

    for pattern in ref_patterns:
        for match in re.finditer(pattern, sql_clean, re.IGNORECASE):
            ref = match.group(1)
            if ref.upper() in ('SELECT', 'VALUES', 'USING'):
                continue
            _, is_sandbox = _extract_identifier(ref, session_prefix)
            if not is_sandbox:
                return True

    return False


# ============================================================================
# RULE CHECKERS - Each function checks one specific rule
# ============================================================================

def _check_create_or_replace(sql: str, session_prefix: str) -> SafetyResult | None:
    """Rule: CREATE OR REPLACE must be on sandbox objects."""
    match = re.search(r'\bCREATE\s+OR\s+REPLACE\s+(TABLE|VIEW|FUNCTION|STAGE|TASK|PIPE|STREAM)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)', sql, re.IGNORECASE)
    if not match:
        return None

    obj_name = match.group(2)
    name, is_sandbox = _extract_identifier(obj_name, session_prefix)
    if is_sandbox:
        return SafetyResult(allowed=True)
    return SafetyResult(
        allowed=False,
        reason=f"CREATE OR REPLACE blocked: '{name}' must start with {session_prefix}"
    )


def _check_replace_into(sql: str, session_prefix: str) -> SafetyResult | None:
    """Rule: REPLACE INTO must be on sandbox objects."""
    match = re.search(r'\bREPLACE\s+INTO\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)', sql, re.IGNORECASE)
    if not match:
        return None

    obj_name = match.group(1)
    name, is_sandbox = _extract_identifier(obj_name, session_prefix)
    if is_sandbox:
        return SafetyResult(allowed=True)
    return SafetyResult(
        allowed=False,
        reason=f"REPLACE INTO blocked: '{name}' must start with {session_prefix}"
    )


def _check_read_only(sql: str, session_prefix: str) -> SafetyResult | None:
    """Rule: Allow read-only queries on ANY object."""
    patterns = [
        r'^\s*SELECT\b',
        r'^\s*WITH\b.*\bSELECT\b',
        r'^\s*SHOW\b',
        r'^\s*DESCRIBE\b',
        r'^\s*DESC\b',
        r'^\s*EXPLAIN\b',
        r'^\s*LIST\s+@',
    ]
    sql_upper = sql.strip().upper()
    for pattern in patterns:
        if re.match(pattern, sql_upper, re.DOTALL):
            return SafetyResult(allowed=True)
    return None


def _check_create_drop(sql: str, session_prefix: str) -> SafetyResult | None:
    """Rule: CREATE/DROP operations must be on sandbox objects."""
    # Support db.table format
    pattern = r'\b(CREATE|DROP)\s+(DATABASE|TABLE|VIEW|STAGE|FUNCTION|USER|ROLE|TASK|PIPE|STREAM|CONNECTION)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)'
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
        return None

    obj_type = match.group(2).upper()
    obj_name = match.group(3)

    # Check object name has sandbox prefix
    name, is_sandbox = _extract_identifier(obj_name, session_prefix)
    if not is_sandbox:
        return SafetyResult(
            allowed=False,
            reason=f"Operation blocked: '{name}' must start with {session_prefix}"
        )

    # For TASK/PIPE, check nested SQL references
    if obj_type in ('TASK', 'PIPE'):
        as_match = re.search(r'\bAS\s+(.+)$', sql, re.IGNORECASE | re.DOTALL)
        if as_match:
            nested_sql = as_match.group(1).strip()
            if _contains_non_sandbox_reference(nested_sql, session_prefix):
                return SafetyResult(
                    allowed=False,
                    reason=f"CREATE {obj_type} blocked: Referenced object in nested SQL must start with {session_prefix}"
                )

    # For STREAM, check ON TABLE reference
    if obj_type == 'STREAM':
        on_table_match = re.search(r'\bON\s+TABLE\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)', sql, re.IGNORECASE)
        if on_table_match:
            table_name = on_table_match.group(1)
            name, is_sandbox = _extract_identifier(table_name, session_prefix)
            if not is_sandbox:
                return SafetyResult(
                    allowed=False,
                    reason=f"CREATE STREAM blocked: Referenced object '{name}' must start with {session_prefix}"
                )

    return SafetyResult(allowed=True)


def _check_alter(sql: str, session_prefix: str) -> SafetyResult | None:
    """Rule: ALTER operations must be on sandbox objects."""
    pattern = r'\bALTER\s+(TABLE|DATABASE|STAGE|USER|ROLE)\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)'
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
        return None

    obj_name = match.group(2)
    name, is_sandbox = _extract_identifier(obj_name, session_prefix)
    if is_sandbox:
        return SafetyResult(allowed=True)
    return SafetyResult(
        allowed=False,
        reason=f"ALTER blocked: '{name}' must start with {session_prefix}"
    )


def _check_optimize_vacuum(sql: str, session_prefix: str) -> SafetyResult | None:
    """Rule: OPTIMIZE/VACUUM/ANALYZE must be on sandbox objects."""
    pattern = r'\b(OPTIMIZE|VACUUM|ANALYZE)\s+TABLE\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)'
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
        return None

    obj_name = match.group(2)
    name, is_sandbox = _extract_identifier(obj_name, session_prefix)
    if is_sandbox:
        return SafetyResult(allowed=True)
    return SafetyResult(
        allowed=False,
        reason=f"{match.group(1)} blocked: '{name}' must start with {session_prefix}"
    )


def _check_grant_revoke(sql: str, session_prefix: str) -> SafetyResult | None:
    """Rule: GRANT/REVOKE - all involved objects must be sandbox."""
    if not re.search(r'\b(GRANT|REVOKE)\b', sql, re.IGNORECASE):
        return None

    grant_identifiers = []

    # Extract ON DATABASE/TABLE patterns - support db.table
    for pattern in [
        r'\bON\s+DATABASE\s+([`\'"]?\w+[`\'"]?)',
        r'\bON\s+TABLE\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)',
        r'\bON\s+([`\'"]?\w+[`\'"]?)\.\\*',
    ]:
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            grant_identifiers.append(match.group(1))

    # Extract ROLE/TO/FROM patterns
    for pattern in [
        r'\bROLE\s+([`\'"]?\w+[`\'"]?)',
        r'\bTO\s+([`\'"]?\w+[`\'"]?)',
        r'\bFROM\s+([`\'"]?\w+[`\'"]?)',
    ]:
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            grant_identifiers.append(match.group(1))

    # Check all extracted identifiers
    for identifier in grant_identifiers:
        name, is_sandbox = _extract_identifier(identifier, session_prefix)
        if not is_sandbox:
            return SafetyResult(
                allowed=False,
                reason=f"GRANT/REVOKE blocked: '{name}' must start with {session_prefix}"
            )

    if grant_identifiers:
        return SafetyResult(allowed=True)
    return None


def _check_dml(sql: str, session_prefix: str) -> SafetyResult | None:
    """Rule: DML operations must be on sandbox objects."""
    # Support db.table format
    dml_patterns = [
        (r'\bINSERT\s+(?:INTO\s+)?([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)', 'INSERT'),
        (r'\bUPDATE\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)', 'UPDATE'),
        (r'\bDELETE\s+FROM\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)', 'DELETE'),
        (r'\bTRUNCATE\s+(?:TABLE\s+)?([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)', 'TRUNCATE'),
        (r'\bCOPY\s+INTO\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)', 'COPY INTO'),
        (r'\bMERGE\s+INTO\s+([`\'"]?\w+[`\'"]?(?:\.[`\'"]?\w+[`\'"]?)?)', 'MERGE INTO'),
    ]

    for pattern, op_name in dml_patterns:
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            obj_name = match.group(1)
            name, is_sandbox = _extract_identifier(obj_name, session_prefix)
            if is_sandbox:
                # Check if SQL contains non-sandbox references in FROM/JOIN
                if _contains_non_sandbox_reference(sql, session_prefix):
                    return SafetyResult(
                        allowed=False,
                        reason=f"{op_name} blocked: SQL references non-sandbox objects. "
                               f"All objects must start with {session_prefix}"
                    )
                return SafetyResult(allowed=True)
            return SafetyResult(
                allowed=False,
                reason=f"{op_name} blocked: '{name}' must start with {session_prefix}"
            )
    return None


def _check_remove_stage(sql: str, session_prefix: str) -> SafetyResult | None:
    """Rule: REMOVE @stage must be on sandbox stage."""
    match = re.search(r'\bREMOVE\s+@(\w+)', sql, re.IGNORECASE)
    if not match:
        return None

    stage_name = match.group(1)
    if stage_name.lower().startswith(session_prefix.lower()):
        return SafetyResult(allowed=True)
    return SafetyResult(
        allowed=False,
        reason=f"REMOVE blocked: stage '{stage_name}' must start with {session_prefix}"
    )


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

# Rule registry: order matters, first match wins
SAFETY_RULES: list[Callable[[str, str], SafetyResult | None]] = [
    _check_create_or_replace,  # Must be first - always reject
    _check_replace_into,       # Reject REPLACE INTO
    _check_read_only,          # Allow read operations
    _check_create_drop,        # DDL operations
    _check_alter,              # ALTER operations
    _check_optimize_vacuum,    # Maintenance operations
    _check_grant_revoke,       # Permission operations
    _check_dml,                # Data manipulation
    _check_remove_stage,       # Stage file operations
]


def check_sql_safety(sql: str) -> SafetyResult:
    """
    Check if SQL is safe - STRICT whitelist approach.

    Rules are checked in order. First matching rule determines the result.
    If no rule matches, operation is REJECTED by default.

    Args:
        sql: SQL query string

    Returns:
        SafetyResult with allowed status and reason if blocked
    """
    # Standardize SQL format first
    sql_std = _standardize_sql(sql)
    session_prefix = get_session_prefix()

    # Check each rule in order
    for rule in SAFETY_RULES:
        result = rule(sql_std, session_prefix)
        if result is not None:
            return result

    # Default: REJECT everything else
    return SafetyResult(
        allowed=False,
        reason=f"Operation not allowed. Only read operations and writes to {session_prefix}* objects are permitted."
    )
