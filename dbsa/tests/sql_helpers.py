"""Helpers for comparing generated SQL in tests."""

import textwrap


def norm_block(sql):
    """Strip leading indentation and outer blank lines (readable multiline asserts)."""
    return textwrap.dedent(sql).strip()


def norm_ws(sql):
    """Collapse all whitespace to single spaces for stable full-string SQL asserts."""
    return " ".join(sql.split())


def one_line(sql):
    """Single line, trimmed; useful for embedding expected fragments."""
    return norm_ws(sql).strip()
