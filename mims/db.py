from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Sequence

DEFAULT_RESOURCE_URI_TEMPLATE = "magnet:?xt=urn:btih:{info_hash}"

ALLOWED_SORT_COLUMNS = {
    "id": "id",
    "category": "category",
    "source_title": "source_title",
    "title": "title",
    "filesize_bytes": "filesize_bytes",
    "info_hash": "info_hash",
    "resource_link": "resource_link",
}

FILTERABLE_COLUMNS = {
    "id": "integer",
    "category": "text",
    "source_title": "text",
    "title": "text",
    "filesize_bytes": "integer",
    "info_hash": "text",
    "resource_link": "text",
}


def sanitize_text(value: str | None) -> str:
    return (value or "").encode("utf-8", "ignore").decode("utf-8", "ignore")


def sanitize_category(value: str | None) -> str:
    return sanitize_text(value).strip()


def build_resource_link(info_hash: str, template: str = DEFAULT_RESOURCE_URI_TEMPLATE) -> str:
    return template.format(info_hash=sanitize_text(info_hash))


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    return conn


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA cache_size = -100000;")


def _column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT '',
            source_title TEXT,
            filesize_bytes INTEGER NOT NULL,
            info_hash TEXT NOT NULL UNIQUE,
            resource_link TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS categories (
            name TEXT PRIMARY KEY COLLATE NOCASE
        );
        """
    )

    columns = _column_names(conn, "metadata_index")
    if "category" not in columns:
        conn.execute("ALTER TABLE metadata_index ADD COLUMN category TEXT")
    if "source_title" not in columns:
        conn.execute("ALTER TABLE metadata_index ADD COLUMN source_title TEXT")

    conn.execute("UPDATE metadata_index SET category = '' WHERE category IS NULL")

    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_metadata_category ON metadata_index(category);
        CREATE INDEX IF NOT EXISTS idx_metadata_source_title ON metadata_index(source_title);
        CREATE INDEX IF NOT EXISTS idx_metadata_title ON metadata_index(title);
        CREATE INDEX IF NOT EXISTS idx_metadata_filesize ON metadata_index(filesize_bytes);
        CREATE INDEX IF NOT EXISTS idx_metadata_hash ON metadata_index(info_hash);
        """
    )
    conn.commit()


def list_categories(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT name FROM categories ORDER BY name COLLATE NOCASE ASC").fetchall()
    return [str(row[0]) for row in rows]


def add_category_definition(conn: sqlite3.Connection, name: str) -> None:
    name = sanitize_category(name)
    if not name:
        raise ValueError("Category name cannot be empty.")
    conn.execute("INSERT OR IGNORE INTO categories(name) VALUES (?)", (name,))
    conn.commit()


def delete_category_definition(conn: sqlite3.Connection, name: str) -> int:
    name = sanitize_category(name)
    if not name:
        return 0
    cur = conn.execute(
        "UPDATE metadata_index SET category = '' WHERE LOWER(category) = LOWER(?)",
        (name,),
    )
    conn.execute("DELETE FROM categories WHERE LOWER(name) = LOWER(?)", (name,))
    conn.commit()
    return int(cur.rowcount)


def ensure_category_exists(conn: sqlite3.Connection, category: str) -> str:
    category = sanitize_category(category)
    if not category:
        return ""
    row = conn.execute("SELECT name FROM categories WHERE LOWER(name) = LOWER(?)", (category,)).fetchone()
    if row:
        return str(row[0])
    add_category_definition(conn, category)
    return category


def bulk_upsert_records(
    conn: sqlite3.Connection,
    records: Iterable[Sequence[object]],
    *,
    batch_size: int = 5000,
) -> int:
    sql = """
        INSERT INTO metadata_index (
            title,
            category,
            source_title,
            filesize_bytes,
            info_hash,
            resource_link
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(info_hash) DO UPDATE SET
            title = excluded.title,
            category = excluded.category,
            source_title = excluded.source_title,
            filesize_bytes = excluded.filesize_bytes,
            resource_link = excluded.resource_link
    """
    total = 0
    batch: list[Sequence[object]] = []
    conn.execute("BEGIN")
    try:
        for record in records:
            batch.append(record)
            if len(batch) >= batch_size:
                conn.executemany(sql, batch)
                total += len(batch)
                batch.clear()
        if batch:
            conn.executemany(sql, batch)
            total += len(batch)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return total


def add_record(
    conn: sqlite3.Connection,
    title: str,
    category: str,
    source_title: str | None,
    filesize_bytes: int,
    info_hash: str,
) -> int:
    title = sanitize_text(title).strip()
    source_title = sanitize_text(source_title).strip() or None
    info_hash = sanitize_text(info_hash).strip()
    category = ensure_category_exists(conn, category)
    cur = conn.execute(
        """
        INSERT INTO metadata_index (
            title,
            category,
            source_title,
            filesize_bytes,
            info_hash,
            resource_link
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            category,
            source_title,
            filesize_bytes,
            info_hash,
            build_resource_link(info_hash),
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def update_record(
    conn: sqlite3.Connection,
    record_id: int,
    title: str,
    category: str,
    source_title: str | None,
    filesize_bytes: int,
    info_hash: str,
) -> None:
    title = sanitize_text(title).strip()
    source_title = sanitize_text(source_title).strip() or None
    info_hash = sanitize_text(info_hash).strip()
    category = ensure_category_exists(conn, category)
    conn.execute(
        """
        UPDATE metadata_index
        SET title = ?,
            category = ?,
            source_title = ?,
            filesize_bytes = ?,
            info_hash = ?,
            resource_link = ?
        WHERE id = ?
        """,
        (
            title,
            category,
            source_title,
            filesize_bytes,
            info_hash,
            build_resource_link(info_hash),
            record_id,
        ),
    )
    conn.commit()


def assign_category(conn: sqlite3.Connection, record_ids: Sequence[int], category: str) -> int:
    if not record_ids:
        return 0
    category = ensure_category_exists(conn, category)
    placeholders = ", ".join("?" for _ in record_ids)
    params: list[object] = [category, *record_ids]
    cur = conn.execute(
        f"UPDATE metadata_index SET category = ? WHERE id IN ({placeholders})",
        params,
    )
    conn.commit()
    return int(cur.rowcount)


def delete_record(conn: sqlite3.Connection, record_id: int) -> None:
    conn.execute("DELETE FROM metadata_index WHERE id = ?", (record_id,))
    conn.commit()


def fetch_records(
    conn: sqlite3.Connection,
    *,
    search: str = "",
    search_mode: str = "any",
    search_field: str = "all",
    sort_column: str = "title",
    sort_desc: bool = False,
    limit: int = 200,
    offset: int = 0,
    filter_column: str = "",
    filter_operator: str = "",
    filter_value: str = "",
) -> list[sqlite3.Row]:
    sort_sql = ALLOWED_SORT_COLUMNS.get(sort_column, "title")
    sort_dir = "DESC" if sort_desc else "ASC"

    base_sql = """
        SELECT id, category, source_title, title, filesize_bytes, info_hash, resource_link
        FROM metadata_index
    """
    where_sql, params = _build_where_clauses(
        search=search,
        search_mode=search_mode,
        search_field=search_field,
        filter_column=filter_column,
        filter_operator=filter_operator,
        filter_value=filter_value,
    )
    if where_sql:
        base_sql += " WHERE " + " AND ".join(where_sql)

    base_sql += f" ORDER BY {sort_sql} {sort_dir}, id ASC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return conn.execute(base_sql, params).fetchall()


def count_records(
    conn: sqlite3.Connection,
    *,
    search: str = "",
    search_mode: str = "any",
    search_field: str = "all",
    filter_column: str = "",
    filter_operator: str = "",
    filter_value: str = "",
) -> int:
    sql = "SELECT COUNT(*) FROM metadata_index"
    where_sql, params = _build_where_clauses(
        search=search,
        search_mode=search_mode,
        search_field=search_field,
        filter_column=filter_column,
        filter_operator=filter_operator,
        filter_value=filter_value,
    )
    if where_sql:
        sql += " WHERE " + " AND ".join(where_sql)

    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def _build_where_clauses(
    *,
    search: str,
    search_mode: str,
    search_field: str,
    filter_column: str,
    filter_operator: str,
    filter_value: str,
) -> tuple[list[str], list[object]]:
    where_clauses: list[str] = []
    params: list[object] = []

    if search.strip():
        search_clause, search_params = _build_search_clause(search=search, search_mode=search_mode, search_field=search_field)
        if search_clause:
            where_clauses.append(search_clause)
            params.extend(search_params)

    column_name = ALLOWED_SORT_COLUMNS.get(filter_column)
    column_type = FILTERABLE_COLUMNS.get(filter_column)
    operator = (filter_operator or "").strip().lower()
    value = (filter_value or "").strip()

    if column_name and column_type and operator:
        clause, clause_params = _build_filter_clause(column_name, column_type, operator, value)
        if clause:
            where_clauses.append(clause)
            params.extend(clause_params)

    return where_clauses, params


def _build_search_clause(*, search: str, search_mode: str, search_field: str) -> tuple[str, list[object]]:
    words = [part for part in search.split() if part]
    if not words:
        return ("", [])

    normalized_mode = (search_mode or "any").strip().lower()
    joiner = " AND " if normalized_mode == "all" else " OR "

    normalized_field = (search_field or "all").strip().lower()
    allowed_fields = {
        "all": ("title", "source_title", "category"),
        "all fields": ("title", "source_title", "category"),
        "title": ("title",),
        "source_title": ("source_title",),
        "source title": ("source_title",),
        "category": ("category",),
    }
    selected_fields = allowed_fields.get(normalized_field, allowed_fields["all"])

    per_word_clauses: list[str] = []
    params: list[object] = []
    for word in words:
        pattern = _like_pattern(word)
        field_clauses = [f"{field} LIKE ? ESCAPE '\\'" for field in selected_fields]
        per_word_clauses.append("(" + " OR ".join(field_clauses) + ")")
        params.extend([pattern] * len(selected_fields))

    return ("(" + joiner.join(per_word_clauses) + ")", params)


def _build_filter_clause(column_name: str, column_type: str, operator: str, value: str) -> tuple[str, list[object]]:
    if operator == "is empty":
        return (f"({column_name} IS NULL OR TRIM(CAST({column_name} AS TEXT)) = '')", [])
    if operator == "is not empty":
        return (f"({column_name} IS NOT NULL AND TRIM(CAST({column_name} AS TEXT)) <> '')", [])

    if column_type == "integer":
        if value == "":
            return ("", [])
        try:
            numeric_value = int(value)
        except ValueError as exc:
            raise ValueError(f"Filter value for {column_name} must be an integer.") from exc
        if operator in {"=", "!=", "<", "<=", ">", ">="}:
            return (f"{column_name} {operator} ?", [numeric_value])
        return ("", [])

    if value == "":
        return ("", [])
    if operator == "contains":
        return (f"{column_name} LIKE ? ESCAPE '\\'", [_like_pattern(value)])
    if operator == "equals":
        return (f"{column_name} = ?", [value])
    if operator == "starts with":
        return (f"{column_name} LIKE ? ESCAPE '\\'", [_escape_like(value) + "%"])
    if operator == "ends with":
        return (f"{column_name} LIKE ? ESCAPE '\\'", ["%" + _escape_like(value)])
    return ("", [])


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _like_pattern(value: str, *, suffix: str = "%") -> str:
    escaped = _escape_like(value)
    return f"%{escaped}{suffix}"
