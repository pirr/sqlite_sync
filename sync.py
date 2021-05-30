"""
HOW TO USE:
>>> path_source_db = 'db.sqlite'
>>> path_target_db = 'db_backup.sqlite'
>>> target_db_alias = 'target_db'
>>> check_name(target_db_alias)

>>> with get_connection(path_source_db, path_target_db, target_db_alias) as conn:
>>>     tables = get_all_tables(conn)
>>>     backup_tables = get_references(conn, tables=tables)
>>>     new_data = []

>>>     for table in backup_tables:
>>>         check_name(table)
>>>         diff_rows = db_diff(
>>>             conn=conn,
>>>             table_name=table,
>>>             target_db_alias=target_db_alias,
>>>         )
>>>     if diff_rows:
>>>        new_data.append((table, diff_rows))

>>> for table, rows in new_data[::-1]:
>>>     if diff_rows:
>>>         copy_rows(
>>>             conn=conn,
>>>             table_name=table,
>>>             rows=rows,
>>>             target_db_alias=target_db_alias,
>>>        )

>>> conn.commit()

"""

import os
import re
import sqlite3
from contextlib import contextmanager
from typing import Any, List, Tuple, Optional


RE_REFERENCES = re.compile(r'references ["]*([A-z0-9]+)["]*', re.IGNORECASE)


def db_diff(
    conn: sqlite3.Connection,
    table_name: str,
    target_db_alias: str,
) -> List[Tuple[Any]]:
    """Compare the same tables in two databases and return new or updated rows from the source table.
    """
    source_table_columns = conn.execute("SELECT * FROM main.PRAGMA_TABLE_INFO(?);",
                                        (table_name,)).fetchall()
    target_table_columns = conn.execute(f"SELECT * FROM {target_db_alias}.PRAGMA_TABLE_INFO(?);",
                                        (table_name,)).fetchall()

    column_errors = []
    columns_only_in_source = set(source_table_columns) - set(target_table_columns)
    if columns_only_in_source:
        column_errors.append(f"Backup table has not columns: {', '.join(columns_only_in_source)}")

    columns_only_in_backup = set(target_table_columns) - set(source_table_columns)
    if columns_only_in_backup:
        column_errors.append(f"Backup table has extra columns: {', '.join(columns_only_in_backup)}")

    if column_errors:
        raise ValueError('\n'.join(column_errors))

    columns = ', '.join(c for _, c, *_ in source_table_columns)

    q = f"""
        SELECT {columns}
        FROM (
            SELECT 1 AS db, {columns} FROM {target_db_alias}.`{table_name}`
            UNION ALL
            SELECT 2 AS db, {columns} FROM main.`{table_name}`
        ) q1 
        group by {columns} having sum(db)=2;
    """

    diff_rows = conn.execute(q).fetchall()

    return diff_rows


def copy_rows(
    conn: sqlite3.Connection,
    table_name: str,
    rows: List[Tuple[Any]],
    target_db_alias: str,
):
    if not rows:
        raise ValueError("rows attribute is empty")

    placeholders = ', '.join('?' for _ in range(len(rows[0])))
    q = f"INSERT OR REPLACE INTO {target_db_alias}.`{table_name}` VALUES ({placeholders})"
    conn.executemany(q, rows)


def get_references(conn: sqlite3.Connection, tables: List[str]) -> List[str]:
    """Get target tables with reference tables in child to parent order
    """
    tables_copy = tables[:]
    references = []
    while tables_copy:
        on_ref_table = tables_copy.pop(0)

        if on_ref_table not in references:
            references.append(on_ref_table)

        sql = conn.execute("select sql from main.sqlite_master where tbl_name=?",
                           (on_ref_table,)).fetchone()
        ref_tables = RE_REFERENCES.findall(sql[0])

        for table in ref_tables:
            if table in references:
                references.sort(key=table.__eq__)
            elif table not in tables_copy:
                tables_copy.append(table)

            references.append(table)

    return references


def get_all_tables(conn: sqlite3.Connection):
    tables = conn.execute("SELECT name FROM main.sqlite_master WHERE type='table';").fetchall()
    return [t[0] for t in tables]


@contextmanager
def get_connection(
    path_source_db: str,
    path_target_db: str,
    target_db_alias: str,
) -> sqlite3.Connection:
    if not os.path.exists(path_source_db):
        raise FileNotFoundError(f"Database {path_source_db} not found")

    if not os.path.exists(path_target_db):
        raise FileNotFoundError(f"Database {path_target_db} not found")

    conn = sqlite3.connect(path_source_db)
    conn.execute("ATTACH DATABASE ? AS ?;", (path_target_db, target_db_alias))

    try:
        yield conn
    finally:
        conn.close()


def check_name(table_name: str):
    for symbol in table_name:
        if symbol.isalnum() or symbol.isdigit() or symbol in ('_', '-'):
            continue
        raise ValueError(f"You shouldn't use the table name - {table_name}")
