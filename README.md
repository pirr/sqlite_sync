# sqlite_sync

Synchronize sqlite rows in a separate database without deleting.

Features:
- save new rows
- update existed rows

HOW TO USE:
```python
path_source_db = 'db.sqlite'
path_target_db = 'db_backup.sqlite'
target_db_alias = 'target_db'
check_name(target_db_alias)

with get_connection(path_source_db, path_target_db, target_db_alias) as conn:
    tables = get_all_tables(conn)
    backup_tables = get_references(conn, tables=tables)
    new_data = []

    for table in backup_tables:
        check_name(table)
        diff_rows = db_diff(
            conn=conn,
            table_name=table,
            target_db_alias=target_db_alias,
        )
    if diff_rows:
       new_data.append((table, diff_rows))

for table, rows in new_data[::-1]:
    if diff_rows:
        copy_rows(
            conn=conn,
            table_name=table,
            rows=rows,
            target_db_alias=target_db_alias,
       )

conn.commit()
```
