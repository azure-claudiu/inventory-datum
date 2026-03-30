import sqlite3
from typing import List, Dict

DB_PATH = "snowflake.db"

def fetch_table_data(tablename: str) -> List[Dict]:
    """Fetch all rows from the specified table as a list of dicts."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM "{tablename}"')
        rows = cur.fetchall()
        return [dict(row) for row in rows]
