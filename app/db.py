import sqlite3
from contextlib import contextmanager

DB_FILE = "tickers.db"

def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS secret_tickers (
            secret TEXT NOT NULL,
            ticker TEXT NOT NULL,
            PRIMARY KEY (secret, ticker)
        )
    ''')
    conn.commit()
    conn.close()

def add_ticker(secret: str, ticker: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('INSERT OR IGNORE INTO secret_tickers (secret, ticker) VALUES (?, ?)', (secret, ticker))
        conn.commit()
    finally:
        conn.close()

def get_tickers(secret: str) -> list[str]:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT ticker FROM secret_tickers WHERE secret = ?', (secret,))
        rows = cursor.fetchall()
        return [row['ticker'] for row in rows]
    finally:
        conn.close()
