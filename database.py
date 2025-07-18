import sqlite3
import pandas as pd

def create_connection(db_file):
    conn = sqlite3.connect(db_file)
    return conn

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS short_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country TEXT,
        position_holder TEXT,
        issuer TEXT,
        isin TEXT,
        position_date TEXT,
        reporting_date TEXT,
        net_short_position REAL,
        orig_short_position REAL
    );
    ''')
    conn.commit()

def load_data(conn, csv_file):
    # Ensure all columns are read as strings to avoid mixed dtype warnings
    df = pd.read_csv(csv_file, dtype=str)
    df['net_short_position'] = df['net_short_position'].astype(float)
    df.to_sql('short_positions', conn, if_exists='replace', index=False)

def query_data(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

if __name__ == '__main__':
    conn = create_connection('short_positions.db')
    create_table(conn)
    load_data(conn, 'data/book data.csv')
