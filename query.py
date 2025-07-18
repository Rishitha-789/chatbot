import sqlite3
import pandas as pd

# Function to create a database connection
def create_connection(db_file):
    print(f"Creating connection to database: {db_file}")
    conn = sqlite3.connect(db_file)
    return conn

# Function to fetch and print data for each specific question
def fetch_data(conn):
    # Question 1: Compare short positions between Sweden and Norway
    print("\n1. Short positions comparison between Sweden and Norway:")
    query1 = """
    SELECT * FROM short_positions
    WHERE country IN ('Sweden', 'Norway');
    """
    df1 = pd.read_sql_query(query1, conn)
    print(df1)

    # Question 2: Trend in short positions in Austria
    print("\n2. Trend in short positions in Austria:")
    query2 = """
    SELECT position_date, net_short_position, orig_short_position FROM short_positions
    WHERE country = 'Austria'
    ORDER BY position_date;
    """
    df2 = pd.read_sql_query(query2, conn)
    print(df2)

    # Question 3: Most shorted stocks in Italy
    print("\n3. Most shorted stocks in Italy:")
    query3 = """
    SELECT issuer, net_short_position FROM short_positions
    WHERE country = 'Italy'
    ORDER BY net_short_position DESC;
    """
    df3 = pd.read_sql_query(query3, conn)
    print(df3)

    # Question 4: Reporting timeliness in Germany
    print("\n4. Reporting timeliness in Germany:")
    query4 = """
    SELECT AVG(julianday('now') - julianday(position_date)) AS average_timeliness
    FROM short_positions
    WHERE country = 'Germany';
    """
    df4 = pd.read_sql_query(query4, conn)
    print(df4)

    # Question 5: Sectoral insights comparing Sweden and Finland
    print("\n5. Sectoral insights comparing Sweden and Finland:")
    query5 = """
    SELECT * FROM short_positions
    WHERE country IN ('Sweden', 'Finland');
    """
    df5 = pd.read_sql_query(query5, conn)
    print(df5)

# Main function to execute the script
def main():
    # Establish connection to the database
    database_file = 'short_positions.db'  # Update with your actual database file if necessary
    conn = create_connection(database_file)
    
    # Fetch and print the data
    fetch_data(conn)

    # Close the connection
    conn.close()

# Run the program
if __name__ == "__main__":
    main()
