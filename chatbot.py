# Import necessary libraries
import os
import pandas as pd
import sqlite3
import re
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access environment variables
database_file = os.getenv('DATABASE_FILE')
csv_file = os.getenv('CSV_FILE')

# Create a database connection
def create_connection(db_file):
    print(f"Creating connection to database: {db_file}")
    conn = sqlite3.connect(db_file)
    return conn

# Create the short_positions table
def create_table(conn):
    print("Creating short_positions table if it doesn't exist.")
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS short_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country TEXT,
        position_holder TEXT,
        issuer TEXT,
        position_date TEXT,
        net_short_position REAL,
        orig_short_position REAL
    );
    ''')
    conn.commit()

# Load data from CSV into the database
def load_data(conn, csv_file):
    print(f"Loading data from CSV file: {csv_file}")
    df = pd.read_csv(csv_file, dtype=str)
    df['net_short_position'] = df['net_short_position'].astype(float)  # Ensure correct data type
    df.to_sql('short_positions', conn, if_exists='replace', index=False)
    print("Data loaded into database successfully.")

# Convert 'position_date' to datetime with error handling
def convert_dates(df):
    df['position_date'] = pd.to_datetime(df['position_date'], errors='coerce')
    print("Converted position_date to datetime.")
    return df

# Handle reporting timeliness queries
def get_reporting_timeliness(query, df):
    country = re.findall(r"in ([a-zA-Z ]+)", query, re.IGNORECASE)
    if not country:
        print("No country found in query for timeliness.")
        return "Could not extract country from query."
    
    country = country[0].strip().lower()  # Normalize case
    print(f"Querying timeliness for country: {country}")  # Debugging output
    result = df[df['country'].str.lower() == country]
    
    if result.empty:
        print(f"No data found for country: {country.title()}.")
        return f"No data found for {country.title()}."
    
    result = result[result['position_date'].notna()]
    
    if result.empty:
        print(f"No valid position dates found for country: {country.title()}.")
        return f"No valid position dates found for {country.title()}."
    
    timeliness = (pd.Timestamp.now() - result['position_date']).dt.days.mean()
    print(f"Average timeliness for {country.title()}: {timeliness:.2f} days.")
    
    return f"Short positions in {country.title()} are reported on average {timeliness:.2f} days ago."

# Handle sectoral insights queries
def get_sectoral_insights(query, df):
    countries = re.findall(r'compare (.+?) and (.+)', query, re.IGNORECASE)
    if not countries:
        print("No countries found in query for sectoral insights.")
        return "Please specify the countries to compare."
    
    country1, country2 = [c.strip().lower() for c in countries[0]]  # Normalize case
    print(f"Comparing {country1} with {country2} for sectoral insights.")  # Debugging output
    result1 = df[df['country'].str.lower() == country1]
    result2 = df[df['country'].str.lower() == country2]
    
    if result1.empty:
        print(f"No data found for country: {country1.title()}.")
        return f"No data found for {country1.title()}."
    if result2.empty:
        print(f"No data found for country: {country2.title()}.")
        return f"No data found for {country2.title()}."

    top_stock1 = result1.nlargest(1, 'net_short_position')
    top_stock2 = result2.nlargest(1, 'net_short_position')
    
    response = f"Sectoral Insights:\n{country1.title()}: {top_stock1['issuer'].values[0]} with {top_stock1['net_short_position'].values[0]}% short position\n"
    response += f"{country2.title()}: {top_stock2['issuer'].values[0]} with {top_stock2['net_short_position'].values[0]}% short position\n"
    
    return response

# Handle country-specific trends queries
def get_country_trend(query, df):
    country = re.findall(r"trend in ([a-zA-Z ]+)", query, re.IGNORECASE)
    if not country:
        print("No country found in query for trends.")
        return "Could not extract country from query."
    
    country = country[0].strip().lower()  # Normalize case
    print(f"Querying trend for country: {country}")  # Debugging output
    result = df[df['country'].str.lower() == country]
    
    if result.empty:
        print(f"No data found for country: {country.title()}.")
        return f"No data found for {country.title()}."
    
    trend_data = result[['position_date', 'net_short_position']].sort_values('position_date').tail(5)
    
    if trend_data.empty:
        print(f"No short positions found for the last 5 records in {country.title()}.")
        return f"Insufficient data for the last 5 records in {country.title()}."
    
    response = f"Trend of short positions in {country.title()}:\n"
    for index, row in trend_data.iterrows():
        response += f"{row['position_date'].date()}: {row['net_short_position']}% short position\n"
    
    return response

# Handle comparison between countries
def compare_short_positions(query, df):
    countries = re.findall(r'how does (.+?) compare to (.+)', query, re.IGNORECASE)
    if not countries:
        print("No countries found in query for comparison.")
        return "Please specify the countries to compare."
    
    country1, country2 = [c.strip().lower() for c in countries[0]]  # Normalize case
    print(f"Comparing short positions between: {country1} and {country2}")  # Debugging output
    result1 = df[df['country'].str.lower() == country1]
    result2 = df[df['country'].str.lower() == country2]
    
    if result1.empty:
        print(f"No data found for country: {country1.title()}.")
        return f"No data found for {country1.title()}."
    if result2.empty:
        print(f"No data found for country: {country2.title()}.")
        return f"No data found for {country2.title()}."
    
    top_stock1 = result1.nlargest(1, 'net_short_position')
    top_stock2 = result2.nlargest(1, 'net_short_position')
    
    response = f"Comparing short positions between {country1.title()} and {country2.title()}:\n"
    
    if not top_stock1.empty:
        response += f"{country1.title()} - {top_stock1.iloc[0]['issuer']}: {top_stock1.iloc[0]['net_short_position']}% short position\n"
    else:
        response += f"{country1.title()} has no short positions recorded.\n"
    
    if not top_stock2.empty:
        response += f"{country2.title()} - {top_stock2.iloc[0]['issuer']}: {top_stock2.iloc[0]['net_short_position']}% short position\n"
    else:
        response += f"{country2.title()} has no short positions recorded.\n"
    
    return response

# Handle country-specific queries (most shorted stocks)
def get_shorted_stocks(query, df):
    country = re.findall(r"in ([a-zA-Z ]+)", query, re.IGNORECASE)
    if not country:
        print("No country found in query for shorted stocks.")
        return "Could not extract country from query."
    
    country = country[0].strip().lower()  # Normalize case
    print(f"Querying most shorted stocks for country: {country}")  # Debugging output
    result = df[df['country'].str.lower() == country]
    
    if result.empty:
        print(f"No data found for country: {country.title()}.")
        return f"No data found for {country.title()}."
    
    top_stocks = result.nlargest(5, 'net_short_position')
    response = f"Most shorted stocks in {country.title()}:\n"
    for index, row in top_stocks.iterrows():
        response += f"{row['issuer']}: {row['net_short_position']}% short position\n"
    
    return response

# Main chatbot function
def chatbot(query, conn):
    print(f"Processing query: {query}")
    df = pd.read_sql_query("SELECT * FROM short_positions", conn)
    
    # Convert position_date to datetime
    df = convert_dates(df)

    # Handle various queries
    if "trend in" in query.lower():
        return get_country_trend(query, df)
    elif "compare" in query.lower() or "how does" in query.lower():
        return compare_short_positions(query, df)
    elif "timeliness" in query.lower():
        return get_reporting_timeliness(query, df)
    elif "most shorted stocks" in query.lower() or "shorted stocks" in query.lower():
        return get_shorted_stocks(query, df)
    elif "average reporting timeliness" in query.lower() or "reporting timeliness" in query.lower():
        return get_reporting_timeliness(query, df)
    else:
        return "I'm sorry, I didn't understand that. Please try again."

# Execution starts here
if __name__ == "__main__":
    # Create a database connection
    conn = create_connection(database_file)
    
    # Create table and load data if not already present
    create_table(conn)
    load_data(conn, csv_file)
    
    # questions to return responses
    sample_queries = [
        "In Germany, what are the most shorted stocks?",
        "Timeliness of short positions in UK?",
        "What is the average reporting timeliness for short positions in France?",
        "What are the most shorted stocks in Italy?",
        "How does Sweden compare to Norway",
        "What is the reporting timeliness in Germany?",
        "Show me the most shorted stocks in France."
    ]
    
    # Process sample queries and print the responses
    for query in sample_queries:
        response = chatbot(query, conn)
        print(f"Query: {query}\nResponse: {response}\n")
    
    # Close the database connection when done
    conn.close()
