import pandas as pd
from datetime import datetime

# Load the dataset
df = pd.read_csv('data/book_data.csv', dtype=str)

# Check for missing data
def check_missing_data(df):
    missing_data_report = df.isnull().sum()
    print("Missing Data Report:")
    print(missing_data_report)
    print()

# Check the data types of each column
def check_data_types(df):
    print("Data Types:")
    print(df.dtypes)
    print()

# Check for invalid date formats
def check_invalid_dates(df, date_columns):
    for column in date_columns:
        print(f"Checking for invalid dates in {column}...")
        invalid_dates = df[~df[column].apply(lambda x: is_valid_date(x))]
        if not invalid_dates.empty:
            print(f"Invalid dates found in column {column}:")
            print(invalid_dates[[column]])
        else:
            print(f"All dates in {column} are valid.")
        print()

# Helper function to check if a date string is valid
def is_valid_date(date_string):
    if isinstance(date_string, str):
        try:
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    return False  # Return False if the value is not a string

# Check for duplicates in the dataset
def check_duplicates(df):
    print("Checking for duplicate rows...")
    duplicate_rows = df[df.duplicated()]
    if not duplicate_rows.empty:
        print(f"Found {len(duplicate_rows)} duplicate rows:")
        print(duplicate_rows)
    else:
        print("No duplicate rows found.")
    print()

# Inspect unique values in country column
def check_unique_countries(df):
    print("Unique Countries in the Dataset:")
    print(df['country'].unique())
    print()

# Inspect unique values in sector column
def check_unique_sectors(df):
    if 'sector' in df.columns:
        print("Unique Sectors in the Dataset:")
        print(df['sector'].unique())
        print()
    else:
        print("Sector column not found in the dataset.")

# Check for missing or invalid net short positions
def check_net_short_positions(df):
    print("Checking for missing or invalid net short positions...")
    # Convert to numeric, coercing errors to NaN
    df['net_short_position'] = pd.to_numeric(df['net_short_position'], errors='coerce')
    # Check for missing or invalid (negative) net short positions
    invalid_positions = df[df['net_short_position'].isnull() | (df['net_short_position'] < 0)]
    if not invalid_positions.empty:
        print(f"Found {len(invalid_positions)} invalid or missing net short positions:")
        print(invalid_positions[['net_short_position']])
    else:
        print("All net short positions are valid.")
    print()

# Run all diagnostic checks
def run_diagnostics(df):
    check_missing_data(df)
    check_data_types(df)
    check_invalid_dates(df, ['position_date', 'reporting_date'])
    check_duplicates(df)
    check_unique_countries(df)
    check_unique_sectors(df)
    check_net_short_positions(df)

# Run diagnostics on the dataset
run_diagnostics(df)
