import pandas as pd
from datetime import datetime

# Load the dataset
df = pd.read_csv('data/book_data.csv', dtype=str)

# Helper function to check if a date string is valid
def is_valid_date(date_string):
    if isinstance(date_string, str):
        try:
            datetime.strptime(date_string, '%m/%d/%Y')  # Adjust to MM/DD/YYYY format
            return True
        except ValueError:
            return False
    return False  # Return False if the value is not a string

# Convert dates to a consistent format
def clean_dates(df, date_columns):
    for column in date_columns:
        df[column] = pd.to_datetime(df[column], errors='coerce', format='%m/%d/%Y')  # Adjust format
    return df

# Fill or drop missing data
def handle_missing_data(df):
    # Drop rows where critical columns are missing
    df = df.dropna(subset=['country', 'position_holder', 'issuer', 'position_date', 'net_short_position'])
    # Drop 'isin' column (too many missing values)
    if 'isin' in df.columns:
        df = df.drop(columns=['isin'])
    if 'reporting_date' in df.columns:
        df = df.drop(columns=['reporting_date'])  # You can keep reporting_date if necessary.
    return df

# Remove duplicate rows
def remove_duplicates(df):
    df = df.drop_duplicates()
    return df

# Handle invalid net short positions
def clean_net_short_positions(df):
    df['net_short_position'] = pd.to_numeric(df['net_short_position'], errors='coerce')
    # Drop rows with invalid or missing net_short_position
    df = df.dropna(subset=['net_short_position'])
    return df

# Main function to clean the dataset
def clean_dataset(df):
    # Clean dates
    df = clean_dates(df, ['position_date', 'reporting_date'])
    
    # Handle missing data
    df = handle_missing_data(df)
    
    # Remove duplicate rows
    df = remove_duplicates(df)
    
    # Clean net short positions
    df = clean_net_short_positions(df)
    
    return df

# Clean the dataset
cleaned_df = clean_dataset(df)

# Check if the cleaned dataset is empty
if cleaned_df.empty:
    print("Warning: The cleaned dataset is empty. Please check the cleaning process.")
else:
    # Save the cleaned dataset
    cleaned_df.to_csv('data/cleaned_short_positions.csv', index=False)
    print("Data cleaning complete. Cleaned dataset saved as 'cleaned_short_positions.csv'.")
