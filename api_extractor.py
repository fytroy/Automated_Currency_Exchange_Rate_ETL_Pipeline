import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account # To use our key file

# --- CONFIGURATION ---
# 1. Your Project ID
PROJECT_ID = "331982935310"

# 2. The name of our service account key file
KEY_PATH = "service_account_key.json"

# 3. The API endpoint URL
API_URL = "https://api.frankfurter.app/latest?from=USD"

# 4. BigQuery table details
#    (We'll create a new 'dataset' called 'currency_data'
#     and a 'table' called 'daily_rates')
DATASET_ID = "currency_data"
TABLE_ID = "daily_rates"
# ---------------------

def extract():
    """Extracts data from the API."""
    print(f"Fetching data from API: {API_URL}")
    try:
        response = requests.get(API_URL)
        response.raise_for_status() # Check for errors
        print("Data fetched successfully!")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def transform(data):
    """Transforms the JSON data into a clean Pandas DataFrame."""
    if data is None:
        return None

    print("Transforming data...")
    # Load the 'rates' dictionary into a DataFrame
    df = pd.DataFrame.from_dict(data['rates'], orient='index', columns=['Rate_vs_USD'])
    df.index.name = 'Currency_Code'

    # Convert the date string to a proper datetime object
    df['Date'] = pd.to_datetime(data['date'])

    # Reset the index to make 'Currency_Code' a regular column
    df = df.reset_index()

    print("Data transformed:")
    print(df.head())
    return df

def load(df, project_id, key_path, dataset_id, table_id):
    """Loads the DataFrame into Google BigQuery."""
    if df is None:
        print("No data to load.")
        return

    print(f"Loading data into BigQuery table {project_id}.{dataset_id}.{table_id}...")

    # 1. Create credentials from our key file
    credentials = service_account.Credentials.from_service_account_file(key_path)

    # 2. Use the pandas-gbq library to upload the DataFrame
    #    'if_exists='append'' will add new rows, not overwrite old ones.
    try:
        df.to_gbq(
            destination_table=f"{dataset_id}.{table_id}",
            project_id=project_id,
            credentials=credentials,
            if_exists='append'
        )
        print("Data loaded successfully to BigQuery!")

    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")

# This is the main function that runs our pipeline
def main():
    raw_data = extract()
    transformed_df = transform(raw_data)
    load(transformed_df, PROJECT_ID, KEY_PATH, DATASET_ID, TABLE_ID)

# This makes the script runnable from the command line
if __name__ == "__main__":
    main()