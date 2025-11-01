import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from datetime import date, timedelta  # We need these to loop through dates
import time  # We need this to pause

# --- CONFIGURATION ---
# All of this is identical to our last script
PROJECT_ID = "331982935310"
KEY_PATH = "service_account_key.json"
DATASET_ID = "currency_data"
TABLE_ID = "daily_rates"

# --- NEW: BACKFILL CONFIGURATION ---
# Let's backfill 1 year of data.
# You can change this to 5 years (5 * 365) if you want!
DAYS_TO_BACKFILL = 365

# The base URL for the historical API
API_BASE_URL = "https://api.frankfurter.app"
# ---------------------

# --- TRANSFORM and LOAD Functions (Copied from our other script) ---
# These functions are identical. No changes needed.

def transform(data):
    """Transforms the JSON data into a clean Pandas DataFrame."""
    if data is None:
        return None

    # Load the 'rates' dictionary into a DataFrame
    df = pd.DataFrame.from_dict(data['rates'], orient='index', columns=['Rate_vs_USD'])
    df.index.name = 'Currency_Code'

    # Convert the date string to a proper datetime object
    df['Date'] = pd.to_datetime(data['date'])

    # Reset the index to make 'Currency_Code' a regular column
    df = df.reset_index()
    return df

def load(df, project_id, key_path, dataset_id, table_id):
    """Loads the DataFrame into Google BigQuery."""
    if df is None:
        print("No data to load.")
        return

    credentials = service_account.Credentials.from_service_account_file(key_path)

    try:
        pandas_gbq.to_gbq(
            df,  # DataFrame comes first
            destination_table=f"{dataset_id}.{table_id}",
            project_id=project_id,
            credentials=credentials,
            if_exists='append'
        )
        print("  ...Data loaded successfully to BigQuery!\n")

    except Exception as e:
        print(f"  ...Error loading data to BigQuery: {e}\n")

# --- NEW: Main Backfill Logic ---

def backfill():
    """Loops through past dates, extracts, transforms, and loads data."""

    # Get today's date
    end_date = date.today()

    # Calculate the start date (e.g., 365 days ago)
    start_date = end_date - timedelta(days=DAYS_TO_BACKFILL)

    print(f"Starting backfill from {start_date} to {end_date}...")

    current_date = start_date

    # Loop from the start date up to today
    while current_date <= end_date:
        # Format the date as 'YYYY-MM-DD' for the API
        date_str = current_date.strftime("%Y-%m-%d")

        # 1. EXTRACT
        api_url = f"{API_BASE_URL}/{date_str}?from=USD"
        print(f"Fetching data for {date_str}...")

        try:
            response = requests.get(api_url)
            response.raise_for_status() # Check for errors
            raw_data = response.json()

            # 2. TRANSFORM
            transformed_df = transform(raw_data)

            # 3. LOAD
            if transformed_df is not None:
                load(transformed_df, PROJECT_ID, KEY_PATH, DATASET_ID, TABLE_ID)

        except requests.exceptions.RequestException as e:
            print(f"  ...Error fetching data: {e} (Skipping date)")
        except Exception as e:
            print(f"  ...An unexpected error occurred: {e} (Skipping date)")

        # IMPORTANT: Be a good citizen. Don't spam the API.
        # Wait for 0.2 seconds between each request.
        time.sleep(0.2)

        # Move to the next day
        current_date += timedelta(days=1)

    print("Backfill complete!")

# This makes the script runnable from the command line
if __name__ == "__main__":
    backfill()