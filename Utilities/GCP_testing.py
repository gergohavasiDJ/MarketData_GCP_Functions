from google.cloud import bigquery
import pandas as pd
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build


# 1. Set the environment variable for credentials in Python
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "Utilities/dj-ds-marketdata-nonprod-5b2c59fc4bff.json"


def read_bq_external_table_to_df(
    project_id: str, 
    dataset_id: str, 
    table_id: str
) -> pd.DataFrame:
    """
    Reads all rows from a BigQuery EXTERNAL table into a pandas DataFrame
    by running a SELECT * query.
    """
    client = bigquery.Client(project=project_id)
    
    # Construct fully-qualified table name
    table_full_name = f"`{project_id}.{dataset_id}.{table_id}`"
    
    # Build a simple query against the external table
    query = f"SELECT * FROM {table_full_name}"
    
    # Run the query, then convert to DataFrame
    query_job = client.query(query)
    df = query_job.to_dataframe()
    
    return df

def read_sheet_to_dataframe(credentials_file, spreadsheet_id, range_name):
    """
    Reads data from a Google Sheet (via the Sheets API) into a pandas DataFrame.

    Args:
        credentials_file (str): Path to your service account JSON key file.
        spreadsheet_id (str): The Google Spreadsheet ID (the part of the URL after 'spreadsheets/d/').
        range_name (str): The sheet range to read (e.g. "Sheet1!A1:C100").

    Returns:
        pd.DataFrame: A DataFrame containing the sheet data.
    """
    # Define the required OAuth scope for read-only Sheets access
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    # 1. Create Credentials from the service account JSON key
    creds = service_account.Credentials.from_service_account_file(credentials_file, scopes=SCOPES)

    # 2. Build the Google Sheets API client
    service = build('sheets', 'v4', credentials=creds)

    # 3. Call the Sheets API to fetch values
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])  # List of lists

    if not values:
        print("No data found in the specified range.")
        return pd.DataFrame()

    # 4. Convert the 2D list to a DataFrame
    #    Assuming the first row is the header
    df = pd.DataFrame(values[1:], columns=values[0])
    return df

if __name__ == "__main__":
    # Example usage
    PROJECT_ID = "dj-ds-marketdata-nonprod"
    DATASET_ID = "MDS_Reporting"
    TABLE_ID   = "fact_jira_issues_current_rt"

    #df = read_bq_external_table_to_df(PROJECT_ID, DATASET_ID, TABLE_ID)
    #print("DataFrame shape:", df.shape)
    #print(df.head())

    SPREADSHEET_ID = "13DHio9dWwO4YyrqdX7YF9UvoW4uZc-Db6TT_qn-ykIg"

    # 2. The range, e.g., "Sheet1!A1:D" for columns A-D with dynamic row length
    RANGE_NAME = "Data!A1:M"

    # 3. Path to your service account JSON key
    SERVICE_ACCOUNT_FILE = "Utilities/dj-ds-marketdata-nonprod-5b2c59fc4bff.json"

    # Fetch the data into a DataFrame
    df = read_sheet_to_dataframe(SERVICE_ACCOUNT_FILE, SPREADSHEET_ID, RANGE_NAME)
    print(df.head())
