import requests
import pandas as pd
import json
from google.cloud import bigquery
from google.oauth2 import service_account
import logging

def run_screen(screen_name : str, environment :str, config ,**kwargs)-> pd.DataFrame:
    """
    Retrive the selected screen from the given environment with optional parameters

    :param screen_name: Screener name
    :param environment: STG or PROD
    :param config: configuration file for API access
    :param **kwargs: optional parameters for screener
    :return: pd.DataFrame
    """

    # Authentication parameters - selecting environment
    if(environment=='STG'):
        url_base = config['URL_BASE_STG']
        ckey = config['C_KEY_STG']
    elif(environment=='PROD'):
        url_base = config['URL_BASE_PRD']
        ckey = config['C_KEY_PRD']
    else:
        url_base = config['URL_BASE_DEV']
        ckey = config['C_KEY_DEV']   

    # Construct the full URL
    url = f"{url_base}ckey={ckey}&ScreenNames={screen_name}"

    # Append optional parameters if they have a value
    for key, value in kwargs.items():
        if value is not None:  # Only include parameters with defined values
            url += f"&{key}={value}"

    #print(f"Constructed URL: {url}")

    # Set the headers
    headers = {
        'Dylan2010.EntitlementToken': ckey
    }

    try:
        # Perform the GET request
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx, 5xx)
    
        # Attempt to parse the JSON response
        try:
            data = response.json()
            # Check if the expected key exists in the JSON response
            if isinstance(data, list) and len(data) > 0 and 'QueryResults' in data[0]:
                df_results = pd.DataFrame(data[0]['QueryResults'])
            else:
                raise KeyError("The key 'QueryResults' was not found in the response.")
        except ValueError as json_err:
            print(f"Error parsing JSON: {json_err}")
            df_results = pd.DataFrame()  # Return an empty DataFrame as fallback

    except requests.exceptions.RequestException as req_err:
        print(f"HTTP Request error: {req_err}")
        df_results = pd.DataFrame()  # Return an empty DataFrame as fallback

    except KeyError as key_err:
        print(f"Key error: {key_err}")
        df_results = pd.DataFrame()  # Return an empty DataFrame as fallback
    return  df_results   
import pandas as pd
from google.cloud import bigquery

def convert_dataframe_types(df, dataset_id, table_id, credentials):
    """
    Dynamically converts DataFrame column types to match BigQuery table schema.

    :param df: Pandas DataFrame
    :param dataset_id: BigQuery dataset ID
    :param table_id: BigQuery table ID
    :param credentials: GCP service account credentials
    :return: Converted DataFrame
    """
    # Initialize BigQuery client
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Fetch BigQuery table schema
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # Get table details

    # Create a dictionary of column data types from BigQuery schema
    schema_dict = {field.name: field.field_type for field in table.schema}

    # Iterate through columns and convert dynamically
    for col in df.columns:
        if col in schema_dict:
            bq_type = schema_dict[col]

            if bq_type == "INTEGER":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")  # Convert to INTEGER
            elif bq_type == "FLOAT":
                df[col] = pd.to_numeric(df[col], errors="coerce")  # Convert to FLOAT
            elif bq_type == "DATE":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date  # Convert to DATE
            elif bq_type == "TIMESTAMP":
                df[col] = pd.to_datetime(df[col], errors="coerce")  # Convert to TIMESTAMP
            elif bq_type == "STRING":
                df[col] = df[col].astype(str)  # Convert to STRING

    return df  # Return the converted DataFrame


def load_to_bigquery(df :pd,dataset_id, table_id, history, credentials):
    """
    Inserts the provided dataframe into the selected BigQuery table

    :param dataset_id: source data extracted from S3
    :param table_id: destination table name
    :param history: determines if a snapshot is created or current data is overwritten
    :param credentials: credentials of GCP service account
    :return: None
    """
    # Initialize the BigQuery client with the credentials
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    #Check if history is required
    insert_method = bigquery.WriteDisposition.WRITE_APPEND if history == 1 else bigquery.WriteDisposition.WRITE_TRUNCATE

    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id) 
    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED, #Adds column names from DF if doesn't exist already
        write_disposition = insert_method,  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        source_format=bigquery.SourceFormat.CSV,
        #schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],  # Allow new fields
        autodetect=True,  # Automatically detect the schema
    )
    # Load the DataFrame into BigQuery
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    #print(f'Successfully loaded {load_job.output_rows} rows into {dataset_id}.{table_id}')
    
def run_batch_process():
    try:

        #Get screener authentication parameters
        with open('Screener_loader/config.json', 'r') as f:
            data = f.read()
        config = json.loads(data)

        #Get Screener list for ingestion
        screen_list_df = pd.read_csv('Screener_loader/screener_config.csv')
        #Define BigQuery parameters
        # Path to your service account key file
        key_path = 'Screener_loader/dj-ds-marketdata-nonprod-5b2c59fc4bff.json'
        # Load the credentials from the key file
        service_account_credentials = service_account.Credentials.from_service_account_file(key_path)
        df_temp = pd.DataFrame()

        for index, row in screen_list_df.iterrows():
            if(row['Active']==1):
                #Retrieve screener data
                screen= row['Screen_name']
                env = row['environment']
                dataset_id = row['Dataset_id']
                table_id = row['Bigquery_table']
                history = row['History']

                df_temp = run_screen(screen,env,config)

                #Replace spaces in column names as BigQuery doesn't support it
                df_temp.columns = df_temp.columns.str.strip().str.replace(' ', '_')
                df_temp.columns = df_temp.columns.str.strip().str.replace('%', 'Pct')

                df_temp['SourceEnv'] = env

                df_temp.dropna(how='all', inplace=True)

                #For reporting purposes, adding extraction timestamp
                df_temp['repDate']  = pd.to_datetime('today')

                df_temp.columns = df_temp.columns.astype(str)

                #Aligning data types with bigquery
                df_temp = convert_dataframe_types(df_temp, dataset_id, table_id, service_account_credentials)

                #Calling bigquery function and inserting to table
                load_to_bigquery(df_temp,dataset_id, table_id, history ,service_account_credentials)
                

        result = "Job executed successfully"  # TODO: Add more details and email alerts
        logging.info(result)
        print(result)
        return {'response': result}
    except Exception as e:
        # Handle any exception that occurs
        error_message = str(e)  # Convert the exception to a string message
        logging.error(error_message)
        print(error_message)
        return {'response': error_message}    

run_batch_process()             
            
'''
def execute_batch(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        If the execution is successful, it returns a positive message. If the execution failed, it return the error code.
        
    """

    # run the function and pass the string
    return (run_batch_process())
'''