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


def load_to_bigquery(df :pd.DataFrame,dataset_id :str, table_id :str, credentials):
    """
    Inserts the provided dataframe into the selected BigQuery table

    :param dataset_id: source data extracted from S3
    :param table_id: destination table name
    :param credentials: credentials of GCP service account
    :return: None
    """
    # Initialize the BigQuery client with the credentials
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id) 
    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,  # Automatically detect the schema
    )
    # Load the DataFrame into BigQuery
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    #print(f'Successfully loaded {load_job.output_rows} rows into {dataset_id}.{table_id}')

def iterative_load(exchangelist :list,screen_name : str, environment : str, screener_parameter : str,config)-> pd.DataFrame:
    """
    The inventory table is iteretavily loaded to avoid system overload. This function iterates over the provided exchange list,
    extracts data from screener for each and returns the results in a single dataframe.

    :param exchangelist: list of exchange IDs that the script iterates over
    :param screen_name: name of screen used for iterative extraction
    :param environment: Screener environment (PROD or STG)
    :param screener_parameter: screener parameters to pass to the API call
    :param config: screener login config
    :return: dataframe with the union of all extracts
    """

    full_extract_df = None

    for exchangeID in exchangelist:
        temp_param = {
           screener_parameter : exchangeID
        }
        temp_df = run_screen(screen_name,environment,config,**temp_param)

        if not temp_df.empty:
            if full_extract_df is None:
                full_extract_df = temp_df  # Initialize with the first valid DataFrame
            else:
                full_extract_df = pd.concat([full_extract_df, temp_df], ignore_index=True)
        print(f"processed exchangeID: {exchangeID}")
    return full_extract_df

# Function to clean text: removes newlines, trims spaces, and replaces problematic characters
def clean_text(value):
    """
    Helper function to clean new lines and extra spaces that would stop Bigquery ingestion

    :param value: dataframe cell value to be cleaned
    :return: cleaned value in the same data type
    """
    if isinstance(value, str):  # Only apply to strings
        return value.replace("\n", " ").replace("\r", " ").strip()  # Remove newlines and extra spaces
    return value

def select_top_date_from_bigquery(dataset_id :str, table_id :str, column : str,  credentials):
    """
    Retrieves the maximum date of a selected table 

    Args:
        :param dataset_id (str): BigQuery dataset ID.
        :param table_id (str): BigQuery table ID.
        :param columns (list): List of column names to retrieve.
        :param credentials: credentials of GCP service account

    Returns:
        pandas.DataFrame: A DataFrame containing the retrieved date.
    """
    # Initialize BigQuery client
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Construct the query
    query = f"""
    SELECT max({column}) 
    FROM `{credentials.project_id}.{dataset_id}.{table_id}`
    """

    # Execute the query
    query_job = client.query(query)

    # Convert the result to a DataFrame
    results = query_job.result().to_dataframe()

    return results

def run_batch_process():
    try:
        print('Process started')
        #Get screener authentication parameters
        with open('Screener inventory/config.json', 'r') as f:
            data = f.read()
        config = json.loads(data)

        #Get Screener list for ingestion
        screen_list_df = pd.read_csv('Screener inventory/screener_config.csv')
        #Define BigQuery parameters
        # Path to your service account key file
        key_path = 'Screener inventory/dj-ds-marketdata-nonprod-5b2c59fc4bff.json'
        # Load the credentials from the key file
        service_account_credentials = service_account.Credentials.from_service_account_file(key_path)
        df_temp = pd.DataFrame()

        for index, row in screen_list_df.iterrows():
            if(row['Active']==1):
                #Retrieve screener config data
                screen= row['Screen_name']
                env = row['environment']
                dataset_id = row['Dataset_id']
                table_id = row['Bigquery_table']
                Iterative_load = row['Iterative_load']
                Param_name = row['Param_name']
                print(f"Processing: {screen}")

                # CRON does not support biweekly schedules. 
                # Therefore the batch is scheduled weekly, and it is checked inside the script how many days have passed since the last refresh
                # If less than 13 days, the extraction is not executed.

                max_date = select_top_date_from_bigquery(dataset_id, table_id, 'repDate' ,service_account_credentials).iloc[0, 0]
                delta = pd.Timestamp.now()- max_date
                tables_updated_ct = 0
                
                if(delta.days>=13):
                    print("Execute")
                    if Iterative_load==0:
                        df_temp = run_screen(screen,env,config)
                        print(f"{screen} extracted successfully")
                    else:
                        Param_values = row['Param_values']
                        #Extracting the list of Exchange IDs that are required to be pasted into each call as a parameter
                        exchange_df = run_screen(Param_values,env,config)
                        exchange_list = exchange_df.iloc[:,0].to_list()

                        df_temp = iterative_load(exchange_list,screen,env,Param_name,config)

                        print("Successfully processed iterative extract")
                        
                    df_temp.dropna(how='all', inplace=True)
                    # Apply to all string columns in the DataFrame
                    df_temp = df_temp.apply(lambda col: col.map(clean_text) if col.dtype == "object" else col)
                    
                    #Replace spaces in column names as BigQuery doesn't support it
                    df_temp.columns = df_temp.columns.str.strip().str.replace(' ', '_')

                    df_temp['SourceEnv'] = env

                    #For reporting purposes, adding extraction timestamp
                    df_temp['repDate']  = pd.to_datetime('today')

                    if df_temp is None or df_temp.empty:
                        print("df_temp is empty before adding SourceEnv")
                    

                    #Calling bigquery function and inserting to table
                    load_to_bigquery(df_temp,dataset_id, table_id, service_account_credentials)

                    tables_updated_ct = tables_updated_ct +1
                else:
                    print('skip')

                

        if(tables_updated_ct>0):
            result = f"Job executed successfully. {tables_updated_ct} tables were refreshed"
        else:
            result = "Job executed successfully, but no tables were refreshed, due to update frequency rules"

        print(result)
        logging.log(result)
        return {'response': result}
    
    
    except Exception as e:
        # Handle any exception that occurs
        error_message = str(e)  # Convert the exception to a string message
        logging.error(error_message)
        print(error_message)
        return {'response': error_message}    

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

run_batch_process()