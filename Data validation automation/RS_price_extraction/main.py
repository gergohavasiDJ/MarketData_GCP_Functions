import os
import pandas as pd
from google.cloud import bigquery
import glob
from google.oauth2 import service_account
import boto3
from botocore.config import Config
import json
from io import StringIO
import requests



def load_to_bigquery(df :pd,dataset_id, table_id, credentials):
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
    autodetect=False,  # Automatically detect the schema
    )
    # Load the DataFrame into BigQuery
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f'Successfully loaded {load_job.output_rows} rows into {dataset_id}.{table_id}')



def select_uniqueue_from_bigquery(dataset_id :str, table_id :str, columns : list,  credentials ,limit=None):
    """
    Retrieves specific columns from a BigQuery table and returns only uniqueue

    Args:
        :param dataset_id (str): BigQuery dataset ID.
        :param table_id (str): BigQuery table ID.
        :param columns (list): List of column names to retrieve.
        :param credentials: credentials of GCP service account
        :param limit (int, optional): Number of rows to retrieve. If None, retrieves all rows.

    Returns:
        pandas.DataFrame: A DataFrame containing the retrieved columns.
    """
    # Initialize BigQuery client
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Format the columns into a comma-separated string
    columns_str = ', '.join(columns)

    # Construct the query
    query = f"""
    SELECT distinct {columns_str}
    FROM `{credentials.project_id}.{dataset_id}.{table_id}`
    """
    if limit is not None:
        query += f" LIMIT {limit}"

    # Execute the query
    query_job = client.query(query)

    # Convert the result to a DataFrame
    results = query_job.result().to_dataframe()

    return results


def fileName_metadata(full_path : str,file_part : str):
    """
    Returns the required section of the selected file

    :param full_path: path to the file
    :param file_part: it returns either the filename without datetime information (FileName) or the datepart after the filename, without file extension (FileDate)
    :return: selected part of filepath in str.
    """
    output = ''

    last_slash_index = full_path.rfind('/')
    file_name = full_path[last_slash_index:]
    last_underscore_index = file_name.rfind('_')

    if file_part=='FileName':
        output = file_name[1:last_underscore_index]
    elif file_part=='FileDate':
        output = file_name[last_underscore_index+1:-4]
    else:
        output = ''
    
    return output    

#handle scenarios where file name does not follow standard naming conventions
def safe_parse_datetime(value):
    try:
        return pd.to_datetime(value, format='%Y%m%d%H%M%S')
    except Exception as e:
        print(f"Failed to parse datetime for value: {value} - Error: {e}")
        return None 

def list_folder_contents(bucket_name, folder_path, aws_access_key_id, aws_secret_access_key):
    """
    Lists the contents of a specific folder in an S3 bucket.

    :param bucket_name: Name of the S3 bucket
    :param folder_path: Path of the folder to list (e.g., "folder/subfolder/")
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :return: None
    """
    # Initialize a session using the provided credentials
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    # Access the S3 resource
    s3 = session.resource('s3')
    
    # Reference the bucket
    my_bucket = s3.Bucket(bucket_name)
    
    # Collect the object keys into a list
    object_keys = [obj.key for obj in my_bucket.objects.filter(Prefix=folder_path)]
    
    # Convert the list to a Pandas DataFrame
    df = pd.DataFrame(object_keys, columns=['ObjectKey'])

    #Adding further columns for filtering

    df['FileName'] = df['ObjectKey'].apply(lambda x: fileName_metadata(x,'FileName'))
    df['FileDateRaw'] = df['ObjectKey'].apply(lambda x: fileName_metadata(x,'FileDate'))
    #Converting to datetime to allow for filtering
    df['FileDate'] = df['FileDateRaw'].apply(safe_parse_datetime)
    
    return df


def ingest_file_from_s3(bucket_name, file_key, aws_access_key_id, aws_secret_access_key, env):
    """
    Downloads a selected file from S3 and ingests it into a Pandas DataFrame.

    :param bucket_name: Name of the S3 bucket
    :param file_key: Key of the file to download
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param env: Environment where the file is taken from
    :return: Pandas DataFrame containing the file data
    """
    # Initialize a session using the provided credentials
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    # Access the S3 client
    s3_client = session.client('s3')
    
    # Download the file content as a string
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read().decode('ISO-8859-1')
    
    # Load the file content into a Pandas DataFrame
    try:
        df = pd.read_csv(StringIO(file_content),sep='|' ,encoding='ISO-8859-1')
    except UnicodeDecodeError:
        # If 'ISO-8859-1' fails, try 'cp1252'
        df = pd.read_csv(StringIO(file_content), sep='|', encoding='cp1252')
    
    #Adding file metadata
    df['SourceEnv'] = env 
    df['FileName'] = fileName_metadata(file_key,'FileName')
    df['FileDate'] = fileName_metadata(file_key,'FileDate')
    df['repDate']  = pd.to_datetime('today')

    return df

def file_in_BigQuery(file_name : str, file_date : str, environment : str, dataset_id : str, table_id : str, credentials)->bool:
    """
    Checks if the selected file is already ingested into Bigquery given the environment.

    :param file_name: File name
    :param file_date: File date
    :param environment: Environment where the file is taken from
    :param dataset_id: Bigquery dataset ID
    :param table_id: Bigquery table name
    :param credentials: Bigquery authentication credentials
    :return: Boolean value, True if the file is already ingested
    """

    result_df = select_uniqueue_from_bigquery(dataset_id = dataset_id, table_id = table_id, columns =['FileName', 'FileDate','SourceEnv'],  credentials=credentials)
    
    #List of files to be excluded from insertion
    result_df = result_df[(result_df['FileName'] == file_name) & (result_df['FileDate'] == file_date) & (result_df['SourceEnv'] == environment)]

    if len(result_df) > 0:
        return True
    else:
        return False

#Adding Screener source files

def run_screen(screen_name : str, environment :str, **kwargs)-> pd.DataFrame:
    with open('Data_validation_google_sheet/config.json', 'r') as f:
        data = f.read()
    config = json.loads(data)


    # Authentication parameters - selecting environment
    if(environment=='STG'):
        url_base = config['URL_BASE_STG']
        ckey = config['C_KEY_STG']
    elif(environment=='PRD'):
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

    print(f"Constructed URL: {url}")

    # Set the headers
    headers = {
        'Dylan2010.EntitlementToken': ckey
    }

    try:
        # Perform the GET request
        response = requests.get(url, headers=headers)
        print(response)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx, 5xx)
    
        # Attempt to parse the JSON response
        try:
            data = response.json()
            print(data)
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



def run_batch_process(source_env :str):
    """
    Iterates through the S3 bucket in the selected environment and extracts the selected files. 
    Then the files are matched with the respective Bigquery table and inserted with a timestamp.
    :param source_env: Determines the source environment. (It can be STG or PROD)
    :return: None
    """
    try:

        # Define authentication parameters and environment
        #Read authentication parameters
        with open('RS_price_extraction/config.json', 'r') as f:
            data = f.read()
        config = json.loads(data)

        #S3 credentials
        bucket_name = config['S3_BUCKET_NAME']
        if source_env == 'STG':
            folder_path = config['S3_DEFAULT_PATH_STG']
        elif source_env == 'PROD':
            folder_path = config['S3_DEFAULT_PATH_PROD']
        else:
            return "Error: Environment not matching."
        
        aws_access_key_id = config['S3_ACCESS_KEY']
        aws_secret_access_key = config['S3_SECRET_KEY']

        #Define BigQuery parameters
        # Path to your service account key file
        key_path = 'RS_price_extraction/dj-ds-marketdata-nonprod-5b2c59fc4bff.json'
        # Load the credentials from the key file
        service_account_credentials = service_account.Credentials.from_service_account_file(key_path)
        #Dataset ID
        dataset_id = 'IBD_Automation'


        # List folder contents
        print('Starting to read files')
        folder_df = list_folder_contents(bucket_name, folder_path, aws_access_key_id, aws_secret_access_key)
        folder_df.to_csv('folder_list_30.csv')
        print('Finished reading files')
        #Apply conditions for file ingestion:

        #File and table configuration. If status is 0, the file is skipped from refresh.
        config_df = pd.read_csv('RS_price_extraction/file_config.csv')
        config_df['S3_file_name'] = config_df['S3_file_name']
        config_df['Bigquery_table'] = config_df['Bigquery_table']
        config_df['Active'] = pd.to_numeric(config_df['Active']) 
        
        #Keeping only files that are selected to be active by the user
        config_df = config_df.loc[config_df['Active']==1]

        #pick only selected files
        folder_df = folder_df[folder_df['FileName'].isin(config_df['S3_file_name'])]

        #Including files only after 2025-01-23
        folder_df = folder_df[folder_df['FileDate'] >= pd.Timestamp('2025-01-23')]

        #Where multiple files are available, pick only the latest one
        #Rationale: the last intraday file is closest in time of extraction to history files on the drive.
        #Additionally, the last intraday extract is the most consistent with history files
        
        #Add file date to support daily partitioning
        folder_df['FileDay'] = folder_df['FileDate'].dt.date

        #Including only the last file each day
        folder_df = folder_df.loc[folder_df.groupby(['FileName','FileDay'])['FileDate'].idxmax()]
        
        #Ensures that the script is only ingesting file from the previous day. To avoid picking up a midday file instead of the last file of the day
        folder_df = folder_df.loc[folder_df['FileDay'] != pd.Timestamp.today().normalize()]

        
        
        #Iterate over the list and insert them into right table
        for index, folder_row in folder_df.iterrows():

            for index, config_row in config_df.iterrows():
                #match the right files to the right tables
                if folder_row['FileName'] == config_row['S3_file_name']:
                    print(f"Path = {folder_row['ObjectKey']}, Filenme = {folder_row['FileName']}, Filedate = {folder_row['FileDate']}")
                    table_id = config_row['Bigquery_table']
                    
                    #Check if file is already in BigQuery
                    file_exists = file_in_BigQuery(file_name=folder_row['FileName'],file_date=folder_row['FileDateRaw'],environment=source_env,dataset_id=dataset_id,table_id=table_id,credentials=service_account_credentials)
                    
                    if file_exists is False:
                        print(f"Path = {folder_row['ObjectKey']}, Filenme = {folder_row['FileName']}, Filedate = {folder_row['FileDate']} DOESNT EXIST")
                        file_df = ingest_file_from_s3(bucket_name, folder_row['ObjectKey'], aws_access_key_id, aws_secret_access_key,source_env)
                        load_to_bigquery(file_df,dataset_id, table_id, service_account_credentials)
        
        #SCREENER:
        #Extracting the selected screen from Screener and transferring to BigQuery
        #TBD when data point is available
        '''
        optional_params = {
            "ExchangeID": "13"
        }
        screener_df=run_screen('DataStrategy.Gergo.IndustryCode','PRD',**optional_params)
        screener_df.to_csv('test_screener.csv')
        #NOTE: table to be defined
        load_to_bigquery(screener_df,dataset_id, table_id, service_account_credentials)
        '''
        result = "Job executed successfully"  # TODO: Add more details
        return {'response': result} 
               
    except Exception as e:
        # Handle any exception that occurs
        error_message = str(e)  # Convert the exception to a string message
        return {'response': error_message}
    
    

'''
def execute_batch(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    # run the function and pass the string
    return (run_batch_process('PROD'))

'''
#Calling the main execution
run_batch_process('PROD')

