import os
import pandas as pd
from google.cloud import bigquery
import glob
from google.oauth2 import service_account
import boto3
from botocore.config import Config
import json
from io import StringIO
import logging
# Enable debug logging
#logging.basicConfig(level=logging.DEBUG)

#Importing credentials
# Path to your service account key file
key_path = '/Users/gergo.havasi/Library/CloudStorage/GoogleDrive-gergo.havasi@dowjones.com/My Drive/JavaScript exmple/dj-ds-marketdata-nonprod-5b2c59fc4bff.json'

# Load the credentials from the key file
service_account_credentials = service_account.Credentials.from_service_account_file(key_path)

# Path to the directory containing your Excel files
csv_directory = '/Users/gergo.havasi/Documents/Projects/IBD automation'

def read_csv_file(file_path):
    df = pd.read_csv(file_path)
    return df

def load_to_bigquery(df,dataset_id, table_id, credentials):
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
    


def import_file(csv_directory,file_name):
    file_path = f'{csv_directory}/{file_name}'
    # Attempt to read the CSV file with 'ISO-8859-1' encoding
    try:
        df = pd.read_csv(file_path,sep='|' ,encoding='ISO-8859-1')
    except UnicodeDecodeError:
        # If 'ISO-8859-1' fails, try 'cp1252'
        df = pd.read_csv(file_path, sep='|', encoding='cp1252')

    base_name = os.path.basename(file_path)
    last_underscore_index = base_name.rfind('_')

    df['FileName'] = base_name[:last_underscore_index]
    df['FileDate'] = base_name[last_underscore_index+1:-4]
    df['repDate']  = pd.to_datetime('today')
    return df


def insert_file():
    # Define your dataset and table
    dataset_id = 'IBD_Automation'
    table_id = 'fact_S3_Secmaster_st'

    for file_path in glob.glob(f'{csv_directory}/*.csv'):
        base_name = os.path.basename(file_path)
        # Find the position of the last underscore
        last_underscore_index = base_name.rfind('_')

        if last_underscore_index != -1:
            # Extract the part before the last underscore
            core_file_name = base_name[:last_underscore_index]
            file_date = base_name[last_underscore_index+1:-4]
        else:
            # Handle cases where there is no underscore
            core_file_name = base_name

        if core_file_name == 'wonW_WONDB_Secmaster':
            print(f'Processing {base_name}...')
            output_df = import_file(csv_directory,base_name)
            load_to_bigquery(output_df,dataset_id, table_id, service_account_credentials)
        elif core_file_name == 'wonW_WONDB_HSFINST3MRSRATING':
            print(f'Processing {base_name}...')
            output_df = import_file(csv_directory,base_name)
            load_to_bigquery(output_df,dataset_id, table_id, service_account_credentials)
        #This needs to be finished

#insert_file()


#S3 credentials
with open('RS_price_extraction/config.json', 'r') as f:
    data = f.read()
config = json.loads(data)



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
    df['FileDate'] = df['ObjectKey'].apply(lambda x: fileName_metadata(x,'FileDate'))
    #Converting to datetime to allow for filtering
    df['FileDate'] = pd.to_datetime(df['FileDate'], format='%Y%m%d%H%M%S')

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

def run_batch_process(source_env :str, selected_files : list):
    """
    Iterates through the S3 bucket in the selected environment and extracts the selected files. 
    Then the files are matched with the respective Bigquery table and inserted with a timestamp.
    :param source_env: Determines the source environment. (It can be STG or PROD)
    :param selected_files: Determines the files to be ingested. NOTE: if more files are added, the code and the database has to be manually modified.
    :return: None
    """
    # Define authentication parameters and environment
    bucket_name = config['S3_BUCKET_NAME']
    if source_env == 'STG':
        folder_path = config['S3_DEFAULT_PATH_STG']
    elif source_env == 'PROD':
        folder_path = config['S3_DEFAULT_PATH_PROD']
    else:
        return "Error: Environment not matching."
    
    aws_access_key_id = config['S3_ACCESS_KEY']
    aws_secret_access_key = config['S3_SECRET_KEY']

    # List folder contents
    folder_df = list_folder_contents(bucket_name, folder_path, aws_access_key_id, aws_secret_access_key)

    #Apply conditions for file ingestion:
    #pick only selected files
    folder_df = folder_df[folder_df['FileName'].isin(selected_files)]

    #Including files only after 2025-01-23
    folder_df = folder_df[folder_df['FileDate'] >= pd.Timestamp('2025-01-23')]

    #Where multiple files are available, pick only the latest one
    #TBD!!!!!

    #Iterate over the list and insert them into right table
    for index, row in folder_df.iterrows():

        #match the right files to the right tables
        if row['FileName'] == 'wonW_WONDB_Secmaster':
            print(f"Path = {row['ObjectKey']}, Filenme = {row['FileName']}, Filedate = {row['FileDate']}")
            file_df = ingest_file_from_s3(bucket_name, row['ObjectKey'], aws_access_key_id, aws_secret_access_key,source_env)
            #load_to_bigquery(file_df,dataset_id, table_id, service_account_credentials)

        elif row['FileName'] == 'wonW_WONDB_HSFINST3MRSRATING':
            print(f"Path = {row['ObjectKey']}, Filenme = {row['FileName']}, Filedate = {row['FileDate']}")
            file_df = ingest_file_from_s3(bucket_name, row['ObjectKey'], aws_access_key_id, aws_secret_access_key,source_env)
            #load_to_bigquery(file_df,dataset_id, table_id, service_account_credentials)

        elif row['FileName'] == 'wonW_WONDB_HSFINST6MRSRATING':
            print(f"Path = {row['ObjectKey']}, Filenme = {row['FileName']}, Filedate = {row['FileDate']}")    
            file_df = ingest_file_from_s3(bucket_name, row['ObjectKey'], aws_access_key_id, aws_secret_access_key,source_env)
            #load_to_bigquery(file_df,dataset_id, table_id, service_account_credentials)

    # Step 2: Select a file to ingest
    selected_file = 'williamoneilco/licensed-feedextract/wonW_WONDB_Secmaster_20250123210148.csv'

    # Step 3: Ingest the selected file
    file_df = ingest_file_from_s3(bucket_name, selected_file, aws_access_key_id, aws_secret_access_key,source_env)


#Calling the main execution
selected_files = ['wonW_WONDB_Secmaster','wonW_WONDB_HSFINST3MRSRATING', 'wonW_WONDB_HSFINST6MRSRATING']
run_batch_process('STG',selected_files)




