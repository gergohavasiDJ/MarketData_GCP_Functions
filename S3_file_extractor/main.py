import pandas as pd
import boto3
from botocore.config import Config
import json
from io import StringIO
import functions_framework
from flask import jsonify
import pandas as pd
import json
from collections import OrderedDict #Required to maintain order fields in statistics
import numpy as np

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

def list_folder_contents(bucket_name, folder_path, aws_access_key_id, aws_secret_access_key, file_name):
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
    s3_client = session.client('s3')

    # List objects using list_objects_v2 (efficient)
    objects = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=folder_path, ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

        # Extract matching keys
        if 'Contents' in response:
            objects.extend([
                obj['Key'] for obj in response['Contents'] if file_name in obj['Key']
            ])

        # Check if there are more results
        if response.get('IsTruncated'):  
            continuation_token = response['NextContinuationToken']
        else:
            break
    # Convert results into a DataFrame
    df = pd.DataFrame(objects, columns=['ObjectKey'])

    #Adding further columns for filtering

    df['FileName'] = df['ObjectKey'].apply(lambda x: fileName_metadata(x,'FileName'))
    df['FileDateRaw'] = df['ObjectKey'].apply(lambda x: fileName_metadata(x,'FileDate'))
    #Converting to datetime to allow for filtering
    df['FileDate'] = pd.to_datetime(df['FileDateRaw'], format='%Y%m%d%H%M%S')

    return df

'''
#Return field statistics of the selected target variable
def data_point_statistics(input_df : pd.DataFrame, data_point : str) -> dict:
    stats = {}

    # Count and unique-related statistics
    stats['count'] = int(input_df[data_point].count())
    stats['unique'] = int(input_df[data_point].nunique())
    stats['null_count'] = int(input_df[data_point].isnull().sum())
    top_value = input_df[data_point].value_counts().idxmax() if not input_df[data_point].isnull().all() else None
    if isinstance(top_value, (np.int64, np.float64)): #Handle int64 values, that cannot be JSONified
        top_value = int(top_value)
    stats['top'] = top_value
    stats['freq'] = int(input_df[data_point].value_counts().iloc[0]) if not input_df[data_point].isnull().all() else None

    # Attempt numeric conversion and calculate numeric statistics if possible
    try:
        numeric_column = pd.to_numeric(input_df[data_point], errors='raise')
        stats['negative_values'] = int((numeric_column < 0).sum())
        stats['positive_values'] = int((numeric_column >= 0).sum())
    except ValueError:
        stats['negative_values'] = None
        stats['positive_values'] = None
    return stats

'''

#Return field statistics of the selected target variable
def data_point_statistics(input_df : pd.DataFrame, data_point : list) -> pd.DataFrame:
    stats_df = pd.DataFrame()

    for field in data_point:
        print(field)

        # Create a single-row dataframe for the current field
        temp_df = pd.DataFrame({
            'field_name': [field],
            'count': [int(input_df[field].count())],
            'unique': [int(input_df[field].nunique())],
            'null_count': [int(input_df[field].isnull().sum())],
        })
        print(temp_df)
        # Determine most frequent value and its frequency
        top_value = input_df[field].value_counts().idxmax() if not input_df[field].isnull().all() else None
        if isinstance(top_value, (np.int64, np.float64)):  # Handle int64 values that cannot be JSONified
            top_value = int(top_value)

        temp_df['top'] = [top_value]
        temp_df['freq'] = [int(input_df[field].value_counts().iloc[0]) if not input_df[field].isnull().all() else None]

        # Try converting column to numeric
        try:
            numeric_column = pd.to_numeric(input_df[field], errors='raise')
            temp_df['negative_values'] = [(numeric_column < 0).sum()]
            temp_df['positive_values'] = [(numeric_column >= 0).sum()]
        except ValueError:
            temp_df['negative_values'] = [None]
            temp_df['positive_values'] = [None]
        
        stats_df = pd.concat([stats_df , temp_df], ignore_index=True)

    #transposing columns to rows to match destination format.
    stats_df = stats_df.set_index('field_name').T
    stats_df.reset_index(inplace=True)
    stats_df.rename(columns={'index':'Measures'},inplace=True)

    return stats_df

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
    
    return df

def run_batch_process(source_env :str, selected_file : str, exact_match :bool)->pd.DataFrame:
    """
    Queries the S3 bucket in the selected environment and extracts the selected file. 
    Then the selected file is returned in Dataframe format.
    :param source_env: Determines the source environment. (It can be STG or PRD)
    :param selected_file: File selected by the user.
    :param exact_match: if True is selected, the user defines the filename explicitly. If False then only the core file name is defined, and the most recent file is returned.
    :return: selected file as DataFrame or None if the file is not found
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
        elif source_env == 'PRD':
            folder_path = config['S3_DEFAULT_PATH_PROD']
        else:
            return "Error: Environment not matching."
        
        aws_access_key_id = config['S3_ACCESS_KEY']
        aws_secret_access_key = config['S3_SECRET_KEY']


        #Check if exact matching is required and filter files accordingly
        if exact_match:
            try:
                file_df = ingest_file_from_s3(bucket_name, f'{folder_path}{selected_file}' , aws_access_key_id, aws_secret_access_key,source_env)
                return file_df
            
            except Exception as e:
                error_message = str(e) 
                return pd.DataFrame({'Error': [f"File was not found. '{error_message}'"]})    
            
        else:
            folder_df = list_folder_contents(bucket_name, folder_path, aws_access_key_id, aws_secret_access_key, selected_file)
            #pick only the latest file
            folder_df = folder_df.sort_values(by='FileDate', ascending=False, ).iloc[[0]]
            #Check if any match is found
            if folder_df.empty:
                return None
            else:
                file_df = ingest_file_from_s3(bucket_name, str(folder_df['ObjectKey'].iloc[0]), aws_access_key_id, aws_secret_access_key,source_env)
                return file_df
               
    except Exception as e:
        # Handle any exception that occurs
        error_message = str(e)  # Convert the exception to a string message
        return pd.DataFrame({'Error': [f"File was not found. Make sure the file name is correct and check if you selected exact matching, you really provided the full filename. Detailed error message:: '{error_message}'"]})
    
result_df = run_batch_process(source_env = 'PRD', selected_file ='wonW_WONDB_HSFINST3MRSRATING_20250128210422.csv', exact_match = True)
result_df.to_csv('test_extract.csv')


contextual_data = data_point_statistics(result_df,['Osid','I3MRSrk'])
#contextual_data_df= pd.Series(contextual_data, name='Osid')
contextual_data.to_csv('data_point_statistics.csv')


'''
# This function is the entry point
@functions_framework.http
def extract_file(request):
    """Calls the S3 bucket based on provided input parameters and returns the output as JSON format."""
    
    try:
        # Capture data from the POST request body
        request_data = request.get_json()
        
        # Extracting the expected parameters
        file_name = request_data.get("FileName", "EmptyFileName")
        file_name = file_name.strip()

        environment = request_data.get("Environment", "STG")

        exact_match = request_data.get("ExactMatch", False) 

        data_point = request_data.get("Data_point", "Not Provided")

        result_df = run_batch_process(source_env = environment, selected_file =file_name, exact_match = exact_match)

        # Convert the result dataframe to JSON format
        result_json = result_df.to_json(orient='split')
        result_data = json.loads(result_json)  # Convert string to actual JSON object

        # Generate contextual data
        if data_point in result_df.columns and data_point != 'Not Provided':
            contextual_data = OrderedDict(data_point_statistics(result_df,data_point))

        elif  data_point == 'Not Provided':
            contextual_data = None
        else:    
            contextual_data = {"error": f"Data point '{data_point}' not found in input dataframe."}

        return jsonify({
            "core_data": result_data,
            "contextual_data": contextual_data
        })
    except Exception as e:
        # Error handling
        return jsonify({"error": str(e)}), 400
'''