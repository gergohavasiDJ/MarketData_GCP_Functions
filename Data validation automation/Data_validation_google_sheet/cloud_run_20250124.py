import functions_framework
from flask import jsonify
import requests
import pandas as pd
import json

def run_screen(screen_name : str, environment :str, **kwargs)-> pd.DataFrame:
    with open('config.json', 'r') as f:
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

'''
COMPLEX USECASES
This section includes functions that perform advanced analysis, where Screener functionality
is not sufficient to carry out validation.
The functions must be named after the respective JIRA ticket.
'''
#Test function to multiply numeric columns
def JIRA_123(input_df: pd.DataFrame) -> pd.DataFrame:
    num_cols = input_df.select_dtypes(include=["number"]).columns
    output_df = input_df
    output_df[num_cols] *= 2
    return output_df

#Return field statistics of the selected target variable
def data_point_statistics(input_df : pd.DataFrame, data_point : str) -> dict:
    transformed_df = input_df[data_point].convert_dtypes()
    results = transformed_df.describe().to_dict()
    return results

# This function is the entry point
@functions_framework.http
def data_validation(request):
    """Calls the Screener screen based on provided input parameters and returns the output as JSON format."""
    
    try:
        # Capture data from the POST request body
        request_data = request.get_json()
        
        # Extracting the expected parameters
        screen_name = request_data.get("ScreenNames", "DefaultScreen")
        screen_name = screen_name.strip()

        jira_ticket = request_data.get("JIRA_ticket", "NotProvided")
        jira_ticket = jira_ticket.upper().strip()

        environment = request_data.get("Environment", "Test")

        data_point = request_data.get("Data_point", "Not Provided")

        # Extract optional parameters dynamically
        optional_params = {
            key: value
            for key, value in request_data.items()
            if key not in {"ScreenNames", "JIRA_ticket", "Environment"}
        }

        # Call the run_screen function with the provided parameters
        result_df = run_screen(screen_name,environment,**optional_params)

        #Function to decide if the special usecase is required
        if jira_ticket == 'JIRA-123':
            result_df = JIRA_123(result_df)
        else:
            pass

        # Convert the result dataframe to JSON format
        result_json = result_df.to_json(orient='split')
        #result_json = result_df.to_json(orient='records')
        result_data = json.loads(result_json)  # Convert string to actual JSON object

        # Generate contextual data
        if data_point in result_df.columns and data_point != 'Not Provided':
            contextual_data = data_point_statistics(result_df,data_point)
        elif  data_point == 'Not Provided':
            contextual_data = None
        else:    
            contextual_data = {"error": f"Data point '{data_point}' not found in input dataframe."}

        # Return the results as JSON
        return jsonify({
            "core_data": result_data,
            "contextual_data": contextual_data
        })

    except Exception as e:
        # Error handling
        return jsonify({"error": str(e)}), 400




