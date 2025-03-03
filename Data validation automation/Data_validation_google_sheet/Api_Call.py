import requests
import pandas as pd
import json

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


optional_params = {
    "ExchangeID": "13"
}

def data_point_statistics(input_df : pd.DataFrame, data_point : str) -> dict:
    stats = {}

    # Count and unique-related statistics
    stats['count'] = int(input_df[data_point].count())
    stats['unique'] = int(input_df[data_point].nunique())
    stats['null_count'] = int(input_df[data_point].isnull().sum())
    stats['top'] = input_df[data_point].value_counts().idxmax() if not input_df[data_point].isnull().all() else None
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


pd.set_option('display.max_columns', None)

output_df=run_screen('DataStrategy.Gergo.IndustryCode','PRD',**optional_params)

print(output_df)

#print(data_point_statistics(output_df,'IndustryCode'))

#,ExchangeID='6'