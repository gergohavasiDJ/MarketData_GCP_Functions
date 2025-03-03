import os
import json
import requests
import google.oauth2.id_token
import google.auth.transport.requests

# Set Google Application Credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'Data validation automation/RS_price_extraction/dj-ds-marketdata-nonprod-5b2c59fc4bff.json'

# Define the Cloud Run URL and audience
audience = 'https://us-central1-dj-ds-marketdata-nonprod.cloudfunctions.net/Transfer_Screener_data_to_Bigquery'

# Fetch the identity token
def get_id_token(audience_url):
    request = google.auth.transport.requests.Request()
    return google.oauth2.id_token.fetch_id_token(request, audience_url)

# Make the GET request
def invoke_cloud_run():
    # Fetch the token
    try:
        token = get_id_token(audience)
    except Exception as e:
        print(f"Error fetching ID token: {e}")
        return

    # Make the GET request
    try:
        response = requests.get(
            audience,
            headers={
                'Authorization': f"Bearer {token}",
                'Content-Type': 'application/json'
            }
        )
        # Print the response
        if response.status_code == 200:
            print("API responded successfully:")
            print(json.dumps(response.json(), indent=2))
        else:
            print(f"API call failed with status code {response.status_code}: {response.reason}")
            print("Response content:")
            print(response.text)
    except Exception as e:
        print(f"Error making the API call: {e}")

# Execute the function
if __name__ == "__main__":
    invoke_cloud_run()

