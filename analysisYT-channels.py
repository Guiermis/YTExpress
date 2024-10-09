import os
import pandas as pd
import json
import time
import google.oauth2.credentials
import google_auth_oauthlib.flow
import pickle
import gspread
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2 import service_account


#calculate the date corresponding to the last two days and transforms it into fixed report
report_day = datetime.today() - timedelta(days=3)
formatted_date = report_day.strftime('%Y-%m-%d')

SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly']

scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
json_file = "credentials.json"

def login():
    credentials = service_account.Credentials.from_service_account_file(json_file)
    scoped_credentials = credentials.with_scopes(scopes)
    gc = gspread.authorize(scoped_credentials)
    return gc

# This function finds the first empty row
def first_empty_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))  # Assumes first column has no empty values in between
    return str(len(str_list)+1)


API_SERVICE_NAME = 'youtubeAnalytics'
API_VERSION = 'v2'
CLIENT_SECRETS_FILE = r'C:\Users\guilherme.oliveira\Desktop\python womp womp\client_secret.json'
def get_service(channel_name):
    creds = None
    # Each channel has its unique token file based on its name
    credentials_path = f'C:\\Users\\guilherme.oliveira\\Desktop\\python womp womp\\pickles\\token_{channel_name}.pickle'

    # Load the existing credentials if they exist
    if os.path.exists(credentials_path):
        with open(credentials_path, 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open(credentials_path, 'wb') as token:
            pickle.dump(creds, token)

    return build(API_SERVICE_NAME, API_VERSION, credentials=creds)

def execute_api_request(client_library_function, **kwargs):
    try:
        response = client_library_function(
            **kwargs
        ).execute()

        return response
    except HttpError as e:
        print(f"An HTTP error occurred: {e}")
        return None
    

def fetch_channel_data(youtubeAnalytics, channel_id):
    # Use the provided YouTube Analytics service object for API requests
    try:
        response = youtubeAnalytics.reports().query(
            ids=f"channel=={channel_id}",
            startDate=formatted_date,
            endDate=formatted_date,
            metrics='estimatedMinutesWatched,views,subscribersGained,subscribersLost,comments,likes,dislikes,shares',
            dimensions='day',
            sort='day'
        ).execute()

        return response
    except HttpError as e:
        print(f"An HTTP error occurred: {e}")
        return None

if __name__ == '__main__':
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    
    # Define your channel IDs here, with channel names as keys
    c = open('channel_ids.json')
    channel_ids = json.load(c)
    
    # Initialize an empty DataFrame
    final_df = pd.DataFrame()
    
    # Loop through each channel ID and fetch the data
    for channel_name, channel_id in channel_ids.items():
        print(f"Fetching data for {channel_name}")
        youtubeAnalytics = get_service(channel_name)  # Pass channel_name to get_service
        data = fetch_channel_data(youtubeAnalytics, channel_id)  # Pass service object and channel ID
        
        # Your approach to handling the API response
        columns = [data['columnHeaders'][i]['name'] for i in range(len(data['columnHeaders']))]
        temp_df = pd.DataFrame(data["rows"], columns=columns)
        temp_df['channel_name'] = channel_name  # Add channel name
        temp_df['watch_hours'] = temp_df['estimatedMinutesWatched'] / 60
        # Append the temporary DataFrame to the final DataFrame
        final_df = pd.concat([final_df, temp_df], ignore_index=True)
    
        time.sleep(2)
    # Now 'final_df' contains all the data fetched, and you can process or save it as needed
    print(final_df)

    gc = login()
    planilha = gc.open("RICyt")
    aba = planilha.worksheet("analytics")
    first_row = first_empty_row(aba)

    # Convert DataFrame to a list of lists (without headers)
    data_to_insert = final_df.values.tolist()

    # Update the worksheet starting from the first empty row
    aba.update(range_name=f'A{first_row}', values=data_to_insert)

    file_path = (r'C:\Users\guilherme.oliveira\Desktop\python womp womp\DADOS\BAGULHO.xlsx')

    final_df.to_excel(file_path, index=False)