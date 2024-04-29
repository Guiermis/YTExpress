import os
import pandas as pd
import json
import datetime
import time
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly']

API_SERVICE_NAME = 'youtubeAnalytics'
API_VERSION = 'v2'
CLIENT_SECRETS_FILE = r'C:\Users\guilherme.oliveira\Desktop\python womp womp\client_secret.json'
def get_service():
  flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
  credentials = flow.run_local_server()
  return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

def execute_api_request(client_library_function, **kwargs):
    try:
        response = client_library_function(
            **kwargs
        ).execute()

        return response
    except HttpError as e:
        print(f"An HTTP error occurred: {e}")
        return None
    

def fetch_channel_data(channel_id):
    # Initialize the YouTube Analytics service object
    youtubeAnalytics = get_service()
    
    # Execute the API request for the given channel ID
    result = execute_api_request(
        youtubeAnalytics.reports().query,
        ids=f"channel=={channel_id}",
        startDate='2023-03-01',
        endDate='2024-03-31',
        metrics='estimatedMinutesWatched,views,subscribersGained,subscribersLost,comments,likes,dislikes,shares',
        dimensions='day',
        sort='day'
    )
    return result

if __name__ == '__main__':
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    
    # Define your channel IDs here, with channel names as keys
    channel_ids = {
    'RICtv':'UCBiZg7ojHPU8ShY7s8xQ6bg',
    'RN24H':'UCR2vSUFSqUMy-sDJEdHeaYA',
    'RIC Rural':'UC0pjfx6qXTDcRlR9bcr_VYA',
    'Jovem Pan Grupo RIC': 'UCgTVofcrHPsDAadZkYraU6g',
    'RIC Entretenimento':'UCGfBE94turq_hyKMxB-Dfrg',
    'GRANDS':'UCs_YgpVHRgjOnKo9RvYfciA',
    'RICtv Londrina':'UC9GobOkoAvmuw3pXSjwQnIg',
    'RIC Podcasts':'UCkF5o3ktqXuEuEO6EZIOTOA',
    'RICtv Maringá':'UClG5ECaZCBQCzCpSqI7hWUA',
    'RIC Portal':'UCJ3gxlwoZMqKwOoqKrO-4dA',
    'RICtv Oeste':'UCDes47D0_xrzOm_r7b9RQAQ',
    }
    
    # Initialize an empty DataFrame
    final_df = pd.DataFrame()
    
    # Loop through each channel ID and fetch the data
    for channel_name, channel_id in channel_ids.items():
        print(f"Fetching data for {channel_name}")
        data = fetch_channel_data(channel_id)
        
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

    file_path = (r'C:\Users\guilherme.oliveira\Desktop\python womp womp\DADOS\RICytMAR2024.xlsx')

    final_df.to_excel(file_path, index=False)