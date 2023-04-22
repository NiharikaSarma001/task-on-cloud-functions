import os
import json
import pandas as pd
from google.cloud import storage
from firebase_admin import credentials, initialize_app, db

# Set up Firebase Realtime Database credentials
cred = credentials.Certificate(r'D:\firebase_task\pandastask-cac90-firebase-adminsdk-wb8e4-c1f34b7753.json')
initialize_app(cred, {'databaseURL': 'https://pandastask-cac90-default-rtdb.firebaseio.com/'})

# Set up Google Cloud Storage credentials
storage_client = storage.Client.from_service_account_json(r'D:\firebase_task\pandastask-cac90-firebase-adminsdk-wb8e4-c1f34b7753.json')
bucket_name = 'pandastask-cac90'

file_path = "D:/firebase_task/sample_data.xlsx"
file_type = "xlsx"

def import_and_write_to_firebase(file_path, file_type):
    # Import data from Excel or CSV file
    if file_type == 'csv':
        df = pd.read_csv(file_path)
    elif file_type == 'xls' or file_type == 'xlsx':
        df = pd.read_excel(file_path)
    else:
        return 'Unsupported file type'
    
    # Convert data to JSON
    data_json = df.to_json(orient='records')
    data_json = json.loads(data_json)
    
    # Write data to Firebase Realtime Database
    ref = db.reference('https://pandastask-cac90-default-rtdb.firebaseio.com/')
    ref.set(data_json)
    
    # Create a folder in Firebase Storage and save the data as a JSON file
    bucket = storage_client.bucket(bucket_name)
    folder_name = 'my-folder/'
    folder_blob = bucket.blob(folder_name)
    folder_blob.upload_from_string('')
    file_name = 'data.json'
    file_blob = bucket.blob(folder_name + '/' + file_name)
    file_blob.upload_from_string(json.dumps(data_json))
    
    return 'Data imported and saved to Firebase'


def import_data(request):
    request_json = request.get_json()
    data = request_json['data']
    file_type = request_json['file_type']
    
    return import_and_write_to_firebase(data, file_type)



