import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import json 
from datetime import datetime
from google.cloud import bigquery


# Path to the service account key JSON file
service_account_path = 'C:/Users/garla/Desktop/AEDAA/ML_AEDAA/service_acc.json'

# Initialize the app with a service account, granting admin privileges
cred = credentials.Certificate(service_account_path)
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://gsma7670c.asia-southeast1.firebasedatabase.app'  # Replace with your database URL
})

# Reference to the Firebase Realtime Database
ref = db.reference('/test')

# Example: Reading data from Firebase
snapshot = ref.get()

bigquery_client = bigquery.Client.from_service_account_json(service_account_path)

# # Print the retrieved data
output_list = list(snapshot.values())

def convert_to_bigquery_timestamp(datetime_str):
    try:
        # Parse the original datetime string
        parsed_datetime = datetime.strptime(datetime_str, '%y/%m/%d,%H:%M')
        # Convert to BigQuery timestamp format
        bigquery_timestamp = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')
        return bigquery_timestamp
    except ValueError:
        # Handle invalid datetime format
        return "1970-01-01 00:00:00"

# Convert the datetime value to BigQuery timestamp format
for item in output_list:
    if 'time' in item:
        item['cre_ts'] = convert_to_bigquery_timestamp(item['time'])
        item.pop("time")

json_output = json.dumps(output_list, ensure_ascii=False)
table_id = "gsma7670c.Firestore_data.firebase_data_1"
print(json_output)
def load_json_to_bigquery(json_data, table_id):
    # Convert JSON string to a list of dictionaries
    rows_to_insert = json.loads(json_data)
    
    # Insert rows into BigQuery table
    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)
    
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))




load_json_to_bigquery(json_output, table_id)

ref.set({})