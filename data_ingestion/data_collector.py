import requests
from google.cloud import storage

class DataCollector:
    def __init__(self, service_account_key_path):
        self.client = storage.Client.from_service_account_json(service_account_key_path)

    def fetch_data_from_url(self, url):
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch data from {url}")
            return None
        return response.content

    def upload_to_gcs(self, bucket_name, blob_name, content):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content)
        print(f"Uploaded {blob_name} to {bucket_name}")
