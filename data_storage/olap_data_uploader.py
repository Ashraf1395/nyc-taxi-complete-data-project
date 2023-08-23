from google.cloud import storage

class DataWarehouseUploader:
    def __init__(self, service_account_key_path):
        self.client = storage.Client.from_service_account_json(service_account_key_path)
    
    def upload_to_gcs(self, bucket_name, dataframe_dict):
        for name, df in dataframe_dict.items():
            print(f'Started writing {name}')
            df.write.parquet(f'{name}.parquet')

            # Upload fact and dimension tables
            df.write.parquet(f"gs://{bucket_name}/OLAP/{name}.parquet")
            print(f'Uploaded {name} to Bucket {bucket_name}')
