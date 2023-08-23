from pyspark.sql import SparkSession
from data_ingestion.data_collector import DataCollector
from data_ingestion.link_extractor import LinkExtractor
from data_preprocessing.data_processor import DataProcessor
from data_preprocessing.utils import Utils
from data_storage.olap_data_uploader import DataWarehouseUploader



def main():
    # Initialize resources and parameters
    service_account_key_path = 'C:\Users\DELL\Desktop\projects\uber-complete-data-project\gothic-sylph-387906-b4e074b5ebb7.json'
    bucket_name = 'nyc-yellow-taxi-data'
    url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    year = '2017'  # Set to 'all' if processing all years

    # Initialize Data Collector and Link Extractor
    data_collector = DataCollector(service_account_key_path)
    links = LinkExtractor(year, url).extract_links()

    # Download data files and upload to GCS
    for year_num, link_list in links.items():
        for link in link_list:
            content = data_collector.fetch_data_from_url(link)
            if content is not None:
                blob_name = f'OLTP/{year_num}/{link.split("/")[4]}'
                data_collector.upload_to_gcs(bucket_name, blob_name, content)

    # Initialize Spark session
    spark = SparkSession.builder.master('local').appName('project').getOrCreate()

    # Initialize Data Processor and Data Warehouse Uploader
    data_processor = DataProcessor(spark)
    data_warehouse_uploader = DataWarehouseUploader(service_account_key_path)

    # Load and process data
    dataframes = data_processor.load_parquet_data(year)

    # Create and process dimension tables
    dim_passenger = data_processor.create_dim_passenger(dataframes[0])
    dim_trip = data_processor.create_dim_trip(dataframes[0])
    dim_location = data_processor.create_dim_location(dataframes[0])
    dim_rate = data_processor.create_dim_rate(dataframes[0])
    dim_payment_type = data_processor.create_dim_payment_type(dataframes[0])
    dim_datetime = data_processor.create_dim_datetime(dataframes[0])
    
    # Cache and broadcast tables
    dim_tables = {'dim_passenger': dim_passenger, 'dim_location': dim_location,
                  'dim_trip': dim_trip, 'dim_Rate': dim_rate, 'dim_payment_type': dim_payment_type,
                  'dim_datetime': dim_datetime}
    Utils.cache_and_broadcast_tables(spark, dim_tables)

     # Create the fact table using Spark SQL joins
    fact_table = data_processor.create_fact_table(dataframes[0], dim_passenger, dim_trip,
                                                  dim_location, dim_rate, dim_payment_type, dim_datetime)

    # Upload data to GCS
    dataframe_dict = {'dim_passenger': dim_passenger, 'dim_location': dim_location,
                      'dim_trip': dim_trip, 'dim_Rate': dim_rate, 'dim_payment_type': dim_payment_type,
                      'dim_datetime': dim_datetime, 'fact_table': fact_table}
    data_warehouse_uploader.upload_to_gcs(bucket_name, dataframe_dict)

if __name__ == "__main__":
    main()
