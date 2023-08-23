from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def load_parquet_data(self, year):
        dataframes = []
        for i in range(2015, 2017):
            if i == 2018:
                continue
            print(f'Year: {i}')
            df = self.spark.read.option('inferschema', True).parquet(f'/content/taxi-trip-data/{i}/*.parquet')
            dataframes.append(df)
        return dataframes

    def create_dim_passenger(self, combined_df):
        #Creating passenger table
        dim_passenger=combined_df.select('passenger_count').\
                          withColumn("passenger_id",monotonically_increasing_id()).\
                          withColumn('passenger_count',col('passenger_count').cast('int')).\
                          select('passenger_id','passenger_count')
        return dim_passenger
        
    def create_dim_trip(self, combined_df):
        #Creating trip table
        dim_trip=combined_df.select('trip_distance','tpep_pickup_datetime','tpep_dropoff_datetime').\
                          withColumn("trip_distance_id",monotonically_increasing_id()).\
                          withColumn("trip_duration",round((unix_timestamp("tpep_dropoff_datetime")-unix_timestamp("tpep_pickup_datetime"))/60,2)).\
                          select('trip_distance_id','trip_distance','trip_duration')
        return dim_trip

    def create_dim_location(self, combined_df):
        #Creating location table
        dim_location=combined_df.select('PULocationID','DOLocationID').\
                          withColumn("location_id",monotonically_increasing_id()).\
                          withColumn('pickup_location',col('PULocationID').cast('int')).\
                          withColumn('dropoff_location',col('DOLocationID').cast('int')).\
                          select('location_id','pickup_location','dropoff_location')
        return dim_location


    def create_dim_rate(self, combined_df):
        #Creating rate table
        rate_code_type = {1:"Standard rate",2:"JFK",3:"Newark",4:"Nassau or Westchester",5:"Negotiated fare",6:"Group ride"}

        def map_rate_code(rate_code):
            return rate_code_type.get(rate_code,"Unknown")

        map_udf_rate=udf(map_rate_code,StringType())
        
        dim_rate=combined_df.select('RateCodeID').\
                      withColumn('rate_id',monotonically_increasing_id()).\
                      withColumnRenamed('RateCodeID','rate_code').\
                      withColumn('rate_code',col('rate_code').cast('int')).\
                      withColumn('rate_name',map_udf_rate(col('rate_code'))).\
                      select('rate_id','rate_code','rate_name')
        return dim_rate


    def create_dim_payment_type(self, combined_df):
        #Creating payment_type table
        payment_type = {1:"Credit card",2:"Cash",3:"No charge",4:"Dispute",5:"Unknown",6:"Voided trip"}

        def map_payment_code(payment_code):
            return payment_type.get(payment_code,"Unknown")
        
        map_udf_payment=udf(map_payment_code,StringType())
        
        dim_payment_type=combined_df.select('payment_type').\
                                    withColumn('payment_id',monotonically_increasing_id()).\
                                    withColumnRenamed('payment_type','payment_code').\
                                    withColumn('payment_type',map_udf_payment(col('payment_code'))).\
                                    select('payment_id','payment_code','payment_type')
        return dim_payment_type


    def create_dim_datetime(self, combined_df):
        #Creating datetime table
        dim_datetime=combined_df.select('tpep_pickup_datetime','tpep_dropoff_datetime',
                                year('tpep_pickup_datetime').alias('pickup_year'),
                                month('tpep_pickup_datetime').alias('pickup_month'),
                                dayofmonth('tpep_pickup_datetime').alias('pickup_day'),
                                dayofweek('tpep_pickup_datetime').alias('pickup_weekday'),
                                year('tpep_dropoff_datetime').alias('dropoff_year')
                                ,month('tpep_dropoff_datetime').alias('dropoff_month'),
                                dayofmonth('tpep_dropoff_datetime').alias('dropoff_day'),
                                dayofweek('tpep_dropoff_datetime').alias('dropoff_weekday')).\
                          withColumnRenamed('tpep_pickup_datetime','pickup_datetime').\
                          withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime').\
                          withColumn('datetime_id',monotonically_increasing_id()).\
                          select('datetime_id','pickup_datetime','pickup_year','pickup_month',
                                 'pickup_day','pickup_weekday','dropoff_datetime','dropoff_year',
                                 'dropoff_month','dropoff_day','dropoff_weekday')
        return dim_datetime



    def create_fact_table(self, combined_df, dim_passenger, dim_trip, dim_location, dim_rate, dim_payment_type, dim_datetime):
        # Creating the fact table using Spark SQL joins
        fact_table = combined_df.join(dim_passenger, on='passenger_count', how='left')\
            .join(dim_trip, on=['trip_distance', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'], how='left')\
            .join(dim_location, on=['PULocationID', 'DOLocationID'], how='left')\
            .join(dim_rate, on='RateCodeID', how='left')\
            .join(dim_payment_type, on='payment_type', how='left')\
            .join(dim_datetime, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime'], how='left')\
            .select('VendorID', 'datetime_id', 'passenger_id', 'trip_distance_id', 'rate_id', 'store_and_fwd_flag',
                    'location_id', 'payment_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge')
        return fact_table

