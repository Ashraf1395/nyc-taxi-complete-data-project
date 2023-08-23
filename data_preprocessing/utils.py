class Utils:
    @staticmethod
    def cache_and_broadcast_tables(spark, dim_tables):
        # Cache smaller dimension tables
        for dim_name, dim_df in dim_tables.items():
            dim_df.cache()
            print(f"Cached {dim_name} dataframe.")

        # Broadcast smaller tables
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # Disable auto-broadcast joins
        for dim_name, dim_df in dim_tables.items():
            dim_df.createOrReplaceTempView(dim_name)
            print(f"Broadcasting {dim_name} dataframe.")

    @staticmethod
    def uncache_tables(dim_tables):
        # Uncache previously cached dimension tables
        for dim_df in dim_tables.values():
            dim_df.unpersist()

