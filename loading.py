import findspark
findspark.init()

from pyspark.sql import DataFrame

class Data_loader():
    def __init__(self) -> None:
        print('---------- Initializing the loader instance  ----------')

    def path_to_files(self, path_to_save: str, table_name: str) -> str:
        """
        This method creates a string that provides a complete path to save files.
        """
        complete_path = f'{path_to_save}/{table_name}' 
        return complete_path
    
    def export_data(self, dataframe_to_save: DataFrame, path_and_name_of_table: str) -> None:
        """
        This method exports data, taking a Spark Dataframe and a path along with the name of a table as a string.
        """
        (
            dataframe_to_save
                .write
                .option('compression', 'snappy')
                .format('parquet')
                .mode('overwrite')
                .save(path_and_name_of_table)
        )

        print(f'The table was saved on to the following path: {path_and_name_of_table}')
        return None
    
    def create_delta_table(self, dataframe_to_save: DataFrame, path_and_name_of_table: str) -> None:
        """
        This method creates a delta table using a dataframe as a data source and a string path as a target.
        """
        (
            dataframe_to_save
                .write
                .format('delta')
                .mode('overwrite')
                .save(path_and_name_of_table)
        )

        return None
    

if __name__ == '__main__':

    from pyspark.sql.functions import col, to_timestamp, from_unixtime

    from manage_spark import Manag_spark
    from extraction import Data_extractor
    from transformation import Data_transformer

    # Extracting data from a public API
    extract_obj = Data_extractor()
    data = extract_obj.get_json_data(
        url = 'https://economia.awesomeapi.com.br/json/daily/EUR-BRL',
        num_days = 20
    )

    # Initializing a spark management instance
    manager_spark_obj = Manag_spark()
    spark_session = manager_spark_obj.start_spark(
        app_name = 'Currency data collector',
        delta = True     
    )


    # Initializing a data transformer instance 
    data_transformer_obj = Data_transformer()
    raw_dataframe = data_transformer_obj.spark_df_using_list(
        data_list = data, 
        spark_session = spark_session)
    
    
    # Initializing a data loader instance
    data_loader_obj = Data_loader()

    # Saving data in a raw layer of the datalake
    path_to_raw_layer = data_loader_obj.path_to_files(
        path_to_save = '/home/welbert/projetos/spark/datalake/raw',
        table_name = 'currency_daily_quotation'
    )

    print(f'Path to raw data: {path_to_raw_layer}')

    # Testing delta table creation in a raw layer
    data_loader_obj.create_delta_table(
        dataframe_to_save = raw_dataframe,
        path_and_name_of_table = path_to_raw_layer
    )

    # Testing export data to a raw layer
    # data_loader_obj.export_data(
    #     dataframe_to_save = raw_dataframe,
    #     path_and_name_of_table = path_to_raw_layer
    # )

    # Transforming the data to save in bronze layer
    bronze_dataframe = (
        data_transformer_obj
            .populate_rows_using_first_value(
                spark_dataframe = raw_dataframe, 
                list_of_column_names = ['code', 'codein', 'name']
            )
    )


    bronze_dataframe = (
        bronze_dataframe
            .drop('create_date')
            .withColumn('ask', col('ask').cast('double'))
            .withColumn('bid', col('bid').cast('double'))
            .withColumn('high', col('high').cast('double'))
            .withColumn('low', col('low').cast('double'))
            .withColumn('pctChange', col('pctChange').cast('double'))
            .withColumn('varBid', col('varBid').cast('double'))
            .withColumn('datetime', from_unixtime('timestamp'))
            .withColumn('datetime', to_timestamp('datetime', 'yyyy-MM-dd HH:mm:ss'))
            .withColumn('date', col('datetime').cast('date'))
    )
    
    bronze_dataframe = (
        data_transformer_obj
            .deduplicate_data(
                spark_dataframe = bronze_dataframe,
                column_name_ref = 'date'
            )
    ).drop('date')

    path_to_bronze_layer = data_loader_obj.path_to_files(
        path_to_save = '/home/welbert/projetos/spark/datalake/bronze',
        table_name = 'currency_daily_quotation'
    )

    # Testing export data to a bronze layer
    # data_loader_obj.export_data(
    #     dataframe_to_save = bronze_dataframe,
    #     path_and_name_of_table = path_to_bronze_layer
    # )

    # Testing delta table creation in a bronze layer
    data_loader_obj.create_delta_table(
        dataframe_to_save = bronze_dataframe,
        path_and_name_of_table = path_to_bronze_layer
    )

    bronze_dataframe.show()

    manager_spark_obj.stop_spark(spark_session)
