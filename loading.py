import findspark
findspark.init()

from pyspark.sql import DataFrame

class DataLoader():
    def __init__(self, path_to_save: str, table_name: str) -> None:
        self._path_to_save = path_to_save
        self._table_name = table_name

    def _path_to_files(self) -> str:
        '''
        This method creates a string that provides a complete path to save files.
        '''
        complete_path = f'{self._path_to_save}/{self._table_name}' 
        return complete_path
    
    def create_delta_table(self, dataframe_to_save: DataFrame) -> None:
        '''
        This method creates a delta table using a dataframe as a data source.
        '''
        path_and_name_of_table = self._path_to_files()

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

    from manage_spark import ManagSpark
    from extraction import DataExtractor
    from transformation import DataTransformer

    # Extracting data from a public API
    extract_obj = DataExtractor(
        project_name = 'Currency daily quotation'
    )
    data = extract_obj.get_json_data(
        url = 'https://economia.awesomeapi.com.br/json/daily/EUR-BRL',
        num_days = 20
    )

    # Initializing a spark management instance
    manager_spark_obj = ManagSpark(
        app_name = 'Currency data collector',
        delta = True
    )
    spark_session = manager_spark_obj.start_spark()


    # Initializing a data transformer instance 
    data_transformer_obj = DataTransformer(
        short_description = 'Applying initial transformations in the raw and bronze layers'
    )
    raw_dataframe = data_transformer_obj.spark_df_using_list(
        data_list = data, 
        spark_session = spark_session)
    
    
    # Initializing a data loader instance to raw layer
    data_loader_obj_raw = DataLoader(
        path_to_save = '/home/welbert/projetos/spark/datalake/raw',
        table_name = 'currency_daily_quotation'
    )

    print(f'Path: {data_loader_obj_raw._path_to_save}')
    print(f'Table name: {data_loader_obj_raw._table_name}')

    # Testing delta table creation in a raw layer
    data_loader_obj_raw.create_delta_table(
        dataframe_to_save = raw_dataframe
    )

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


    # Initializing a data loader instance to bronze layer
    data_loader_obj_bronze = DataLoader(
        path_to_save = '/home/welbert/projetos/spark/datalake/bronze',
        table_name = 'currency_daily_quotation'
    )

    # Testing delta table creation in a bronze layer
    data_loader_obj_bronze.create_delta_table(
        dataframe_to_save = bronze_dataframe
    )

    bronze_dataframe.show()

    manager_spark_obj.stop_spark(spark_session)
