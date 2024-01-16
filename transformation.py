import findspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, first, lit, row_number

class Data_transformer():
    def __init__(self, short_description: str, layer: str = 'silver') -> None:
        self.short_description = short_description
        self.layer = layer

    def spark_df_using_list(self, data_list: list, spark_session: SparkSession) -> DataFrame:
        '''
        This method creates a Spark Dataframe, receiving a list of records and a Spark Session.
        '''
        spark_dataframe = spark_session.createDataFrame(data_list)
        return spark_dataframe
    
    def populate_rows_using_first_value(self, spark_dataframe: DataFrame, list_of_column_names: list) -> DataFrame:
        '''
        This method populates the rows of a Spark DataFrame using the value found in the first row of a specific column, receiving a Spark DataFrame and a list of column names.
        '''
        for column in list_of_column_names:
            first_row_value = spark_dataframe.select(first(column)).first()[0]
            spark_dataframe = spark_dataframe.withColumn(column, lit(first_row_value))
        return spark_dataframe


    def deduplicate_data(self, spark_dataframe: DataFrame, column_name_ref: str) -> DataFrame:
        '''
        This method deduplicate data using a specific column, receiving a Spark DataFrame and the column name as a string.
        '''
        window_spec = (
            Window
                .partitionBy('code', col(column_name_ref))
                .orderBy(col(column_name_ref).desc())
        )

        spark_dataframe = (
            spark_dataframe
                .withColumn('rank', row_number().over(window_spec))
                .filter(col('rank') == 1)
                .drop('rank')
                .orderBy(col(column_name_ref).desc())
        )

        return spark_dataframe
    

if __name__ == '__main__':
    from pyspark.sql.functions import to_timestamp, from_unixtime
    
    from manage_spark import Manag_spark
    from extraction import Data_extractor

    # Extracting data from a public API
    extract_obj = Data_extractor(
        project_name = 'Currency daily quotation'
    )

    data = extract_obj.get_json_data(
        url = 'https://economia.awesomeapi.com.br/json/daily/EUR-BRL',
        num_days = 20
    )

    # Initializing a spark management instance
    manager_spark_obj = Manag_spark(
        app_name = 'Data Engineering',
        delta = True
    )
    spark_session = manager_spark_obj.start_spark()

    # Initializing a data transformer instance 
    data_transformer_obj = Data_transformer(
        short_description = 'Applying initial transformations in the raw and bronze layers',
        layer = 'bronze'
    )

    raw_dataframe = data_transformer_obj.spark_df_using_list(
        data_list = data, 
        spark_session = spark_session)

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

    print(f'Short description: {data_transformer_obj.short_description}')
    print(f'Layer: {data_transformer_obj.layer}')
    bronze_dataframe.show()

    manager_spark_obj.stop_spark(spark_session)
