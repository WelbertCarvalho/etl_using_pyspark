import findspark
findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, first, lit, from_unixtime, to_timestamp

from manage_spark import Manag_spark
from extraction import Data_extractor

class Data_loader():
    def __init__(self) -> None:
        print('---------- Inicializing the loader instance ----------')

    def spark_df_using_list(self, data_list: list, spark_session: SparkSession) -> DataFrame:
        spark_dataframe = spark_session.createDataFrame(data_list)
        return spark_dataframe
    
    def populate_rows_using_first_value(self, spark_dataframe: DataFrame, list_of_column_names: list) -> DataFrame:
        for column in list_of_column_names:
            first_row_value = spark_dataframe.select(first(column)).first()[0]
            spark_dataframe = spark_dataframe.withColumn(column, lit(first_row_value))
        return spark_dataframe



if __name__ == '__main__':
    # Extraction data from a public API
    extract_obj = Data_extractor()
    data = extract_obj.get_json_data(
        url = 'https://economia.awesomeapi.com.br/json/daily/USD-BRL',
        num_days = 10
    )

    # Initializing a spark management instance
    manager_spark_obj = Manag_spark()
    spark_session = manager_spark_obj.start_spark('Currency data collector')


    # Initializing a data loader instance 
    data_loader_obj = Data_loader()
    raw_dataframe = data_loader_obj.spark_df_using_list(
        data_list = data, 
        spark_session = spark_session)
    
    raw_dataframe.show()

    bronze_dataframe = (
        data_loader_obj
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
    )
    
    bronze_dataframe.show()
    bronze_dataframe.printSchema()

    manager_spark_obj.stop_spark(spark_session)
