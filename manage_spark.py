import findspark
findspark.init()

from pyspark.sql import SparkSession

class Manag_spark():
    def __init__(self):
        print('---------- Initializing the spark management instance ----------')

    def start_spark(self, app_name: str) -> SparkSession:
        spark = (
            SparkSession
                .builder
                .master('local[*]')
                .appName(app_name)
                .getOrCreate()
        )

        return spark

    def stop_spark(self, spark_session: SparkSession) -> None:
        spark_session.stop()
        print('------- The spark session was ended -------')


if __name__ == '__main__':
    obj_gerenc_spark = Manag_spark()
    spark_session = obj_gerenc_spark.start_spark("Data engineering")
    print(spark_session)

    obj_gerenc_spark.stop_spark(spark_session)