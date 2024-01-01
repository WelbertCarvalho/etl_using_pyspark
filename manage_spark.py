import findspark
findspark.init()

from pyspark.sql import SparkSession

class Manag_spark():
    def __init__(self):
        print('---------- Initializing the spark management instance ----------')

    def start_spark(self, app_name: str) -> SparkSession:
        """
        This method starts a new Spark Session, receiving an app name as a parameter.
        """

        spark = (
            SparkSession
                .builder
                .master('local[*]')
                .appName(app_name)
                .getOrCreate()
        )

        return spark

    def stop_spark(self, spark_session: SparkSession) -> None:
        """
        This method stops an existing Spark Session, taking as a parameter the variable that contains the Spark Session.
        """
        spark_session.stop()
        print('------- The spark session was ended -------')


if __name__ == '__main__':
    obj_gerenc_spark = Manag_spark()
    spark_session = obj_gerenc_spark.start_spark("Data engineering")
    print(spark_session)

    obj_gerenc_spark.stop_spark(spark_session)
