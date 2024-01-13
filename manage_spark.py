import findspark
findspark.init()

from pyspark.sql import SparkSession

class Manag_spark():
    def __init__(self):
        print('---------- Initializing the spark management instance ----------')

    def start_spark(self, app_name: str, delta: bool = False) -> SparkSession:
        """
        This method starts a new Spark Session, receiving an app name and an option to use a delta table as a parameter.
        The default value to use delta tables is false.
        """

        if delta:
            spark = (
                SparkSession
                    .builder
                    .master('local[*]')
                    .config('spark.jars.packages', 'io.delta:delta-core_2.12:1.0.0')
                    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                    .getOrCreate()
            )
            
        else:
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
    spark_session = obj_gerenc_spark.start_spark(app_name = "Data engineering", delta = True)
    print(spark_session)

    obj_gerenc_spark.stop_spark(spark_session)
