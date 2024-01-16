from datetime import datetime
import findspark
findspark.init()

from pyspark.sql import SparkSession

class ManagSpark():
    def __init__(self, app_name: str, delta: bool = False):
        self.started_in = datetime.now().strftime('%y-%m-%d %H:%M:%S')
        self.app_name = app_name
        self.delta = delta

    def start_spark(self) -> SparkSession:
        '''
        This method starts a new Spark Session. The default value to use delta tables is false.
        '''

        if self.delta:
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
                    .appName(self.app_name)
                    .getOrCreate()
            )

        return spark

    def stop_spark(self, spark_session: SparkSession) -> None:
        '''
        This method stops an existing Spark Session, taking as a parameter the variable that contains the Spark Session.
        '''
        spark_session.stop()
        print('------- The spark session was ended -------')


if __name__ == '__main__':
    obj_gerenc_spark = ManagSpark(app_name = 'Data engineering', delta = True)
    spark_session = obj_gerenc_spark.start_spark()
    print(spark_session)
    print(obj_gerenc_spark.started_in)

    obj_gerenc_spark.stop_spark(spark_session)
