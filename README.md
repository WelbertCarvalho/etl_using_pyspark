# Pyspark project using OOP and a medallion architecture

## Project structure
- Object Oriented Programming
- This project uses pyspark 3.5 and delta-spark 3.0
- A virtual environment named 'venv' needs to be created exporting JAVA_HOME and SPARK_HOME
- The classes are separated per finallity having data engineering activities

## Classes

### To manage spark (manage_spark.py)
This class provides a spark session object having the option to use delta tables or not.

### Data extraction via API for testing (extraction.py)
In this class I developed a data extraction using an specific API for testing data transformation and storing using spark and delta tables.

### Data transformation (transformation.py)
A class created to provide some high level methods in order to transform data easier.

### A class to load data (loading.py)
The objective here is to provide methods to create delta tables or just to export the transformed data into a datalake with medallion architecture.

#### requirements.txt
To install all the requirements use: `pip install -r requirements.txt`
