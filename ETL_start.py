from pyspark.sql import SparkSession
from dependencies.spark import start_spark_app
from jobs.extraction import * 

def smallDataset(spark):
    airlines = spark.read.option("header", "true").csv("/airlines_batch.csv")

    airports = spark.read.option("header", "true").csv("/airports_batch.csv")

    flights = spark.read.option("header", "true").csv("/flights_batch.csv")

    return airlines, airports, flights

def extract_data(spark):
    fr_api = getDataFlightRadarAPI()
    """airlines = fr_api.get_airlines()
    airports = fr_api.get_airports()
    flights = fr_api.get_flights()

    df_flights, spark = extract_flights_toDF(fr_api, spark)
    df_airports, spark = extract_airports_toDF(fr_api, spark)
    df_airlines, spark = extract_airlines_toDF(fr_api, spark)"""

    airlines, airports, flights = smallDataset(spark)
    df_airports, spark = extract_airports_toDF(airports, spark)
    df_flights, spark = extract_flights_toDF(fr_api, flights, spark)
    df_airlines, spark = extract_airlines_toDF(airlines, spark)

    return df_flights, df_airports, df_airlines, spark

def transform_data(data):
   return

def load_data(data):
    return

def main():
    spark, log, config = start_spark_app(app_name='my_etl_job')

    log.warn('ETL_job is up-and-running')

    # execute ETL pipeline
    df_flights, df_airports, df_airlines, spark = extract_data(spark)
    #data_transformed = transform_data(data)
    #load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    
    spark.stop()
    return None

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()