import __main__

from os import environ, listdir, path
import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies.logger import Log4j


def start_spark_app(app_name='my_spark_app', master='local[*]',files=[], spark_config={}):
 
    spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))

        # create Spark JAR packages string  
    spark_files = ','.join(list(files))
    spark_builder.config('spark.files', spark_files)

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = Log4j(spark_sess)

    #BUM= log4jLogger.LogManager.getLogger(__full_name__)
    #BUM.info("Hello logger...")
    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        spark_logger.warn('no config file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict