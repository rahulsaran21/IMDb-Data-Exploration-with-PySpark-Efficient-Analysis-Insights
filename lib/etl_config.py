import os
import configparser
from pyspark import SparkConf
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def load_spark_config():
    """Loads Spark configurations from a configuration file."""
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for key, value in config.items("SPARK_APP_CONFIG"):
        spark_conf.set(key, value)

    return spark_conf

def get_db_config():
    """Fetches database connection details from environment variables."""
    return {
        "url": os.getenv("DB_URL"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "table_movies": os.getenv("DB_TABLE_MOVIES"),
        "table_ratings": os.getenv("DB_TABLE_RATINGS"),
        "driver": os.getenv("DB_DRIVER"),
    }
