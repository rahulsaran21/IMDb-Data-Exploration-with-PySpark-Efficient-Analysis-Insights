from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, when
from lib.etl_logger import setup_logger
from lib.etl_config import load_spark_config, get_db_config
import time
import sys, logging


def initialize_etl():
    # Load Configurations
    spark_conf = load_spark_config()
    db_config = get_db_config()

    # Initialize Logger
    log_folder = "logs"
    logger = setup_logger(f"{log_folder}/etl_log.log")

    # Initialize Spark Session
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    return spark, db_config, logger, start_time

def extract_data():
    try:
        print("Connecting to PostgreSQL...")
        # Extract movies data
        movies_df = spark.read \
                        .format("jdbc") \
                        .option("url", db_config["url"]) \
                        .option("dbtable", db_config["table_movies"]) \
                        .option("user", db_config["user"]) \
                        .option("password", db_config["password"]) \
                        .option("driver", db_config["driver"]) \
                        .option("fetchsize", "50000") \
                        .option("numPartitions", "16") \
                        .option("partitionColumn", "startyear") \
                        .option("lowerBound", "1874") \
                        .option("upperBound", "2031") \
                        .load()
        
        print("✅ Connection Successful! Movies data extracted")

        # Extract ratings data
        ratings_df = spark.read \
                        .format("jdbc") \
                        .option("url", db_config["url"]) \
                        .option("dbtable", db_config["table_ratings"]) \
                        .option("user", db_config["user"]) \
                        .option("password", db_config["password"]) \
                        .option("driver", db_config["driver"]) \
                        .load()
        
        print("Ratings data extracted")

        print("Checkind the data")
        movies_df.orderBy(col("tconst")).show(truncate=False)
        ratings_df.orderBy(col("tconst")).show(truncate=False)

        return movies_df, ratings_df

    except Exception as e:
        print(f"Error connecting to PostgreSQL: {str(e)}")
        return None, None
    

def transform_data(movies_df, ratings_df):
    print("Applying Transformation...")

    print("1. Handling Null Values")
    # Handling Null Values and Medium Runtime Values
    movies_df = movies_df.fillna({"startyear": 0, "endyear": 0})
    movies_df = movies_df.filter(col("titletype") != 'tvPilot')

    medium_runtime_df = movies_df.groupBy(col("titletype")).agg(avg("runtimeminutes").alias("medium_runtime"))
    movies_df = movies_df.join(medium_runtime_df, "titletype", "left") \
                        .withColumn("runtimeminutes", when(col("runtimeminutes").isNull(), col("medium_runtime")).otherwise(col("runtimeminutes"))) \
                        .drop("medium_runtime")
    

    print("2. Applying Data Conversion")
    movies_df = movies_df.withColumn("runtimeminutes", col("runtimeminutes").cast("int"))
    ratings_df = ratings_df.withColumn("averagerating", col("averagerating").cast("float"))

    # Log Schema Information
    print("\n Movies Schema:")
    movies_df.printSchema()

    print("\n Ratings Schema:")
    ratings_df.printSchema()


    # Logging NULL Value Check
    print("🔍 Checking NULL values in Movies dataset:")
    movies_df.select([sum(col(column).isNull().cast("int")).alias(column) for column in movies_df.columns]).show()
    print("🔍 Checking NULL values in Ratings dataset:")
    ratings_df.select([sum(col(column).isNull().cast("int")).alias(column) for column in ratings_df.columns]).show()
    

    return movies_df, ratings_df

    
def load_data(movies_df, ratings_df):
    parquet_folder = "Parquet"

    print("Saving the transformed data as Parquet File...")
    movies_df.write.parquet(f"data/{parquet_folder}/MOVIES_FILE.parquet", mode="overwrite")
    ratings_df.write.parquet(f"data/{parquet_folder}/RATINGS_FILE.parquet", mode="overwrite")
    print(f"Data saved successfully in {parquet_folder} folder.")


if __name__ == "__main__":
    start_time = time.time()
    spark, db_config, logger, start_time = initialize_etl()
    movies, ratings = extract_data()
    if movies and ratings:
        movies, ratings = transform_data(movies, ratings)
        load_data(movies, ratings)
    end_time = time.time()
    execution_time = f"Total time taken to complete: {end_time - start_time:2f} seconds"
    # Print only to console, not the log file
    sys.__stdout__.write(execution_time)
    sys.__stdout__.flush()

    # Log execution time in the log file
    logging.info(execution_time.strip())