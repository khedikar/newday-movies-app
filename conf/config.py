# Library and external modules declaration
from pyspark.sql.types import *


class CleanConfigurations:
    # Spark custom properties
    SPARK_CONF = {"CONF":
        [
            ("spark.dynamicAllocation.enabled", "true"),
            ("spark.speculation", "true"),
            ("spark.sql.codegen.wholeStage", "false"),
        ]}    

    # Custom schema
    MOVIES_SCHEMA = StructType(fields=[StructField("MovieID", IntegerType(), True),
                            StructField("Title", StringType(), True),
                            StructField("Genres", StringType(), True)])
    MOVIES_FILE_PATH = "s3://<bucketname>/clean_data/newday-movie-app/input/movies.dat"
    MOVIES_FILE_EXTENSION = 'dat'
    MOVIES_DELIMITER = '::'
    MOVIES_HEADER = False

    RATINGS_SCHEMA = StructType(fields=[StructField("UserID", IntegerType(), True),
                            StructField("MovieID", IntegerType(), True),
                            StructField("Rating", IntegerType(), True),
                            StructField("Timestamp", StringType(), True)])
    RATINGS_FILE_PATH = "s3://<bucketname>/clean_data/newday-movie-app/input/ratings.dat"
    RATINGS_FILE_EXTENSION = 'dat'
    RATINGS_DELIMITER = '::'
    RATINGS_HEADER = False

    TASK2_OUTPUT_PATH = "s3://<bucketname>/clean_data/newday-movie-app/output/task2_output"
    TASK3_OUTPUT_PATH = "s3://<bucketname>/clean_data/newday-movie-app/output/task3_output"