
# Library and external modules declaration
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from conf.config import CleanConfigurations
import pyspark
import logging

logging.getLogger("py4j").setLevel(logging.INFO)
logging.basicConfig(format='[%(asctime)s] - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class ETLJob(object):
    def main(self):
        """
            This function is used for performing transformation operations.
        """
        # Reading Movies data
        movies_df = self.read_source_data(CleanConfigurations.MOVIES_FILE_PATH, CleanConfigurations.MOVIES_FILE_EXTENSION, CleanConfigurations.MOVIES_SCHEMA, 
        CleanConfigurations.MOVIES_HEADER, CleanConfigurations.MOVIES_DELIMITER)
        logger.info("Movies file data read completed")
        
        # Reading Ratings data
        ratings_df = self.read_source_data(CleanConfigurations.RATINGS_FILE_PATH, CleanConfigurations.RATINGS_FILE_EXTENSION, CleanConfigurations.RATINGS_SCHEMA, 
        CleanConfigurations.RATINGS_HEADER, CleanConfigurations.RATINGS_DELIMITER)
        logger.info("Ratings file data read completed")

        # Getting Min, Max and Average for each movie
        output_df= self.joined_data(movies_df, ratings_df)

        # Getting top 3 movies based on their rating
        top_3_movies_df = self.top_3_movies_data(movies_df, ratings_df)

        # Writing data
        self.write_output_data(output_df,CleanConfigurations.TASK2_OUTPUT_PATH)
        self.write_output_data(top_3_movies_df,CleanConfigurations.TASK3_OUTPUT_PATH)

    def read_source_data(self, path, file_extension, final_struc, header, delimiter):
        """
            This function is used to read data with mutiple options.
        """
        if file_extension == 'csv' or file_extension == 'txt' or file_extension == 'dat':
            input_df = spark.read.csv(path, header=header, sep=delimiter, schema=final_struc)
        elif file_extension == 'parquet':
            # input_df = spark.read.parquet(path, schema=final_struc)
            input_df = spark.read.schema(final_struc).parquet(path)
        elif file_extension == 'json':
            input_df = spark.read.json(path, multiLine=True, schema=final_struc)
        else:
            raise Exception("File Format Not Supported")
        return input_df
    
    def joined_data(self, movies_df, ratings_df):
        """
            Getting max, min and average rating for each movie from ratimgs data 
        """
        # Getting Min, Max and Average for each movie
        grouped_ratings_df = ratings_df.groupBy("MovieID").agg(expr("max(Rating)").alias("MaxRating"),
                                        expr("min(Rating)").alias("MinRating"),
                                        expr("avg(Rating)").alias("AverageRating"))

        # Joining movies data with ratings data
        joined_df = movies_df.join(grouped_ratings_df, movies_df.MovieID == grouped_ratings_df.MovieID).drop(grouped_ratings_df.MovieID)
        return joined_df
    
    def top_3_movies_data(self, movies_df, ratings_df):
        """
            Getting top 3 movies based on their rating
        """
        # Getting top 3 movies rated by each user
        w = Window.partitionBy("UserID").orderBy(col("Rating").desc())
        combined_df = ratings_df.join(movies_df, movies_df.MovieID == ratings_df.MovieID,'leftouter').drop(movies_df.MovieID)
        combined_rank_df = combined_df.withColumn("rank",row_number().over(w))
        top_3_movies_df = combined_rank_df.filter(combined_rank_df.rank <= 3).drop("rank")
        return top_3_movies_df
    
    def write_output_data(self, df, path):
        """
            This function is used to write data at specified output location.
        """
        df.coalesce(1).write.mode("overwrite").parquet(path)
        logger.info("Parquet file created in directory: {}".format(path))

if __name__ == "__main__":
    try:
        application_name = "movies-app"
        logger.info("Application - {} started".format(application_name))
        spark_conf = CleanConfigurations.SPARK_CONF.get('CONF', [])
        if spark_conf:
            logger.info("SparkSession getting created with custom spark configuration")
            conf = pyspark.SparkConf().setAll(spark_conf)
            spark = SparkSession.builder.appName(application_name) \
                .config(conf=conf) \
                .enableHiveSupport() \
                .master("local[*]")\
                .getOrCreate()
        else:
            logger.info("SparkSession getting created with default spark configuration")
            spark = SparkSession.builder.appName(application_name) \
                .enableHiveSupport() \
                .master("local[*]")\
                .getOrCreate()
        ETLJob().main()
        spark.stop()
    except Exception as e:
        logger.error("Failed while performing ETL job with error - " + str(e))
        raise e

