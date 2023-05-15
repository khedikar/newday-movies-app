import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(1, ROOT_DIR)

from application import ETLJob

def test_joined_data(spark):
    movies_df = spark.createDataFrame(
        data=[
            (7, "Sabrina (1995)", "Comedy|Romance"),
            (8, "Tom and Huck (1995)", "Adventure|Children's"),
            (9, "Sudden Death (1995)", "Action"),
        ],
        schema=["MovieID", "Title", "Genres"],
    )

    ratings_df = spark.createDataFrame(
        data=[
            (10, 7, 4, "123"),
            (26, 7, 4, "234"),
            (23, 7, 5, "2345"),
            (10, 8, 4, "5556"),
            (11, 8, 2, "5342"),
            (55, 9, 4, "97822"),
            (10, 12, 5, "123"),
            (10, 11, 4, "5556"),
        ],
        schema=["UserID", "MovieID","Rating", "Timestamp"],
    )

    output_df = ETLJob().joined_data(movies_df, ratings_df)
    output_columns = output_df.columns

    assert output_df.count() == 3
    assert "MaxRating" in output_columns
    assert "MinRating" in output_columns
    assert "AverageRating" in output_columns
    assert output_df.where("Title = 'Tom and Huck (1995)'").select("MaxRating").collect()[0][0] == 4
    assert output_df.where("Title = 'Tom and Huck (1995)'").select("MinRating").collect()[0][0] == 2
    assert output_df.where("Title = 'Tom and Huck (1995)'").select("AverageRating").collect()[0][0] == 3


def test_top_3_movies_data(spark):
    movies_df = spark.createDataFrame(
        data=[
            (7, "Sabrina (1995)", "Comedy|Romance"),
            (8, "Tom and Huck (1995)", "Adventure|Children's"),
            (9, "Sudden Death (1995)", "Action"),
        ],
        schema=["MovieID", "Title", "Genres"],
    )

    ratings_df = spark.createDataFrame(
        data=[
            (10, 7, 4, "123"),
            (26, 7, 4, "234"),
            (23, 7, 5, "2345"),
            (10, 8, 4, "5556"),
            (11, 8, 2, "5342"),
            (55, 9, 4, "97822"),
            (10, 12, 5, "123"),
            (10, 11, 4, "5556"),
        ],
        schema=["UserID", "MovieID","Rating", "Timestamp"],
    )

    output_df = ETLJob().top_3_movies_data(movies_df, ratings_df)
    print(output_df.count())

    assert output_df.count() == 7
    assert output_df.where("UserID = 10").count() == 3
    expected_classifications = [12,7,8]
    assert sorted(output_df.where("UserID = 10").select("MovieID").rdd.flatMap(lambda x: x).collect()) == sorted(expected_classifications)
    # assert output_df.where("Title = 'Tom and Huck (1995)'").select("AverageRating").collect()[0][0] == 3