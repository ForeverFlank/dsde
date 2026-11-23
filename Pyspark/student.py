from pathlib import Path
from typing import Any, cast

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def q1(df: DataFrame):  # DO NOT modify this line
    # Count number of videos in the given spark dataframe
    # Return number of videos (integer)

    return df.count()


def q2(df: DataFrame):  # DO NOT modify this line
    # Which video has the most number of comments?
    # Return the video_id with the most comments (string)

    result = df.orderBy(F.desc("comments")).first()

    return result["video_id"]


def q3(df: DataFrame):  # DO NOT modify this line
    # Which category has the most number of videos?
    # Return the category_id (integer)

    result = (
        df.groupBy("category_id")
        .agg(F.countDistinct("video_id").alias("video_count"))
        .orderBy(F.desc("video_count"))
        .first()
    )

    return int(result["category_id"])


def q4(df: DataFrame):  # DO NOT modify this line
    # Let like_ratio be the ratio of likes to views for a video
    # like_ratio = likes / views
    # Skip videos with 0 views: the grader runs Spark 4, where dividing by 0 is an error
    # Return the highest like_ratio (float)

    result = (
        df.filter(F.col("views") > 0)
        .withColumn(
            "like_ratio", F.col("likes").cast("double") / F.col("views").cast("double")
        )
        .agg(F.max("like_ratio").alias("max_like_ratio"))
        .first()
    )

    return float(result["max_like_ratio"])


if __name__ == "__main__":
    host = "local"
    current_file_path = Path(__file__).resolve()
    data_file_path = str(current_file_path.parent / "youtube_trending.csv")
    print(f"Reading data from: {data_file_path}")

    # Work around incomplete Pylance type info for SparkSession.builder (classproperty).
    builder = cast(Any, SparkSession).builder
    spark: SparkSession = builder.appName("DSDE exam").master(host).getOrCreate()
    df: DataFrame = spark.read.csv(data_file_path, header=True, inferSchema=True)
    print("###")  # marker to separate contestant output from other prints
    print(q1(df))  # this should print '13579'
    print(q2(df))  # this should print 'g3zTR0_n-YY'
    print(q3(df))  # this should print '20'
    print(q4(df))  # this should print '1258.8'
    spark.stop()
