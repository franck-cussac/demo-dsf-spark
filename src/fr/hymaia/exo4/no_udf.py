import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder \
        .appName("exo4") \
        .master("local[*]") \
        .getOrCreate()

    df = spark.read.option("header", True).csv("src/resources/exo4/sell.csv")
    df_category = extract_category_name(df)

    window = Window.partitionBy("category", "date")
    df_total_price = df_category.withColumn("total_price_per_category_per_day", f.sum("price").over(window))

    res = add_column_total_price_per_category_last_30_days(df_total_price)
    res.write.parquet("outputNoUdf")


def extract_category_name(df, category_col="category", category_name_col="category_name"):
    return df.withColumn(category_name_col, f.when(f.col(category_col) < 6, "food").otherwise("furniture"))


def add_column_total_price_per_category_last_30_days(df):
    df = df.withColumn("date_timestamp", df.date.cast('timestamp'))
    window = Window.partitionBy("category").orderBy(f.col("date_timestamp").cast('long')).rangeBetween(-2592000, 0)
    sum_column = f.sum("price").over(window)

    return df.withColumn("total_price_per_category_per_day_last_30_days", sum_column).drop("date_timestamp")
