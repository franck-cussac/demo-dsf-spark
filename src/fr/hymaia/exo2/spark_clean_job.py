import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main(spark=SparkSession.builder.appName("wordcount").master("local[*]").getOrCreate(),
         client_file="src/resources/exo2/clients_bdd.csv",
         city_file="src/resources/exo2/city_zipcode.csv",
         output_file="data/exo2/clean"):

    df_city = spark.read.option("header", True).csv(city_file)
    df_clients = spark.read.option("header", True).csv(client_file)

    df_clients_major_cities_dpt = clean_data(df_city, df_clients)

    df_clients_major_cities_dpt.write \
        .mode("overwrite") \
        .option("header", True) \
        .parquet(output_file)


def clean_data(df_clients, df_city):
    # Filter age > 18
    df_clients_major = filter_on_age(df_clients)

    # Join with cities
    df_clients_major_cities = join_on_zip(df_clients_major, df_city)

    # Add departement
    return add_department(df_clients_major_cities)

def filter_on_age(df, col_name="age"):
    return df.filter(f.col(col_name) >= 18)


def join_on_zip(df, df_city, on="zip", how="inner"):
    return df.join(other=df_city, on=on, how=how)


def add_department(df, zip_col="zip", dpt_col_name="department"):
    df_dpt_raw = df.withColumn(dpt_col_name,
                               f.when((f.substring(f.col(zip_col), 1, 2) == "20") & (f.col(zip_col) <= "20190"), "2A") \
                                .when((f.substring(f.col(zip_col), 1, 2) == "20") & (f.col(zip_col) > "20190"), "2B") \
                                .otherwise(f.substring(f.col(zip_col), 1, 2)))
    return df_dpt_raw
