import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_clean_job import main

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "CLIENT_FILE", "CITY_FILE", "OUTPUT_FILE"])
    job.init(args['JOB_NAME'], args)

    CLIENT_FILE = args["CLIENT_FILE"]
    CITY_FILE = args["CITY_FILE"]
    OUTPUT_FILE = args["OUTPUT_FILE"]

    main(spark, CLIENT_FILE, CITY_FILE, OUTPUT_FILE)

    job.commit()
