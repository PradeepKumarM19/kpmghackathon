import pandas as pd
from pyspark.sql import SparkSession
from utility.scd_computation import ScdComputation


def config_read(spark, config_file, imported_file_name):

    df = spark.read.csv(config_file, inferSchema=True, header=True)
    df = df.where(df["file_name"] == imported_file_name)

    return df

def interface(files, config_file):

    spark = SparkSession.builder.enableHiveSupport().appName("My main").getOrCreate()


    for file in files:
        print("="*100, file)
        imported_file_name = file.split("\\")[-1]
        config_data = config_read(spark, config_file, imported_file_name)

        print(config_data.show())
