import os

import pandas as pd
from pyspark.sql import SparkSession
from utility.scd_computation import ScdComputation
class Interface:
    @staticmethod
    def config_read(config_file):
        df = pd.read_csv(config_file)
        return df

    @staticmethod
    def filter_config(conf, file_name):
        file_conf = conf[conf["file_name"]==file_name].to_dict('records')
        if file_conf:
            return file_conf[0]

    @staticmethod
    def spark_session():
        return SparkSession.builder.enableHiveSupport().appName("My main").getOrCreate()

    @classmethod
    def execute_config(cls, files, config_file):
        spark = cls.spark_session()
        configuration = cls.config_read(config_file)

        for file in files:
            print("-"*50,"File : ", file)
            file_name = file.split("/")[-1]
            file_config = cls.filter_config(configuration, file_name)
            print(file_config)
            ScdComputation.read_raw_scd_records(
                raw_file = file,
                scd_type = file_config["scd_type"],
                scd_table = file_config["scd_table"],
                primary_keys = file_config["primary_keys"],
                partition_keys = file_config["partition_keys"],
            )


