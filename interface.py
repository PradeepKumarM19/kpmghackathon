import os

import pandas as pd
import logging
from pyspark.sql import SparkSession
from utility.scd_utils import ScdComputation
from utility.hive_operations import HiveOperations
from scd_type1 import scd_type_1
from scd_type2 import scd_type_2


logger = logging.getLogger("SCD")

SCD_FUNCTION = {
    1: scd_type_1,
    2: scd_type_2,
}

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

        scd_object = ScdComputation(spark, logger)
        hive_object = HiveOperations(spark, logger)

        for file in files:
            logger.info("-"*50,"File : ", file)
            file_name = file.split("/")[-1]
            file_config = cls.filter_config(configuration, file_name)
            if file_config is None:
                logger.warn("File %s entry not found in configuration", file)
                continue
            logger.info(file_config)

            raw_df = hive_object.read_raw_records(file)
            scd_df = None
            # if hive_object.does_table_exists(file_config['target_table']):
            #     scd_df = hive_object.read_scd_table(file_config['target_table'])
            # else:
            #     scd_df = None

            scd_invoke = SCD_FUNCTION[file_config['scd_type']]
            scd_invoke(spark, raw_df, scd_df, file_config['primary_key'].split(","), hive_object, scd_object)

            logger.info("Process Completed")
            # ScdComputation.read_raw_scd_records(
            #     raw_file = file,
            #     scd_type = file_config["scd_type"],
            #     scd_table = file_config["scd_table"],
            #     primary_keys = file_config["primary_keys"],
            #     partition_keys = file_config["partition_keys"],
            # )


