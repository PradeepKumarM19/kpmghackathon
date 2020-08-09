from pysprak.sql.functions import sha2
from pysprak.sql.functions import concat
from pysprak.sql.functions import format_string

from utility import spark_utils
from utility.spark_utils import SparkUtils

CREATE_EXETRNAL_TABLE_WITH_PARTITION = """
CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} ({schema})
PARTITIONED BY ({partition})
STORED AS PARQUET LOCATION '{hive_location}'
"""

CREATE_EXETRNAL_TABLE_WITHOUT_PARTITION = """
CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} ({schema})
STORED AS PARQUET LOCATION '{hive_location}'
"""

CREATE_INTERNAL_TABLE_WITH_PARTITION = """
CREATE  TABLE IF NOT EXISTS {table_name} ({schema})
PARTITIONED BY ({partition})
STORED AS PARQUET LOCATION '{hive_location}'
"""

CREATE_INTERNAL_TABLE_WITHOUT_PARTITION = """
CREATE TABLE IF NOT EXISTS {table_name} ({schema})
STORED AS PARQUET LOCATION '{hive_location}'
"""

READ_TABLE_WITH_PARTITION = """
SELECT * FROM {table_name} where {partition}
"""

READ_TABLE_WITHOUT_PARTITION = """
SELECT * FROM {table_name}
"""

INSERT_INTO_WITH_PARTITION = """
INSERT INTO TABLE {table_name} PARTITION({partition})
SELECT * FROM {view_name}
"""

INSERT_INTO_WITHOUT_PARTITION = """
INSERT INTO TABLE {table_name} SELECT * FROM {view_name}
"""

INSERT_OVERWRITE_WITH_PARTITION = """
INSERT OVERWRITE TABLE {table_name} PARTITION({partition})
SELECT * FROM {view_name}
"""

INSERT_OVERWRITE_WITHOUT_PARTITION = """
INSERT OVERWRITE TABLE {table_name}
SELECT * FROM {view_name}
"""


class HiveOperations:
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    def concatinate_operation(self, dataframe, colum_list, new_column_name):
        """Concate the columns in the column list and create a hash value."""
        dataframe = (
            dataframe
            .withColumn(new_column_name, sha2(concat(*[format_string("%s_", column) for column in colum_list]), 256))
        )
        return dataframe

    def rename_source_dataframe_columns(self, src_dataframe, source_colum_list):
        """Prefix src_ to all the columns in source dataframe."""
        for column in source_colum_list:
            source_df = src_dataframe.withColumnRenamed(column, f"src_{column}")
        return source_df

    def rename_target_dataframe_columns(self, targer_df, target_colum_list):
        """Remove the perfix src_ from all the columns in target dataframe."""
        for column in target_colum_list:
            targer_df = target_colum_list.withColumnRenamed(column, column.split("src_", 1)[1])
        return targer_df

    def does_table_exists(self, table_name):
        """Check if table exixts or not."""
        return self.spark._jcatalog.tableExists(table_name)


    def read_raw_scd_records(
            self,
            raw_file,
            scd_type,
            scd_table,
            primary_keys,
            partition_keys,
    ):
        raw_input = SparkUtils.read_data(self.spark, "file", raw_file)
        scd_table =  SparkUtils.read_data(self.spark, "table", scd_table)


    # def read_raw_scd_records(self, raw_table_name, scd_table_name, raw_partition=None, scd_partition=None):
    #     """Purpose : Read the Raw and Scd Table Records."""
    #     #Check if the scd table exists
    #     table_status = self.does_table_exists(scd_table_name)

    #     if table_status:
    #         #Read Raw Dataframe Records
    #         if raw_partition:
    #             raw_df = READ_TABLE_WITH_PARTITION.format(table_name=raw_table_name, partition=raw_partition)
    #         else:
    #             raw_df = READ_TABLE_WITHOUT_PARTITION.format(table_name=raw_table_name)

    #         #Read SCD Dataframe Records
    #         if scd_partition:
    #             scd_df = READ_TABLE_WITH_PARTITION.format(table_name=scd_table_name, partition=scd_partition)
    #         else:
    #             scd_df = READ_TABLE_WITHOUT_PARTITION.format(table_name=scd_table_name)
    #     else:
    #         #Read Raw Dataframe Records
    #         if raw_partition:
    #             raw_df = READ_TABLE_WITH_PARTITION.format(table_name=raw_table_name, partition=raw_partition)
    #         else:
    #             raw_df = READ_TABLE_WITHOUT_PARTITION.format(table_name=raw_table_name)

    #         #Set SCD DF to NONE
    #         scd_df = None

    #     return raw_df, scd_df

    def create_schema_operation(self, schema_list):
        """Create the table creation schema"""
        schema = ",".join([f"{data[0]} {data[1]}" for data in schema_list])
        return schema

    def create_table_operation(self, dataframe, table_name, hive_location, partition_column=None, external=None):
        """Create hive table."""
        schema_list = dataframe.dtypes
        if not external:
            if not partition_column:
                schema = self.create_schema_operation(schema_list)
                query = CREATE_INTERNAL_TABLE_WITHOUT_PARTITION.format(
                    table_name=table_name,
                    schema=schema,
                    hive_location=hive_location
                )
            else:
                pre_schema_list = [schema_key for schema_key in schema_list if schema_key[0] not in partition_column]
                schema = self.create_schema_operation(pre_schema_list)
                partition_string = ",".join([f"{data} string" for data in partition_column])
                query = CREATE_INTERNAL_TABLE_WITH_PARTITION.format(
                    table_name=table_name,
                    schema=schema,
                    partition=partition_string,
                    hive_location=hive_location
                )
        else:
            if not partition_column:
                schema = self.create_schema_operation(schema_list)
                query = CREATE_EXETRNAL_TABLE_WITHOUT_PARTITION.format(
                    table_name=table_name,
                    schema=schema,
                    hive_location=hive_location
                )
            else:
                pre_schema_list = [schema_key for schema_key in schema_list if schema_key[0] not in partition_column]
                schema = self.create_schema_operation(pre_schema_list)
                partition_string = ",".join([f"{data} string" for data in partition_column])
                query = CREATE_EXETRNAL_TABLE_WITH_PARTITION.format(
                    table_name=table_name,
                    schema=schema,
                    partition=partition_string,
                    hive_location=hive_location
                )
        
        #execute query
        self.spark.sql(query)

    def insert_into_operation(self, dataframe, table_name, partition_column=None):
        """Insert into hive table."""

        if partition_column:
            #Reorder the partition columns at the end
            column_list = dataframe.columns
            non_partition_column = [column for column in column_list if column not in partition_column]
            final_df_column = [*non_partition_column, *partition_column]
            
            #create the final df with partition column at the end.
            final_df = dataframe.select(final_df_column)

            #create a temp view
            final_df.createOrReplaceTempView("temp_view")

            query = INSERT_INTO_WITH_PARTITION.format(
                table_name=table_name,
                partition=partition_column,
                view_name="temp_view"
            )
        else:
            #create a temp view
            dataframe.createOrReplaceTempView("temp_view")

            query = INSERT_INTO_WITHOUT_PARTITION.format(
                table_name=table_name,
                view_name="temp_view"
            )
        self.spark.sql(query)

    def insert_overwrite_operation(self, dataframe, table_name, partition_column=None):
        """Insert overwrite into hive table."""
        #dataframe.createOrReplaceTempView("temp_view")
        
        if partition_column:
            #Reorder the partition columns at the end
            column_list = dataframe.columns
            non_partition_column = [column for column in column_list if column not in partition_column]
            final_df_column = [*non_partition_column, *partition_column]
            
            #create the final df with partition column at the end.
            final_df = dataframe.select(final_df_column)

            #create a temp view
            final_df.createOrReplaceTempView("temp_view")

            query = INSERT_OVERWRITE_WITH_PARTITION.format(
                table_name=table_name,
                partition=partition_column,
                view_name="temp_view"
            )
        else:
            #create a temp view
            dataframe.createOrReplaceTempView("temp_view")

            query = INSERT_OVERWRITE_WITHOUT_PARTITION.format(
                table_name=table_name,
                view_name="temp_view"
            )

        self.spark.sql(query)