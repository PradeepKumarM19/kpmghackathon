from pyspark.sql.functions import sha2
from pyspark.sql.functions import concat
from pyspark.sql.functions import format_string

from utility import spark_utils
# from utility.spark_utils import SparkUtils

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
STORED AS PARQUET
"""

CREATE_INTERNAL_TABLE_WITHOUT_PARTITION = """
CREATE TABLE IF NOT EXISTS {table_name} ({schema})
STORED AS PARQUET
"""

READ_TABLE_WITH_PARTITION = """
SELECT * FROM {table_name}
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

    def rename_source_dataframe_columns(self, src_dataframe):
        """Prefix src_ to all the columns in source dataframe."""
        source_colum_list = src_dataframe.columns
        for column in source_colum_list:
            src_dataframe = src_dataframe.withColumnRenamed(column, f"src_{column}")

        return src_dataframe

    def rename_target_dataframe_columns(self, targer_df, target_colum_list):
        """Remove the perfix src_ from all the columns in target dataframe."""

        for column in target_colum_list:
            targer_df = targer_df.withColumnRenamed(column, column.split("src_", 1)[1])
        return targer_df

    def does_table_exists(self, table_name):
        """Check if table exixts or not."""
        return self.spark.catalog._jcatalog.tableExists(table_name)

    def read_raw_records(self, raw_file):
        raw_df = spark_utils.read_data(self.spark, "file", raw_file)

        return raw_df

    def read_scd_table(self, target_table):
        scd_df =  spark_utils.read_data(self.spark, "table", target_table)

        return scd_df

    def create_schema_operation(self, schema_list):
        """Create the table creation schema"""
        schema = ",".join([f"{data[0]} {data[1]}" for data in schema_list])
        return schema

    def create_table_operation(self, dataframe, db_name, table_name, hive_location=None, partition_column=None, external=None):
        """Create hive table."""

        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db_name}")

        table_name = f"{db_name}.{table_name}"

        schema_list = dataframe.dtypes
        if external is None:
            if partition_column is None:
                schema = self.create_schema_operation(schema_list)
                query = CREATE_INTERNAL_TABLE_WITHOUT_PARTITION.format(
                    table_name=table_name,
                    schema=schema,
                )
            else:
                pre_schema_list = [schema_key for schema_key in schema_list if schema_key[0] not in partition_column]
                schema = self.create_schema_operation(pre_schema_list)
                partition_string = ",".join([f"{data} string" for data in partition_column])
                query = CREATE_INTERNAL_TABLE_WITH_PARTITION.format(
                    table_name=table_name,
                    schema=schema,
                    partition=partition_string,
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

    def insert_overwrite_operation(self, dataframe, db_name, table_name, partition_column=None):
        """Insert overwrite into hive table."""

        table_name = f"{db_name}.{table_name}"
        if partition_column:

            dataframe.createOrReplaceTempView("temp_view")

            partition_string = ",".join(partition_column)

            query = INSERT_OVERWRITE_WITH_PARTITION.format(
                table_name=table_name,
                partition=partition_string,
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
