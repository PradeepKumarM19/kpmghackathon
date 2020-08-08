from pysprak.sql.functions import sha2
from pysprak.sql.functions import concat
from pysprak.sql.functions import format_string

READ_TABLE_WITH_PARTITION = """
SELECT * FROM {table_name} where {partition}
"""

READ_TABLE_WITHOUT_PARTITION = """
SELECT * FROM {table_name}
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