import logging

logger = logging.getLogger(__file__)

# class ScdType1:
#     def __init__(self, spark, logger):
#         """Constructor."""
#         self.spark = spark
        # self.logger = logger

def scd_type_1(spark, raw_df, scd_df, config_file, hive_object, scd_object):
    """Purpose  : Get the final Dataframe after SCD type1 Computation
        Result   : Returns the New and Update records
    """
    primary_key_columns = config_file['primary_key'].split(",")
    #Get the column list for record has calculation
    column_list = raw_df.columns
    record_hash_columns = [column for column in column_list if column not in primary_key_columns]

    #Add primarykey column to raw_df
    raw_df = hive_object.concatinate_operation(raw_df, primary_key_columns, "primarykey")

    #Add recordhash column to raw_df
    raw_df = hive_object.concatinate_operation(raw_df, record_hash_columns, "recordhash")

    if scd_df:
        #Perform SCD Computation
        final_df = scd_object.scd1_records(raw_df, scd_df)

    else:
        final_df = raw_df
        hive_object.create_table_operation(
            final_df, config_file['schema'], config_file['target_table']
        )

    hive_object.insert_overwrite_operation(
        final_df, config_file['target_table'], config_file['partition_column']
    )

