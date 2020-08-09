import logging

logger = logging.getLogger(__file__)


def scd_type_1(spark, raw_df, scd_df, config_file, hive_object, scd_object):
    """Purpose  : Get the final Dataframe after SCD type1 Computation
        Result   : Returns the New and Update records
    """
    primary_key_columns = config_file['primary_key'].split(",")
    partition_column = config_file['partition_column'].split(",")
    #Get the column list for record has calculation
    column_list = raw_df.columns
    record_hash_columns = [column for column in column_list if column not in primary_key_columns]

    #Add primarykey column to raw_df
    raw_df = hive_object.concatinate_operation(raw_df, primary_key_columns, "primarykey")

    #Add recordhash column to raw_df
    raw_df = hive_object.concatinate_operation(raw_df, record_hash_columns, "recordhash")

    if scd_df:
        #Perform SCD Computation
        column_list = scd_df.columns
        new_records_df = scd_object.scd1_new_records(raw_df, scd_df)
        new_records_df = new_records_df.select(column_list)

        unchanged_records_df = scd_object.scd1_unchanged_records(raw_df, scd_df)

        final_df = new_records_df.union(unchanged_records_df)

    else:
        column_list = raw_df.columns
        non_partition_column = [column for column in column_list if column not in partition_column]
        final_df_column = [*non_partition_column, *partition_column]
        final_df = raw_df.select(final_df_column)
        hive_object.create_table_operation(
            final_df, config_file['schema'], config_file['target_table'], partition_column=partition_column
        )

    hive_object.insert_overwrite_operation(
        final_df, config_file['schema'], config_file['target_table'], partition_column=partition_column
    )
