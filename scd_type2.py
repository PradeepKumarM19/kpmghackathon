import logging

logger = logging.getLogger(__file__)

def prepare_raw_df(raw_df, primary_key_columns, hive_object, scd_object):

    #Get the column list for record has calculation
    pre_raw_column_list = raw_df.columns
    record_hash_columns = [column for column in pre_raw_column_list if column not in primary_key_columns]

    #Add primarykey column to raw_df
    raw_df = hive_object.concatinate_operation(raw_df, primary_key_columns, "primarykey")

    #Add recordhash column to raw_df
    raw_df = hive_object.concatinate_operation(raw_df, record_hash_columns, "recordhash")

    #Rename raw_df columns prefixed with "src_"

    raw_df = hive_object.rename_source_dataframe_columns(raw_df)

    #Add Audit colums to RAW
    raw_df = scd_object.add_raw_audit_columns(raw_df)

    return raw_df

def scd_type_2(self, raw_df, scd_df, config_file, hive_object, scd_object):
    """Purpose  : Get the final Dataframe after SCD type1 Computation
        Result   : Returns the New and Update records
    """
    #Prepare Raw Dataframe
    primary_key_columns = config_file['primary_key'].split(",")
    partition_column = config_file['partition_column'].split(",") if config_file['partition_column'] else None

    raw_df = prepare_raw_df(raw_df, primary_key_columns, hive_object, scd_object)
    
    if scd_df:
        #Read the Raw and Scd Column list
        raw_column_list = raw_df.columns
        scd_column_list = scd_df.columns

        #Perform SCD Computation
        merged_df = scd_object.full_merge(raw_df, scd_df)

        unchanged_df = scd_object.unchanged_records(merged_df, scd_column_list)

        #New RAW Records
        new_df = scd_object.insert_new_records(merged_df, raw_column_list)

        #Upadte RAW Records
        update_raw_df = scd_object.update_raw_records(merged_df, raw_column_list)

        #Upadte SCD Records
        update_scd_df = scd_object.update_scd_records(merged_df, scd_column_list)

        #Union the Raw records to Rename the columns
        pre_insert_update_raw_df = new_df.unionAll(update_raw_df)
        pre_insert_update_raw_df = hive_object.rename_target_dataframe_columns(
            pre_insert_update_raw_df, raw_column_list
        )
        pre_insert_update_raw_df = pre_insert_update_raw_df.select(scd_column_list)

        #Union all dataframe for active Records
        final_df = unchanged_df.unionAll(pre_insert_update_raw_df).unionAll(update_scd_df)

    else:
        #Get the Column list
        raw_column_list = raw_df.columns
        #Rename the target dataframe
        final_df = hive_object.rename_target_dataframe_columns(raw_df, raw_column_list)

        column_list = final_df.columns
        non_partition_column = [column for column in column_list if column not in partition_column]
        final_df_column = [*non_partition_column, *partition_column]
        final_df = final_df.select(final_df_column)

        print("1"*100, final_df)
        hive_object.create_table_operation(
            final_df, config_file['schema'], config_file['target_table'], partition_column=partition_column
        )
        
    hive_object.insert_overwrite_operation(
        final_df, config_file['schema'], config_file['target_table'], partition_column=partition_column
    )
