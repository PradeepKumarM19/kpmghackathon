import logging

logger = logging.getLogger(__file__)
# class ScdType2:
#     def __init__(self, spark, logger):
#         """Constructor."""
#         self.spark = spark
#         self.logger = logger

#     @staticmethod
def prepare_raw_df(raw_df, primary_key_columns, hive_object, scd_object):
    """" """
    #Get the column list for record has calculation
    pre_raw_column_list = raw_df.columns
    record_hash_columns = [column for column in pre_raw_column_list if column not in primary_key_columns]

    #Add primarykey column to raw_df
    raw_df = hive_object.concatinate_operation(raw_df, primary_key_columns, "primarykey")

    #Add recordhash column to raw_df
    raw_df = hive_object.concatinate_operation(raw_df, record_hash_columns, "recordhash")

    #Rename raw_df columns prefixed with "src_"
    raw_df = hive_object.rename_source_dataframe_columns(raw_df, pre_raw_column_list)

    #Add Audit colums to RAW
    raw_df = scd_object.add_raw_audit_columns(raw_df)

    return raw_df

def scd_type_2(self, raw_df, scd_df, primary_key_columns, hive_object, scd_object):
    """Purpose  : Get the final Dataframe after SCD type1 Computation
        Result   : Returns the New and Update records
    """
    #Prepare Raw Dataframe
    raw_df = ScdType2.prepare_raw_df(raw_df, primary_key_columns, hive_object, scd_object)

    #Read the Raw and Scd Column list
    raw_column_list = raw_df.columns
    scd_column_list = scd_df.columns

    #Perform SCD Computation
    merged_df = scd_object.scd1_records(raw_df, scd_df)

    #Unchanged SCD Records
    unchanged_df = scd_object.unchanged_records(merged_df, scd_column_list)

    #New RAW Records
    new_df = scd_object.insert_new_records(merged_df, raw_column_list)

    #Delete SCD Records
    delete_df = scd_object.delete_scd_records(merged_df, scd_column_list)

    #Upadte RAW Records
    update_raw_df = scd_object.update_raw_records(merged_df, raw_column_list)

    #Upadte SCD Records
    update_scd_df = scd_object.update_scd_records(merged_df, scd_column_list)

    #Union the Raw records to Rename the columns
    pre_insert_update_raw_df = new_df.unionAll(update_raw_df)
    pre_insert_update_raw_df = hive_object.rename_target_dataframe_columns(pre_insert_update_raw_df, raw_column_list)

    #Union all dataframe for active Records
    active_df = unchanged_df.unionAll(pre_insert_update_raw_df)

    #Union all dataframe for inactive Records
    inactive_df = delete_df.unionAll(update_scd_df)

    return active_df, inactive_df

def first_load(self, raw_df, primary_key_columns, hive_object, scd_object):
    """Add the Audit columsn to Raw df."""
    #Prepare Raw Dataframe
    raw_df = ScdType2.prepare_raw_df(raw_df, primary_key_columns, hive_object, scd_object)

    #Get the Column list
    raw_column_list = raw_df.columns

    #Rename the target dataframe
    final_df = hive_object.rename_target_dataframe_columns(raw_df, raw_column_list)

    return final_df








