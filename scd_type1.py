class ScdType1:
    def __init__(self, spark, logger):
        """Constructor."""
        self.spark = spark
        self.logger = logger

    def scd_computation(self, raw_df, scd_df, primary_key_columns, hive_object, scd_object):
        """Purpose  : Get the final Dataframe after SCD type1 Computation
           Result   : Returns the New and Update records
        """

        #Get the column list for record has calculation
        column_list = raw_df.column
        record_hash_columns = [column for column in column_list if column not in primary_key_columns]

        #Add primarykey column to raw_df
        raw_df = hive_object.concatinate_operation(raw_df, primary_key_columns, "primarykey")

        #Add recordhash column to raw_df
        raw_df = hive_object.concatinate_operation(raw_df, record_hash_columns, "recordhash")

        if not scd_df:
            #Perform SCD Computation
            final_df = scd_object.scd1_records(raw_df, scd_df)
        else:
            final_df = raw_df

        return final_df








