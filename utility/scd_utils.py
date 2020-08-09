from pyspark.sql.functions import lit, when, date_sub, col
from datetime import datetime

high_date = datetime.strptime('9999-12-31', '%Y-%m-%d').date()
current_date = datetime.today().date()

class ScdComputation:
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger
    
    @staticmethod
    def add_raw_audit_columns(raw_df):
        """ Add the audit columns for the input dataframe."""
        raw_df = (
            raw_df
            .withColumn("src_is_current", lit(True))
            .withColumn("src_start_date", lit(current_date))
            .withColumn("src_end_date", lit(high_date))
        ) 
        return raw_df

    def full_merge(self, raw_df, scd_df):
        """ purpose     : Perform full outer join on RAW and SCD
            Expected    : raw_df should have its column renamed with 'src_' prefixed
            Result      : mereged dataframe with additional column 'action' which expalins type of action
                          to be performed on the record (INSERT, UPSERT, DELETE, NOACTION)     
        """
        new_raw_df = ScdComputation.add_raw_audit_columns(raw_df)

        condition = [col("left.primarykey") == col("right.src_primarykey"),
                     col("left.end_date") == col("right.src_end_date")]
        merged_df = (
            scd_df.alias("left")
            .join(new_raw_df.alias("right"), condition, how='fullouter')
        )

        #Derive new column to indicate the action
        merged_df = (
            merged_df
            .withColumn(
                'action',
                when(merged_df.src_recordhash != merged_df.recordhash, 'UPSERT')
                .when(merged_df.primarykey.isNull(), 'INSERT')
                .otherwise('NOACTION')
            )
        )
        return merged_df

    def unchanged_records(self, merged_df, scd_df_columns):
        """Return unchanged records."""
        unchanged_df = (
            merged_df.filter(merged_df.action == 'NOACTION')
            .select(scd_df_columns)
        )
        return unchanged_df

    def insert_new_records(self, merged_df, raw_df_columns):
        """Return new records."""
        ##Column names should be renamed before merge by removing src_
        latest_df = (
            merged_df.filter(merged_df.action == 'INSERT')
            .withColumn("src_is_current", lit(True))
            .select(raw_df_columns)
        )
        return latest_df

    def update_raw_records(self, merged_df, raw_df_columns):
        """Return the raw records to be updated."""
        ##Column names should be renamed before merge by removing src_
        update_raw_df = (
            merged_df.filter(merged_df.action == 'UPSERT')
            .withColumn("src_is_current", lit(True))
            .select(raw_df_columns)
        )
        return update_raw_df

    def update_scd_records(self, merged_df, scd_df_columns):
        """Return the SCD recorsd to be updates."""
        update_scd_records = (
            merged_df.filter(merged_df.action == 'UPSERT')
            .withColumn("end_date", date_sub(merged_df.src_start_date, 1))
            .withColumn("is_current", lit(False))
            .select(scd_df_columns)
        )
        return update_scd_records

    def scd1_records(self, raw_df, scd_df):
        """Return the scd1 computed records."""
        condition = [col("left.primarykey") == col("right.primarykey"),
                     col("left.recordhash") == col("right.recordhash")]
        final_df = (
            raw_df.alias("left")
            .join(scd_df.alias("right"), condition, "leftanti")
            .select("left.*")
        )
        return final_df

            