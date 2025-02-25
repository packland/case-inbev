from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit, to_date
from config import Settings
from .helper_sc2 import SCDHandler
import os
import shutil

class SilverLayerBrewerySC2:
    def __init__(self):
        self.settings = Settings()
        self.spark = SparkSession.builder.appName("SilverLayerBrewerySC2_SQL").getOrCreate()
        self.lake_dir = self.settings.DATA_LAKE_DIR
        self.bronze_path = os.path.join(self.lake_dir, "bronze")
        self.silver_path = os.path.join(self.lake_dir, "silver", "brewery")
        self.scd_handler = SCDHandler()

    def process(self, file_name: str = None, partition_column: str = None):
        """
        Reads a JSON, applies SCD Type 2, and saves as a partitioned Parquet.
        Uses a temp Parquet, then renames.

        Args:
            file_name (str): JSON file name (without .json).
            partition_column (str, optional): Column to partition by.
                                             Defaults to None (no partitioning).
        """
        if not file_name:
            raise ValueError("file_name must be provided.")

        json_file_path = os.path.join(self.bronze_path, f"{file_name}.json")
        temp_silver_path = self.silver_path + "_temp"

        try:
            source_df = self.spark.read.json(json_file_path)

            try:
                target_df = self.spark.read.parquet(self.silver_path)
            except Exception as e:
                target_df = None

            join_keys = ['id']
            metadata_cols = ['eff_start_date', 'eff_end_date', 'flag']

            if target_df:
                result_df = self.scd_handler.scd_2(source_df, target_df, join_keys, metadata_cols)
            else:
                result_df = source_df.withColumn("eff_start_date", current_date()) \
                                     .withColumn("eff_end_date", to_date(lit(None))) \
                                     .withColumn("flag", lit(1))

            # Write to the temporary Parquet file, with partitioning
            if partition_column:
                result_df.write.mode("overwrite").partitionBy(partition_column).parquet(temp_silver_path)
            else:
                result_df.write.mode("overwrite").parquet(temp_silver_path)

            # Delete original (if exists)
            if os.path.exists(self.silver_path):
                shutil.rmtree(self.silver_path)

            # Rename temp to original
            os.rename(temp_silver_path, self.silver_path)

            print(f"Successfully processed {file_name}, saved to {self.silver_path}")
            return True

        except Exception as e:
            print(f"Error processing {file_name}: {e}")
            raise
            return False