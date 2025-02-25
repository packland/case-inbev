from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from config import Settings
import os

class GoldLayerBrewery:
    def __init__(self):
        self.settings = Settings()
        self.spark = SparkSession.builder.appName("GoldLayerBrewery_SQL").getOrCreate()
        self.lake_dir = self.settings.DATA_LAKE_DIR
        self.silver_path = os.path.join(self.lake_dir, "silver", "brewery")
        self.gold_path = os.path.join(self.lake_dir, "gold", "brewery")
        self.gold_temp_path = os.path.join(self.lake_dir, "gold", "brewery_temp")

    def process(self):
        # Ler os dados do parquet em silver_path
        df = self.spark.read.parquet(self.silver_path)

        # Filtrar por flag = 1
        df_filtered = df.filter(col("flag") == 1)

        # Agregar a quantidade de breweries por tipo e local
        df_aggregated = df_filtered.groupBy("brewery_type", "country").agg(count("id").alias("brewery_count"))

        # Salvar o resultado em gold_temp_path
        df_aggregated.write.mode("overwrite").parquet(self.gold_temp_path)

        # Apagar a tabela original se existir
        if os.path.exists(self.gold_path):
            os.rmdir(self.gold_path)

        # Renomear a tabela _temp para a tabela original
        os.rename(self.gold_temp_path, self.gold_path)

        print("Processamento concluído com sucesso")