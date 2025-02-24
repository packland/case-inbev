from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, current_date, lit, col  # Adicione 'col' aqui
from config import Settings
import os

class SilverLayerBrewerySC2:
    def __init__(self):
        self.settings = Settings()
        self.spark = SparkSession.builder \
            .appName("SilverLayerBrewerySC2") \
            .getOrCreate()
        self.lake_dir = self.settings.DATA_LAKE_DIR
        self.bronze_path = f"{self.lake_dir}/bronze/"
        self.silver_path = f"{self.lake_dir}/silver/brewery/"  # Modifiquei para incluir o subdiretório 'brewery'
        self.columns = ["name", "brewery_type", "street", "address_1", "address_2", "address_3", "city", "state", "state_province", "postal_code", "country", "longitude", "latitude", "phone", "website_url"]

    def process(self, file_name):
        try:
            # Ler a última carga da Bronze Layer
            df_bronze = self.spark.read.json(os.path.join(self.bronze_path, file_name))

            # Limpeza e normalização
            select_expr = [trim(col("id")).alias("id")]
            for c in self.columns:
                select_expr.append(trim(col(c)).alias(c))

            df_new = df_bronze.select(select_expr).dropDuplicates()

            # Adicionar colunas SC2
            df_new = df_new.withColumn("start_date", current_date()) \
                           .withColumn("end_date", lit(None).cast("date")) \
                           .withColumn("is_active", lit(True))

            # Verificar se a tabela Parquet já existe
            if not os.path.exists(self.silver_path):
                # Se não existir, criar uma nova tabela Parquet
                df_new.write.parquet(self.silver_path)
                print("✅ Tabela Parquet criada com sucesso!")

            # Carregar dados existentes da Silver Layer (se já existir)
            df_silver = self.spark.read.parquet(self.silver_path)

            # Criar uma condição para identificar mudanças dinamicamente
            update_condition = " OR ".join([f"silver.{col} <> new.{col}" for col in self.columns])

            # Upsert SC2
            df_merged = df_silver.alias("silver").join(df_new.alias("new"), "id", "outer") \
                .selectExpr(
                    "coalesce(silver.id, new.id) as id",
                    *[f"coalesce(new.{col}, silver.{col}) as {col}" for col in self.columns],
                    "coalesce(new.start_date, silver.start_date) as start_date",
                    "coalesce(new.end_date, silver.end_date) as end_date",
                    "coalesce(new.is_active, silver.is_active) as is_active"
                )

            df_merged.write.mode("overwrite").parquet(self.silver_path)
            print("✅ Silver Layer com SC2 processada com sucesso!")
            return True
        except Exception as e:
            print(f"❌ Erro ao processar Silver Layer com SC2: {e}")
            return False

# Exemplo de uso
if __name__ == "__main__":
    file_name = "2025_02_23_16_26_51_breweries.json"  # Substitua pelo nome do arquivo que deseja processar
    processor = SilverLayerBrewerySC2()
    success = processor.process(file_name)
    if success:
        print("Processamento concluído com sucesso.")
    else:
        print("Processamento falhou.")
