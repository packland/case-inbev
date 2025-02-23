from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, current_date, lit
from delta.tables import DeltaTable
from config import Settings
import os

class SilverLayerBrewerySC2:
    def __init__(self):
        self.settings = Settings()
        self.spark = SparkSession.builder \
            .appName("SilverLayerBrewerySC2") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        self.lake_dir = self.settings.DATA_LAKE_DIR
        self.bronze_path = f"{self.lake_dir}/bronze/"
        self.silver_path = f"{self.lake_dir}/silver/"
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

            # Verificar se a tabela Delta já existe
            if not DeltaTable.isDeltaTable(self.spark, self.silver_path):
                # Se não existir, criar uma nova tabela Delta
                df_new.write.format("delta").save(self.silver_path)
                print("✅ Tabela Delta criada com sucesso!")

            # Carregar dados existentes da Silver Layer (se já existir)
            delta_silver = DeltaTable.forPath(self.spark, self.silver_path)

            # Criar uma condição para identificar mudanças dinamicamente
            update_condition = "delta.id = new.id AND ("
            for col in self.columns:
                update_condition += f"delta.{col} <> new.{col} OR "
            update_condition = update_condition.rstrip(" OR ") + ")"

            # Upsert SC2
            delta_silver.alias("delta") \
                .merge(df_new.alias("new"), "delta.id = new.id") \
                .whenMatchedUpdate(
                    condition=update_condition,
                    set={
                        "end_date": current_date(),
                        "is_active": lit(False)
                    }
                ) \
                .whenNotMatchedInsertAll() \
                .execute()

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
