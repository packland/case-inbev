from classes.silver_breweries import SilverLayerBrewerySC2
from classes.log_tracking import LogTracking
from config import Settings
import os
import sys

# Instanciar as classes necessárias
settings = Settings()
log_tracker = LogTracking()
silver_layer = SilverLayerBrewerySC2()

# Obter a última execução
last_execution = log_tracker.return_last_execution()
last_execution_file = last_execution['nome_do_arquivo'] if last_execution else None

# Caminho do diretório de arquivos
data_lake_dir = os.path.join(settings.DATA_LAKE_DIR, 'bronze')

# Obter a lista de arquivos e ordená-los alfabeticamente
files = []
for root, dirs, filenames in os.walk(data_lake_dir):
    for filename in filenames:
        files.append(filename)
files.sort()

# Variável para rastrear o sucesso da execução
execution_success = True

# Iterar sobre os arquivos ordenados
for file_name in files:
    # Verificar se o arquivo é posterior ao último arquivo processado
    if last_execution_file is None or file_name > last_execution_file:
        try:
            # Remover a extensão do nome do arquivo
            file_name_without_extension = os.path.splitext(file_name)[0]
            # Executar o método process
            silver_layer.process(file_name_without_extension, "country")
            print(f"Processado com sucesso: {file_name}")
        except Exception as e:
            print(f"Erro ao processar {file_name}: {e}")
            execution_success = False

# Adicionar nova execução ao log
log_tracker.add_new_execution('get_silver_data.py')

# Lançar sucesso ou erro para o sistema
if execution_success:
    print("Execução bem-sucedida")
    sys.exit(0)
else:
    print("Execução falhou")
    sys.exit(1)

