from classes.gold_breweries import GoldLayerBrewery
import sys

# Instanciar a classe GoldLayerBrewery
gold_layer = GoldLayerBrewery()

# Variável para rastrear o sucesso da execução
execution_success = True

try:
    # Executar o método process
    gold_layer.process()
    print("Processamento concluído com sucesso")
except Exception as e:
    print(f"Erro ao processar os dados: {e}")
    execution_success = False

# Lançar sucesso ou erro para o sistema
if execution_success:
    sys.exit(0)
else:
    sys.exit(1)