from classes.bronze_breweries import FetchBreweries
import time
import sys

fetcher = FetchBreweries()

success = False
for attempt in range(3):
    try:
        fetcher.execute()
        success = True
        break
    except Exception as e:
        print(f"Tentativa {attempt + 1} falhou: {e}")
        time.sleep(10)

if success:
    print("Execução bem-sucedida")
    sys.exit(0)
else:
    #Insira aqui uma lógica de erro. Alerta no slack ou algo do tipo. 

    print("Execução falhou após 3 tentativas")
    sys.exit(1)