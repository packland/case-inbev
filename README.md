# Case InBev: Análise de Cervejarias com Apache Spark e Airflow

## Introdução

Este repositório contém a solução para o case proposto, que envolve a extração, transformação e carregamento (ETL) de dados de cervejarias da API Open Brewery DB (<https://api.openbrewerydb.org/breweries>) utilizando Apache Spark e Apache Airflow. O projeto implementa uma arquitetura de dados em medalhão (Bronze, Silver e Gold) para processar e refinar os dados.

## Objetivo

O objetivo principal é obter dados da API, processá-los e organizá-los em uma estrutura de *Data Lake* em camadas (medalhão). A camada final (Gold) consiste em uma tabela agregada que contabiliza o número de estabelecimentos por tipo e localização.

## Arquitetura e Organização do Projeto

O projeto foi desenvolvido utilizando Apache Airflow e Apache Spark, ambos executados em contêineres Docker, conforme as especificações do case. A estrutura de diretórios é organizada da seguinte forma:

*   **`airflow/`**: Contém os arquivos de configuração e dados do Airflow (DAGs, logs, etc.).
*   **`data_lake/`**:  Armazena os dados nas diferentes camadas do *Data Lake* (Bronze, Silver e Gold).
*   **`scripts/`**:  Contém os scripts Python responsáveis pelas etapas de ETL.
*  **`Dockerfile.spark`**: Arquivo para build da imagem docker do Spark.
*  **`docker-compose.yml`**: Arquivo para orquestrar os containers do Airflow e Spark.

## Scripts de ETL (Extração, Transformação e Carregamento)

A lógica de ETL foi implementada em Python, buscando maximizar a utilização de classes e princípios SOLID (Single Responsibility, Open/Closed, Liskov Substitution, Interface Segregation, Dependency Inversion) para um código mais limpo, modular e manutenível, dentro do razoável para um projeto pequeno. A DAG do Airflow foi mantida o mais enxuta possível, focando na orquestração das tarefas.

Os scripts Python principais são:

*   **`get_bronze_data.py`**: Responsável por extrair os dados brutos da API.
*   **`get_silver_data.py`**: Processa os dados da camada Bronze, aplicando transformações e salvando-os na camada Silver.
*   **`get_gold_data.py`**: Agrega os dados da camada Silver para gerar a tabela final na camada Gold.

Esses scripts controlam o fluxo de execução, incluindo *loops*, tratamento de erros, *retries*, etc.

A interação com o Spark é encapsulada em classes separadas, localizadas no diretório `scripts/classes`:

*   **`bronze_breweries.py`**:  Realiza as requisições à API Open Brewery DB.
*   **`silver_breweries.py`**:
    *   Lê os dados da camada Bronze.
    *   Consulta um log de execuções (`log_tracking.py`) para identificar o último arquivo processado.
    *   Itera sobre os arquivos mais recentes, aplicando as transformações.
    *   Salva os dados na camada Silver utilizando o padrão *Slowly Changing Dimension* tipo 2 (SCD2). Isso permite rastrear o histórico completo de alterações nos dados e recriar o estado do *Data Lake* em qualquer ponto no tempo (tabela idempotente).
*   **`gold_breweries.py`**:  Cria a tabela agregada da camada Gold a partir dos dados da camada Silver.
*   **`log_tracking.py`**:  Gerencia o log de execuções, registrando o progresso do processamento.
*   **`helper_sc2.py`**:  Classe auxiliar (com créditos ao autor original referenciado no arquivo) que implementa as transformações necessárias para o padrão SCD2.

## Como Executar o Projeto

1.  **Configuração do Ambiente:**

    *   Crie um arquivo `.env` na raiz do projeto. Defina as seguintes variáveis de ambiente (ajuste os valores conforme o seu ambiente):

        ```
        AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5
        AIRFLOW_UID=50000
        AIRFLOW_PROJ_DIR=./airflow
        AIRFLOW_PLUGINS_DIR=./airflow/plugins
        SCRIPTS_DIR=[Seu_caminho_para/case-inbev/scripts]
        DATA_LAKE_DIR=[Seu_caminho_para/case-inbev/data_lake]
        _AIRFLOW_WWW_USER_USERNAME=airflow
        _AIRFLOW_WWW_USER_PASSWORD=airflow
        _PIP_ADDITIONAL_REQUIREMENTS=
        FETCH_URL=https://api.openbrewerydb.org/breweries
        ```
        Substitua `[Seu_caminho_para/...] ` com os paths absolutos corretos.

2.  **Construção da Imagem Docker do Spark:**

    *   Execute o seguinte comando na raiz do projeto para construir a imagem Docker customizada do Spark:

        ```bash
        docker build -t delta-spark -f Dockerfile.spark .
        ```

3.  **Execução do Docker Compose:**

    *   Inicie os contêineres do Airflow e do Spark com o seguinte comando:

        ```bash
        docker-compose up
        ```

4.  **Acesso ao Airflow:**

    *   Após a inicialização, acesse a interface web do Airflow em `http://localhost:8080`.
    *   Utilize as credenciais definidas no arquivo `.env` (airflow/airflow) para fazer login.
    *   Localize a DAG `process_etl_brewery` e execute-a.

## Monitoramento e Qualidade de Dados

A separação entre a lógica de execução das tarefas e a interação com o Spark (nas classes `get_[camada]_data.py`) facilita a implementação de recursos como:

*   **Alertas:** Monitoramento de erros e envio de notificações.
*   ***Retries***: Tentativas automáticas de reexecução em caso de falhas.

É recomendado implementar esses recursos nos arquivos `get_[camada]_data.py`, exceto para erros específicos do próprio Airflow.

Para garantir a qualidade dos dados, seria ideal:

*   Criar uma classe de validação de dados.
*   Implementar métodos de teste para verificar:
    *   Campos obrigatórios.
    *   Tipos de dados válidos.
    *   Outras regras de negócio.
* No meu trabalho atual, utilizamos macros do DBT (Data Build Tool) para realizar essas validações, uma abordagem que pode ser adaptada a este projeto.

## Considerações Finais

Este projeto demonstra uma solução completa para o case, abordando desde a extração dos dados até a criação de uma camada agregada para análise.  A utilização de Docker, Airflow e Spark proporciona escalabilidade, confiabilidade e facilidade de manutenção.  A implementação de SCD2 e a separação de responsabilidades em classes contribuem para a qualidade e a robustez do código.  Além disso, foram apresentadas sugestões para aprimorar o monitoramento e a qualidade dos dados, garantindo a integridade e a confiabilidade das informações processadas.
