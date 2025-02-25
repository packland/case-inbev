from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from datetime import datetime
from docker.types import Mount
import os

# Caminho dos scripts mapeado
scripts_path_at_spark = "/opt/bitnami/spark/scripts"
lake_path_at_spark = "/opt/bitnami/spark/data_lake"
full_path_to_scripts = os.getenv('SCRIPTS_DIR')
full_path_to_lake = os.getenv('DATA_LAKE_DIR')

with DAG('process_etl_brewery', start_date=datetime(2025, 2, 23), schedule_interval=None) as dag:

    # Tarefa para executar get_bronze_data.py
    get_bronze_data = DockerOperator(
        task_id='get_bronze_data',
        image='delta-spark',
        command=f"python {scripts_path_at_spark}/get_bronze_data.py",
        mounts=[Mount(source=full_path_to_scripts, target=scripts_path_at_spark, type='bind'),
                Mount(source=full_path_to_lake, target=lake_path_at_spark, type='bind')],
        mount_tmp_dir=False,
        dag=dag,
    )

    # Tarefa para executar get_silver_data.py
    get_silver_data = DockerOperator(
        task_id='get_silver_data',
        image='delta-spark',
        command=f"python {scripts_path_at_spark}/get_silver_data.py",
        mounts=[Mount(source=full_path_to_scripts, target=scripts_path_at_spark, type='bind'),
                Mount(source=full_path_to_lake, target=lake_path_at_spark, type='bind')],
        mount_tmp_dir=False,
        dag=dag,
    )

    # Tarefa para executar get_gold_data.py
    get_gold_data = DockerOperator(
        task_id='get_gold_data',
        image='delta-spark',
        command=f"python {scripts_path_at_spark}/get_gold_data.py",
        mounts=[Mount(source=full_path_to_scripts, target=scripts_path_at_spark, type='bind'),
                Mount(source=full_path_to_lake, target=lake_path_at_spark, type='bind')],
        mount_tmp_dir=False,
        dag=dag,
    )

    # Definir a ordem de execução das tarefas
    get_bronze_data >> get_silver_data >> get_gold_data
