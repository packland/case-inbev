from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from datetime import datetime
from docker.types import Mount
import os

# Caminho dos scripts mapeado
local_path_at_spark = "/opt/bitnami/spark/scripts" 
full_path_to_scripts= full_path_to_scripts = os.getenv('SCRIPTS_DIR')


with DAG('spark_test', start_date=datetime(2025, 2, 23), schedule_interval=None) as dag:

    # Tarefa Spark
    spark_submit = DockerOperator(
        task_id='spark_submit',
        image='bitnami/spark:3.5',
        command=f"spark-submit --master local {local_path_at_spark}/pi.py",  # Caminho para o script dentro do contêiner
        mounts=[Mount(source=full_path_to_scripts, target=local_path_at_spark, type='bind')],  # O volume que você já tem configurado
        mount_tmp_dir=False,
        dag=dag,
    )
