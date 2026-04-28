"""
DAG: b3_ingest_daily
Ingestão diária de cotações da B3 via API brapi.dev
Camada: Bronze
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {        # Argumentos padrão para as tarefas do DAG
    "owner": "wendel",  # Responsável pelo DAG
    "depends_on_past": False, # Não depende da execução anterior
    "email_on_failure": False, # Não enviar email em caso de falha
    "retries": 2, # Número de tentativas em caso de falha
    "retry_delay": timedelta(minutes=5), # Tempo entre tentativas
}

with DAG(
    dag_id="b3_ingest_daily", # Identificador único do DAG
    description="Ingestão diária de cotações da B3 (bronze layer)", # Descrição do DAG
    default_args=default_args, # Argumentos padrão para as tarefas
    start_date=datetime(2026, 1, 1), # Data de início do DAG
    schedule="0 19 * * 1-5",  # 19:00 UTC = 16:00 BRT, dias úteis
    catchup=False, # Não executar para datas anteriores à data de início
    tags=["b3", "bronze", "ingest"],#` Tags para organização e busca do DAG
) as dag:# Contexto do DAG, todas as tarefas definidas dentro deste bloco pertencerão a este DAG

    ingest_cotacoes = BashOperator( # Tarefa para executar o script de ingestão
        task_id="ingest_cotacoes_b3", # Identificador único da tarefa
        bash_command="python /opt/airflow/scripts/ingest/ingest_b3.py", # Comando para executar o script de ingestão
        doc_md="Busca cotações dos principais papéis da B3 e salva como JSON particionado por data", #` Documentação da tarefa em Markdown
    )
    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command="python /opt/airflow/scripts/transform/transform_silver.py",
        doc_md="Lê Bronze, limpa e salva em Parquet na camada Silver",
    )

    transform_gold = BashOperator(
        task_id="transform_gold",
        bash_command="python /opt/airflow/scripts/transform/transform_gold.py",
        doc_md="Lê Silver, calcula métricas e salva na camada Gold",
    )

    ingest_cotacoes >> transform_silver >> transform_gold
    
    