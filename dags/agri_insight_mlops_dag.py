from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import subprocess

default_args = {
    'owner': 'agri-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def compute_drift(**ctx):
    # Simulation de drift (à remplacer par vraie métrique)
    import random
    mae = random.uniform(150, 250)
    ctx['ti'].xcom_push(key='mae', value=mae)
    return 'retrain_model' if mae > 200 else 'generate_recommendations'

def retrain_model(**ctx):
    subprocess.run([
        'spark-submit', '--master', 'spark://spark-master:7077',
        '/opt/scripts/yield_prediction.py', '--mode', 'train'
    ], check=True, timeout=3600)

def generate_recommendations(**ctx):
    # Lance l'inférence puis écrit dans HBase
    subprocess.run([
        'spark-submit', '--master', 'spark://spark-master:7077',
        '/opt/scripts/yield_prediction.py', '--mode', 'infer'
    ], check=True, timeout=1800)
    # Ici on pourrait ajouter l'insertion HBase avec happybase

with DAG('agri_insight_mlops', default_args=default_args,
         schedule_interval='0 3 * * 1', catchup=False, tags=['agri-insight']) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    drift = BranchPythonOperator(task_id='compute_drift', python_callable=compute_drift, provide_context=True)
    train = PythonOperator(task_id='retrain_model', python_callable=retrain_model, provide_context=True)
    reco = PythonOperator(task_id='generate_recommendations', python_callable=generate_recommendations, provide_context=True)
    start >> drift
    drift >> train >> reco >> end
    drift >> reco >> end
