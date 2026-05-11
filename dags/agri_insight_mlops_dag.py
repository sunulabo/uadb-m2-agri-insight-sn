# dags/agri_insight_mlops_dag.py — DAG MLOps Agri-Insight SN
# Réentraînement hebdomadaire + détection de drift + recommandations
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess, logging

logger = logging.getLogger("agri_insight_dag")

default_args = {
    "owner": "seye_ahmed",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["agri-insight@sonatel.sn"],
}

def compute_drift(**ctx):
    """Détecte le drift : compare RMSE courant vs RMSE de référence."""
    from pyhive import hive
    try:
        conn = hive.Connection(host="hive-metastore", port=10000)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
            AVG(ABS(rendement_predit - rendement_kg_ha)) AS mae_courant
            FROM agri_insight.predictions_gold g
            JOIN agri_insight.parcelles_silver s
            ON g.plot_id_secure = s.plot_id_secure
            WHERE g.prediction_ts >= DATE_SUB(CURRENT_DATE, 7)
        """)
        row = cursor.fetchone()
        mae = row[0] if row and row[0] is not None else 0.0
        logger.info(f"MAE courante : {mae:.2f} kg/ha")
        ctx["ti"].xcom_push(key="mae", value=mae)
        # Seuil de drift : MAE > 200 kg/ha → réentraîner
        return "retrain_model" if mae > 200 else "generate_recommendations"
    except Exception as e:
        logger.error(f"Erreur drift detection: {e}")
        return "generate_recommendations"

def retrain_model(**ctx):
    """Lance le réentraînement Spark via spark-submit."""
    result = subprocess.run([
        "spark-submit", "--master", "spark://spark-master:7077",
        "/opt/airflow/scripts/train_yield.py",
        "--mode", "train",
        "--input-table", "agri_insight.parcelles_silver",
    ], capture_output=True, text=True, timeout=3600)
    if result.returncode != 0:
        raise Exception(f"Spark train failed: {result.stderr[-500:]}")
    logger.info("Modèle réentraîné avec succès ✓")

def generate_recommendations(**ctx):
    """Lance l'inférence et écrit les recommandations dans HBase."""
    from pyhive import hive
    import happybase
    from datetime import datetime
    # 1. Lancer inférence Spark
    subprocess.run([
        "spark-submit", "--master", "spark://spark-master:7077",
        "/opt/airflow/scripts/train_yield.py",
        "--mode", "infer",
    ], check=True, timeout=1800)
    # 2. Écrire les recommandations dans HBase
    try:
        conn_hbase = happybase.Connection("hbase", port=9090)
        conn_hbase.open()
        conn_hive = hive.Connection(host="hive-metastore", port=10000)
        cursor = conn_hive.cursor()
        cursor.execute("""
            SELECT region, type_sol, culture,
            rendement_moyen_predit, statut_recommandation
            FROM agri_insight.vue_recommandations
            WHERE statut_recommandation = 'CULTURE_OPTIMALE'
        """)
        table = conn_hbase.table(b"agri:recommandations")
        ts = datetime.utcnow().isoformat()
        for row in cursor.fetchall():
            region, sol, culture, rend, statut = row
            key = f"{region}_{sol}_{culture}".encode()
            table.put(key, {
                b"meta:region": region.encode(),
                b"meta:type_sol": sol.encode(),
                b"predict:culture": culture.encode(),
                b"predict:rendement": str(round(rend,1)).encode(),
                b"conseil:statut": statut.encode(),
                b"conseil:ts": ts.encode(),
            })
        conn_hbase.close()
        logger.info("Recommandations HBase mises à jour ✓")
    except Exception as e:
        logger.error(f"Erreur recommandations HBase: {e}")

with DAG("agri_insight_mlops",
    default_args=default_args,
    description="MLOps Agri-Insight SN — drift + recommandations",
    schedule_interval="0 3 * * 1", # Chaque lundi 3h
    start_date=days_ago(1), catchup=False,
    tags=["agri-insight","mlops","securite-alimentaire"]) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    t_drift = BranchPythonOperator(task_id="compute_drift",
        python_callable=compute_drift,
        provide_context=True)
    
    t_train = PythonOperator(task_id="retrain_model",
        python_callable=retrain_model,
        provide_context=True)
    
    t_reco = PythonOperator(task_id="generate_recommendations",
        python_callable=generate_recommendations,
        provide_context=True)

    start >> t_drift >> [t_train, t_reco]
    t_train >> t_reco >> end
    t_drift >> t_reco # Path if no drift
