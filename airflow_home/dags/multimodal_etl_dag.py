from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import sys
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
sys.path.append(os.path.join(os.getcwd(), "src"))
# Importer tes scripts existants
from extract import newsdata_extractor, newsapi_extractor, fakenewsnet_extractor, afp_factuel_extractor
from transform import transform_pipeline

# Chemin de la base SQLite
SQLITE_DB_PATH = os.path.join(os.getcwd(), "data/processed/multimodal_etl.db")

default_args = {
    "owner": "Paul Lesage",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 3),
    "retries": 1,
}

def load_to_sqlite():
    """Charge le dataset final JSON dans SQLite"""
    cleaned_json_path = os.path.join("data/processed/cleaned_dataset.json")
    if not os.path.exists(cleaned_json_path):
        raise FileNotFoundError(f"{cleaned_json_path} introuvable. Transform pipeline à exécuter d'abord.")

    df = pd.read_json(cleaned_json_path)
    os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)

    from sqlalchemy import create_engine
    engine = create_engine(f"sqlite:///{SQLITE_DB_PATH}", echo=False)
    df.to_sql("articles", con=engine, if_exists="replace", index=False)
    print(f"{len(df)} articles chargés dans SQLite: {SQLITE_DB_PATH}")

# ---------------------
# DAG
# ---------------------
with DAG(
    "multimodal_etl",
    default_args=default_args,
    schedule=None,  # manuel
    catchup=False,
    description="Pipeline ETL multimodal texte+image avec SQLite",
) as dag:

    task_extract_newsdata = PythonOperator(
        task_id="extract_newsdata",
        python_callable=newsdata_extractor.main
    )

    task_extract_newsapi = PythonOperator(
        task_id="extract_newsapi",
        python_callable=newsapi_extractor.main
    )

    task_extract_fakenewsnet = PythonOperator(
        task_id="extract_fakenewsnet",
        python_callable=fakenewsnet_extractor.main
    )

    task_extract_afp = PythonOperator(
        task_id="extract_afp",
        python_callable=afp_factuel_extractor.main
    )

    task_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_pipeline.main
    )

    task_load_sqlite = PythonOperator(
        task_id="load_to_sqlite",
        python_callable=load_to_sqlite
    )

    # ---------------------
    # Dépendances
    # ---------------------
    [task_extract_newsdata, task_extract_newsapi, task_extract_fakenewsnet, task_extract_afp] >> task_transform >> task_load_sqlite