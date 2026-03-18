import streamlit as st
import pandas as pd
import sqlite3
import os
import json
from datetime import datetime

# ----------------------
# CONFIGURATION
# ----------------------
DB_PATH = "data/processed/multimodal_etl.db"
LOGS_PATH = "logs"  # dossier avec les logs transformation
st.set_page_config(page_title="Dashboard ETL Multimodal", layout="wide")

st.title("Dashboard ETL Multimodal - KPI")

# ----------------------
# CHARGER LES DONNEES
# ----------------------
if os.path.exists(DB_PATH):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM articles", conn)
    conn.close()
else:
    st.error(f"Base SQLite non trouvée : {DB_PATH}")
    st.stop()

# ----------------------
# KPI CALCULS
# ----------------------
total_articles = len(df)
valid_articles = df.dropna(subset=["text", "image_url"])
num_valid = len(valid_articles)
percent_valid = (num_valid / total_articles * 100) if total_articles > 0 else 0

# Duplicatas
num_duplicates = total_articles - df["url"].nunique()
percent_duplicates = (num_duplicates / total_articles * 100) if total_articles > 0 else 0

# Disponibilité image
num_images_valid = valid_articles["image_url"].apply(lambda url: bool(url)).sum()
percent_images_valid = (num_images_valid / total_articles * 100) if total_articles > 0 else 0

# Nombre d'articles par source/domain
articles_per_domain = valid_articles["domain"].value_counts()


# ----------------------
# Temps d'exécution par tâche depuis logs Airflow (correct)
# ----------------------
import glob

task_times = {}
DAG_LOGS_PATH = "airflow_home/logs/dag_id=multimodal_etl"

# Lister les exécutions les plus récentes
run_dirs = sorted(glob.glob(os.path.join(DAG_LOGS_PATH, "run_id=*")), reverse=True)[:5]

for run_dir in run_dirs:
    task_dirs = [d for d in glob.glob(os.path.join(run_dir, "task_id=*")) if os.path.isdir(d)]
    for task_dir in task_dirs:
        # Récupérer tous les fichiers .log
        log_files = sorted(glob.glob(os.path.join(task_dir, "*.log")))
        if not log_files:
            continue

        # Lire toutes les lignes JSON de chaque fichier
        all_lines = []
        for log_file in log_files:
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:  # éviter lignes vides
                        try:
                            all_lines.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue  # ignorer lignes non JSON

        if not all_lines:
            continue

        start_time = datetime.fromisoformat(all_lines[0]["timestamp"].replace("Z", "+00:00"))
        end_time = datetime.fromisoformat(all_lines[-1]["timestamp"].replace("Z", "+00:00"))

        task_name = os.path.basename(task_dir).replace("task_id=", "")
        task_times[task_name] = (end_time - start_time).total_seconds()
        
# Nombre d'erreurs
num_errors = 0
for logfile in os.listdir(LOGS_PATH):
    if logfile.endswith(".log"):
        with open(os.path.join(LOGS_PATH, logfile), "r", encoding="utf-8") as f:
            num_errors += sum(1 for line in f if "ERROR" in line)

# ----------------------
# AFFICHAGE KPI
# ----------------------
st.subheader("Résumé général")
col1, col2, col3 = st.columns(3)
col1.metric("Articles totaux", total_articles)
col2.metric("Articles valides (%)", f"{percent_valid:.1f}%")
col3.metric("Articles doublons (%)", f"{percent_duplicates:.1f}%")

col1, col2 = st.columns(2)
col1.metric("Images valides (%)", f"{percent_images_valid:.1f}%")
col2.metric("Nombre d'erreurs", num_errors)

print(task_times)
if task_times:
    for task, t in task_times.items():
        st.metric(f"Temps exécution {task} (s)", f"{t:.1f}")

# ----------------------
# GRAPHIQUES
# ----------------------
st.subheader("Distribution longueur texte")
st.bar_chart(valid_articles["text_length"])

st.subheader("Répartition par domaine/source")
st.bar_chart(articles_per_domain)

st.subheader("Répartition par disponibilité d'image")
image_status = valid_articles["image_url"].apply(lambda x: "Valide" if x else "Manquant").value_counts()
st.bar_chart(image_status)