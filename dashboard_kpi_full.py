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
#num_images_valid = valid_articles["image_url"].apply(lambda url: bool(url)).sum()
num_images_valid = len(valid_articles[valid_articles["image_url"].notna()])
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
# Drop Menu BDD
# ----------------------
st.sidebar.title(" Explorer les données")
option = st.sidebar.selectbox(
    "Choisir une vue :",
    ["Dashboard KPI", "Aperçu table", "Filtrer par source", "Détail article"]
)

if option == "Dashboard KPI":
    #print(task_times)
    if task_times:
        task_list = sorted(task_times.items(), key=lambda x: x[1], reverse=True)[:8]
        
        # LIGNE 1
        cols1 = st.columns(4)
        for i in range(min(4, len(task_list))):
            task, t = task_list[i]
            cols1[i].metric(task, f"{t:.1f}s")
        
        # LIGNE 2
        if len(task_list) > 4:
            cols2 = st.columns(4)
            for i in range(4, len(task_list)):
                task, t = task_list[i]
                cols2[i-4].metric(task, f"{t:.1f}s")
    # ----------------------
    # AFFICHAGE KPI EN LIGNES
    # ----------------------
    st.subheader("Résumé général")

    # Ligne 1 : 4 KPI
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Articles totaux", total_articles)
    col2.metric("Valides %", f"{percent_valid:.1f}%")
    col3.metric("Doublons %", f"{percent_duplicates:.1f}%")
    col4.metric("Erreurs", num_errors, delta_color="inverse")

    # Ligne 2 : Images + bouton erreurs
    col5, col6 = st.columns([3,1])
    col5.metric("Images OK %", f"{percent_images_valid:.1f}%")

    # Bouton erreurs (CORRIGÉ)
    with col6:
        show_errors = st.checkbox(" Afficher les erreurs", key="show_errors_cb")
        if show_errors and num_errors > 0:
            st.subheader(" Logs d'erreurs (20 dernières)")
    
    if show_errors and num_errors > 0:
        with st.expander("20 dernières erreurs"):
            errors_display = []
            for logfile in os.listdir(LOGS_PATH):
                if logfile.endswith(".log"):
                    filepath = os.path.join(LOGS_PATH, logfile)
                    try:
                        with open(filepath, "r", encoding="utf-8") as f:
                            errors = [line.strip() for line in f if "ERROR" in line][-10:]
                            if errors:
                                errors_display.extend(errors)
                    except:
                        continue
            
            for error_line in errors_display[-20:]:
                st.code(error_line)
    if st.session_state.get("show_errors", False) and num_errors > 0:
        st.subheader(" Logs d'erreurs (20 dernières)")
        errors_display = []
        for logfile in os.listdir(LOGS_PATH):
            if logfile.endswith(".log"):
                filepath = os.path.join(LOGS_PATH, logfile)
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        errors = [line.strip() for line in f if "ERROR" in line][-10:]
                        errors_display.extend(errors)
                except:
                    continue
        
        for error_line in errors_display[-20:]:
            st.code(error_line)
    #col2.metric("Nombre d'erreurs", num_errors)
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
        
        
    #pass
elif option == "Aperçu table":
    st.subheader(" Aperçu base de données")
    st.dataframe(df.head(100), use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total lignes", len(df))
    with col2:
        st.metric("Sources uniques", df["source"].nunique())
        
elif option == "Filtrer par source":
    st.subheader("Filtrer par source")
    selected_source = st.selectbox("Source :", df["source"].unique())
    filtered_df = df[df["source"] == selected_source]
    st.dataframe(filtered_df, use_container_width=True)
    st.bar_chart(filtered_df["label"].value_counts())
    
elif option == "Détail article":
    st.subheader("Détail article")
    article_idx = st.slider("Choisir article (0-99)", 0, min(99, len(df)-1))
    article = df.iloc[article_idx]
    st.json(dict(article))
    if article.get("image_url"):
        st.image(article["image_url"], caption=article["title"][:50])




