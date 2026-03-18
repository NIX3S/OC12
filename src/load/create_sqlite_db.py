# src/load/create_sqlite_db.py
import os
import sqlite3

DB_PATH = os.path.join("data/processed", "multimodal_etl.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Connexion SQLite
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Création de la table articles
cursor.execute("""
CREATE TABLE IF NOT EXISTS articles (
    title TEXT,
    text TEXT,
    image_url TEXT,
    publication_date DATETIME,
    domain TEXT,
    text_length INTEGER,
    has_label INTEGER
)
""")

conn.commit()
conn.close()
print(f"SQLite DB créée avec table 'articles' : {DB_PATH}")