import os
import json
import logging
import re
import requests
import pandas as pd
from datetime import datetime

# ----------------------
# CONFIGURATION
# ----------------------

RAW_PATH = "data/raw"
OUTPUT_PATH = "data/processed/cleaned_dataset.json"
BASE_PROJET = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_FILE = os.path.join(BASE_PROJET, "logs", "transform.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# éviter doublons si rechargé
if not logger.handlers:
    file_handler = logging.FileHandler(LOG_FILE)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
# ----------------------
# LOAD DATA
# ----------------------

def load_data():
    all_data = []

    for file in os.listdir(RAW_PATH):
        if file.endswith(".json"):
            with open(os.path.join(RAW_PATH, file), "r", encoding="utf-8") as f:
                data = json.load(f)
                all_data.extend(data)

    logger.info(f"{len(all_data)} raw articles loaded")
    return pd.DataFrame(all_data)


# ----------------------
# CLEAN TEXT
# ----------------------

def clean_text(text):
    if not text:
        return None

    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'<.*?>', '', text)
    return text.strip()


# ----------------------
# VALIDATE IMAGE
# ----------------------

def validate_image_url(url):
    try:
        response = requests.head(url, timeout=5)
        return response.status_code == 200
    except:
        return False


# ----------------------
# NORMALIZE DATE
# ----------------------

def normalize_date(date_str):
    if not date_str:
        return None
    try:
        return pd.to_datetime(date_str, errors="coerce")
    except:
        return None


# ----------------------
# ENRICH FEATURES
# ----------------------

def enrich_features(df):
    df["text_length"] = df["text"].apply(lambda x: len(x) if x else 0)
    df["has_label"] = df["label"].apply(lambda x: 1 if x else 0)
    df["domain"] = df["url"].apply(lambda x: x.split("/")[2] if isinstance(x, str) else None)
    return df


# ----------------------
# MAIN PIPELINE
# ----------------------

def main():
    logger.info("Starting transformation pipeline")

    df = load_data()

    # Clean text
    df["text"] = df["text"].apply(clean_text)

    # Normalize date
    df["publication_date"] = df["publication_date"].apply(normalize_date)

    # Remove rows without text or image
    df = df.dropna(subset=["text", "image_url"])
    logger.info(f"{len(df)} articles après filtrage texte/image")
    # Validate image accessibility
    df["image_valid"] = df["image_url"].apply(validate_image_url)
    logger.info(f"{sum(df['image_valid'])}/{len(df)} images valides")
    df = df[df["image_valid"] == True]

    # Remove duplicates based on URL
    df = df.drop_duplicates(subset=["url"])

    # Enrich features
    df = enrich_features(df)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_json(OUTPUT_PATH, orient="records", force_ascii=False, indent=4)

    logger.info(f"Transformation completed. {len(df)} clean articles saved.")
    print(f"Pipeline completed: {len(df)} articles ready.")


if __name__ == "__main__":
    main()