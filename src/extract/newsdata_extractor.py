import requests
import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

# ----------------------
# CONFIGURATION
# ----------------------
load_dotenv()

API_KEY = os.getenv("NEWS_data_KEY")
BASE_URL = "https://newsdata.io/api/1/news"
OUTPUT_PATH = "data/raw/newsdata_articles.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------------
# SAFE STRING HELPER
# ----------------------
def safe_strip(value):
    """Safe strip() qui gère None"""
    return value.strip() if value else ""

# ----------------------
# FETCH DATA
# ----------------------
def fetch_articles(language="fr", page=None):
    params = {
        "apikey": API_KEY,
        "language": language,
        "image": 1
    }
    if page:
        params["page"] = page

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None

# ----------------------
# VALIDATION ROBUSTE
# ----------------------
def validate_article(article):
    title = safe_strip(article.get("title"))
    description = safe_strip(article.get("description"))
    image = article.get("image_url")
    
    has_title = len(title) > 10
    has_text = len(description) > 50
    has_image = bool(image)
    
    print(f"   Title: {len(title)}c {'OK' if has_title else 'KO'}")
    print(f"   Desc: {len(description)}c {'OK' if has_text else 'KO'}")
    print(f"   Image: {'OK' if has_image else 'KO'}")
    
    return has_title and has_text

# ----------------------
# NORMALIZATION
# ----------------------
def normalize_article(article):
    return {
        "id": article.get("article_id"),
        "source": article.get("source_id", article.get("source_name", "Unknown")),
        "title": safe_strip(article.get("title")),
        "text": safe_strip(article.get("description")),  # description fallback
        "image_url": article.get("image_url"),
        "publication_date": article.get("pubDate"),
        "label": None,
        "language": article.get("language", "fr"),
        "url": article.get("link"),
        "retrieved_at": datetime.now(timezone.utc).isoformat()
    }

# ----------------------
# SAVE DATA
# ----------------------
def save_to_json(data, path=OUTPUT_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f" {len(data)} articles → {path}")

# ----------------------
# MAIN
# ----------------------
def main():
    if not API_KEY:
        print(" NEWS_data_KEY manquante")
        return
        
    print(" NewsData extraction (SAFE parsing)")
    all_articles = []
    page = None

    for i in range(5):
        print(f"\n--- Page {i+1}/5 ---")
        response = fetch_articles(language="fr", page=page)
        
        if not response or not response.get("results"):
            print(" Plus d'articles")
            break

        articles = response.get("results", [])
        print(f"Trouvé {len(articles)} articles")
        
        valid_count = 0
        for j, article in enumerate(articles):
            print(f"\nArticle {j+1}: {article.get('title', 'No title')[:50]}...")
            if validate_article(article):
                normalized = normalize_article(article)
                all_articles.append(normalized)
                valid_count += 1
        
        print(f" {valid_count}/{len(articles)} validés")
        page = response.get("nextPage")
        if not page:
            break
    
    save_to_json(all_articles)
    print(f"\n TOTAL: {len(all_articles)} articles")

if __name__ == "__main__":
    main()
