import requests
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

# ----------------------
# CONFIGURATION
# ----------------------
load_dotenv()

API_KEY = os.getenv("NEWS_API_KEY")
print(API_KEY)
BASE_URL = "https://newsapi.org/v2/everything"
OUTPUT_PATH = "data/raw/newsapi_articles.json"

logging.basicConfig(
    filename="logs/extraction.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# ----------------------
# FETCH DATA
# ----------------------
def fetch_articles(query="news", page=1, page_size=100):
    params = {
        "q": query,
        "apiKey": API_KEY,
        "page": page,
        "pageSize": page_size,
        "language": "en",
        "sortBy": "publishedAt"
    }

    print(f" Fetching page {page}...")
    try:
        response = requests.get(BASE_URL, params=params, timeout=15, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        print(f" Page {page}: {len(data.get('articles', []))} articles")
        return data
    except requests.RequestException as e:
        logging.error(f"NewsAPI request failed: {e}")
        print(f" API Error: {e}")
        return None

# ----------------------
# VALIDATION ASSOUPLIE
# ----------------------
def validate_article(article):
    title = article.get("title")
    content = article.get("content", "").strip()
    image = article.get("urlToImage")
    
    has_title = bool(title and title.strip() and not title.lower().startswith("sponsored"))
    has_content = len(content) > 50  # Au moins 50 chars
    has_image = bool(image)
    
    print(f"   Title: {'OK' if has_title else 'KO'} ({len(title or '')}c)")
    print(f"   Content: {'OK' if has_content else 'KO'} ({len(content)}c)")
    print(f"   Image: {'OK' if has_image else 'KO'}")
    
    return has_title and has_content  # Image optionnelle

# ----------------------
# NORMALIZATION
# ----------------------
def normalize_article(article):
    return {
        "id": article.get("url"),
        "source": article.get("source", {}).get("name", "Unknown"),
        "title": article.get("title"),
        "text": article.get("content", ""),
        "image_url": article.get("urlToImage"),
        "publication_date": article.get("publishedAt"),
        "label": None,  # NewsAPI = actualités générales
        "language": "en",
        "url": article.get("url"),
        "retrieved_at": datetime.utcnow().isoformat()
    }

# ----------------------
# SAVE DATA
# ----------------------
def save_to_json(data, path=OUTPUT_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f" {len(data)} articles sauvés → {path}")
    logging.info(f"{len(data)} NewsAPI articles saved")

# ----------------------
# MAIN PIPELINE
# ----------------------
def main():
    if not API_KEY:
        print(" NEWS_API_KEY manquante dans .env")
        return
    
    print(" Starting NewsAPI extraction")
    logging.info("Starting NewsAPI extraction")
    
    all_articles = []
    
    for page in range(1, 4):
        response = fetch_articles(query="technology", page=page)  # Query plus précise
        
        if not response or response.get("totalResults", 0) == 0:
            print(f" Arrêt page {page}")
            break
            
        articles = response.get("articles", [])
        valid_count = 0
        
        for i, article in enumerate(articles):
            print(f"\n--- Article {i+1}/{len(articles)} ---")
            print(f"Titre: {article.get('title')[:80]}...")
            
            if validate_article(article):
                normalized = normalize_article(article)
                all_articles.append(normalized)
                valid_count += 1
                print(f"   VALIDÉ")
            else:
                print(f"    Rejeté")
        
        print(f"Page {page}: {valid_count}/{len(articles)} validés")
        
        if valid_count == 0:
            print(" Aucun article valide sur cette page")
    
    save_to_json(all_articles)
    print(f"\n {len(all_articles)} articles au total")

if __name__ == "__main__":
    main()
