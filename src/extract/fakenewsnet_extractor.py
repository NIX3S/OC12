import requests
import feedparser
import os
import json
import logging
import re  # ← AJOUTÉ ICI
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

# ----------------------
# CONFIGURATION
# ----------------------
RSS_URL = "https://www.legorafi.fr/feed/"
OUTPUT_PATH = "data/raw/legorafi_articles.json"

logging.basicConfig(
    filename="logs/extraction.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ----------------------
# PARSE ARTICLE
# ----------------------
def parse_article(entry):
    """Parse un article depuis l'URL via scraping"""
    url = entry.link
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Extraction Gorafi (structure WordPress standard)
    title_elem = soup.find("h1") or soup.find("h1", class_="entry-title")
    title = title_elem.get_text().strip() if title_elem else entry.title
    
    # Contenu principal
    content_elem = soup.find("div", class_=re.compile(r"content|entry-content|post-content"))
    if not content_elem:
        content_elem = soup.find("article")
    
    paragraphs = content_elem.find_all("p") if content_elem else []
    text_parts = [p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 20]
    text = " ".join(text_parts)[:10000]  # Limite taille
    
    # Image principale
    image_elem = (soup.find("img", class_="wp-post-image") or 
                  soup.find("img", alt=re.compile(title[:30], re.I)) or
                  soup.find("img", class_=re.compile("attachment")))
    
    image_url = None
    if image_elem and image_elem.get("src"):
        image_url = urljoin(url, image_elem["src"])
    
    return {
        "id": url.split("/")[-2] if "/" in url else url,  # Slug de l'URL
        "source": "Le Gorafi",
        "title": title,
        "text": text,
        "image_url": image_url,
        "publication_date": entry.get("published"),
        "label": "satire",  # Le Gorafi = satire/pastiche
        "language": "fr",
        "url": url,
        "retrieved_at": datetime.utcnow().isoformat()
    }

# ----------------------
# MAIN EXTRACTION
# ----------------------
def main():
    logging.info("Starting Le Gorafi extraction")
    
    print("📡 Récupération flux RSS Le Gorafi...")
    feed = feedparser.parse(RSS_URL)
    
    all_articles = []
    print(f"✅ {len(feed.entries)} articles trouvés dans le flux")
    
    for i, entry in enumerate(feed.entries[:50]):  # 50 articles max
        print(f"🔄 {i+1}/50: {entry.title[:60]}...")
        
        article = parse_article(entry)
        
        if article and article["text"]:  # Texte obligatoire
            all_articles.append(article)
            print(f"   → OK (label: {article['label']}, {len(article['text'])} chars)")
        else:
            print("   → SKIP (pas de texte)")
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)
    
    print(f"\n🎉 Extraction terminée: {len(all_articles)} articles → {OUTPUT_PATH}")
    logging.info(f"{len(all_articles)} Le Gorafi articles saved")

if __name__ == "__main__":
    main()
