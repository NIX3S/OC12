import requests
import json
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin
import os
import re

# CONFIG
BASE_URL = "https://factuel.afp.com/"
OUTPUT_PATH = "data/raw/afp_factuel_articles.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def extract_articles_from_homepage():
    """Extrait les articles depuis la page d'accueil"""
    response = requests.get(BASE_URL, headers=HEADERS, timeout=15)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    articles = []
    
    # Cherche les liens d'articles dans field-editor-s-choice
    choice_links = soup.find_all("a", href=re.compile(r"doc\.afp\.com\.[A-Z0-9]+"))
    
    print(f"Trouvé {len(choice_links)} liens d'articles")
    
    for i, link in enumerate(choice_links[:15]):  # 15 articles max
        article_url = urljoin(BASE_URL, link["href"])
        article_title = link.get_text().strip()
        
        print(f"{i+1}/15: {article_title[:60]}...")
        
        article_data = scrape_article(article_url)
        if article_data:
            article_data["title"] = article_title
            article_data["url"] = article_url
            articles.append(article_data)
    
    return articles

def scrape_article(url):
    """Scrape un article fact-checking spécifique"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Erreur {url}: {e}")
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Extraction titre principal
    title = (soup.find("h1") or soup.select_one("h1.page-title")).get_text().strip()
    
    # Extraction verdict (badges AFP spécifiques)
    verdict_elem = soup.find("div", string=re.compile(r"Faux|Trompeur|Vrai|Intox", re.I))
    label = "unknown"
    if verdict_elem:
        verdict_text = verdict_elem.get_text().lower()
        if any(word in verdict_text for word in ["faux", "intox"]):
            label = "fake"
        elif "trompeur" in verdict_text:
            label = "misleading"
        elif any(word in verdict_text for word in ["vrai", "vérifié"]):
            label = "real"
    
    # Texte principal (évite sidebar/pub)
    main_content = soup.find("div", class_=re.compile(r"content|article-body|field-body"))
    if not main_content:
        main_content = soup.find("article")
    
    paragraphs = main_content.find_all("p") if main_content else []
    text_parts = [p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 30]
    text = " ".join(text_parts)[:15000]  # Limite taille
    
    # Image principale
    images = soup.find_all("img", src=True)
    image_url = None
    for img in images:
        src = img["src"]
        if any(ext in src.lower() for ext in ['.jpg', '.jpeg', '.png']) and "width" in img.attrs:
            image_url = urljoin(url, src)
            break
    
    logging.info(f"Article '{title[:50]}...' → label={label}, text_len={len(text)}")
    
    return {
        "id": url,
        "source": "AFP Factuel",
        "text": text,
        "image_url": image_url,
        "label": label,
        "language": "fr",
        "retrieved_at": datetime.utcnow().isoformat()
    }

def main():
    print(" Extraction AFP Factuel (page d'accueil)")
    articles = extract_articles_from_homepage()
    
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    
    print(f" {len(articles)} articles extraits → {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
