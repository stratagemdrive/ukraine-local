"""
Ukraine News RSS Aggregator
Fetches, categorizes, translates, and outputs news to docs/ukraine_news.json
No external APIs required — uses deep-translator (Google Translate web scraping).
"""

import json
import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateparser

import feedparser
import requests
from deep_translator import GoogleTranslator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

COUNTRY = "ukraine"
OUTPUT_DIR = "docs"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f"{COUNTRY}_news.json")
MAX_AGE_DAYS = 7
MAX_PER_CATEGORY = 20
CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# ---------------------------------------------------------------------------
# RSS Sources
# All sources publish in English or mixed EN/UK. Ukrainian-only titles will
# be translated automatically.
# ---------------------------------------------------------------------------
SOURCES = [
    {
        "name": "Kyiv Independent",
        "url": "https://kyivindependent.com/news-archive/feed/",
        "lang": "en",
    },
    {
        "name": "Ukrainska Pravda",
        "url": "https://www.pravda.com.ua/eng/rss/",
        "lang": "en",
    },
    {
        "name": "The New Voice of Ukraine",
        "url": "https://english.nv.ua/rss/all_news.xml",
        "lang": "en",
    },
    {
        "name": "UNIAN",
        "url": "https://rss.unian.net/site/news_eng.rss",
        "lang": "en",
    },
    {
        "name": "Ukrinform",
        "url": "https://www.ukrinform.net/rss/block-lastnews",
        "lang": "en",
    },
    {
        "name": "Euromaidan Press",
        "url": "https://euromaidanpress.com/feed/",
        "lang": "en",
    },
    {
        "name": "UkraineWorld",
        "url": "https://ukraineworld.org/en/rss.xml",
        "lang": "en",
    },
    # Hromadske Radio publishes very infrequently in English text form; replaced
    # with Kyiv Post which has a reliable high-volume English RSS feed.
    {
        "name": "Kyiv Post",
        "url": "https://www.kyivpost.com/rss",
        "lang": "en",
    },
    {
        "name": "Rubryka",
        "url": "https://rubryka.com/en/feed/",
        "lang": "en",
    },
    {
        "name": "RFE/RL Ukraine",
        "url": "https://www.rferl.org/api/zphpmooi_uvq-ep/latest.xml",
        "lang": "en",
    },
]

# ---------------------------------------------------------------------------
# Category keyword maps
# ---------------------------------------------------------------------------
CATEGORY_KEYWORDS = {
    "Military": [
        "military", "war", "army", "weapon", "drone", "missile", "attack",
        "troops", "front", "battle", "soldier", "artillery", "air force",
        "navy", "defense", "combat", "strike", "shelling", "invasion",
        "armed forces", "general staff", "casualties", "prisoner", "offensive",
        "tank", "rocket", "bomb", "ceasefire", "mobilization", "nato",
    ],
    "Diplomacy": [
        "diplomat", "diplomacy", "sanction", "peace", "negotiat", "treaty",
        "foreign minister", "president", "summit", "alliance", "eu ", "europe",
        "united nations", "un ", "g7", "g20", "bilateral", "ambassador",
        "agreement", "ceasefire", "talks", "meeting", "relations", "zelensky",
        "zelenskyy", "macron", "biden", "trump", "starmer", "scholz", "nato",
        "international", "aid", "support package",
    ],
    "Energy": [
        "energy", "power", "electricity", "grid", "blackout", "nuclear",
        "gas", "oil", "fuel", "pipeline", "heating", "thermal", "hydroelectric",
        "renewable", "solar", "wind", "generator", "outage", "supply",
        "infrastructure attack", "power plant", "substation", "naftogaz",
        "dtek", "ukrenergo",
    ],
    "Economy": [
        "economy", "economic", "gdp", "budget", "finance", "bank", "hryvnia",
        "inflation", "trade", "export", "import", "business", "invest",
        "market", "reconstruction", "aid package", "loan", "imf", "world bank",
        "revenue", "tax", "agriculture", "grain", "harvest", "wheat",
        "industry", "manufacturing", "tech", "startup",
    ],
    "Local Events": [
        "local", "region", "city", "village", "oblast", "municipality",
        "community", "resident", "civilian", "evacuation", "humanitarian",
        "hospital", "school", "shelter", "volunteer", "culture", "festival",
        "sport", "crime", "court", "police", "corruption", "protest",
        "election", "mayor", "governor",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def translate_to_english(text: str) -> str:
    """Translate text to English using GoogleTranslator (no API key needed)."""
    if not text or not text.strip():
        return text
    try:
        translated = GoogleTranslator(source="auto", target="en").translate(text[:4990])
        return translated or text
    except Exception as exc:
        log.warning("Translation failed: %s", exc)
        return text


def is_english(text: str) -> bool:
    """Rough heuristic: if most characters are ASCII, treat as English."""
    if not text:
        return True
    ascii_count = sum(1 for c in text if ord(c) < 128)
    return (ascii_count / len(text)) > 0.85


def classify(title: str, description: str = "") -> str:
    """Classify article into a category based on keyword matching."""
    combined = (title + " " + (description or "")).lower()

    scores = {cat: 0 for cat in CATEGORIES}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in combined:
                scores[cat] += 1

    best = max(scores, key=scores.get)
    # If no keywords matched at all, default to Local Events
    if scores[best] == 0:
        return "Local Events"
    return best


def parse_date(entry) -> datetime | None:
    """Return a timezone-aware UTC datetime from a feedparser entry."""
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    # Fallback: try raw string fields
    for attr in ("published", "updated"):
        raw = getattr(entry, attr, None)
        if raw:
            try:
                dt = dateparser.parse(raw)
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
    return None


def fetch_feed(source: dict) -> list[dict]:
    """Fetch and parse a single RSS source, returning a list of article dicts."""
    articles = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)

    log.info("Fetching: %s  (%s)", source["name"], source["url"])
    try:
        feed = feedparser.parse(
            source["url"],
            agent="Mozilla/5.0 (compatible; UkraineNewsBot/1.0)",
            request_headers={"Accept": "application/rss+xml, application/xml, text/xml"},
        )
    except Exception as exc:
        log.error("Failed to fetch %s: %s", source["name"], exc)
        return articles

    if feed.bozo and not feed.entries:
        log.warning("Bozo feed with no entries for %s", source["name"])
        return articles

    for entry in feed.entries:
        pub_date = parse_date(entry)
        if pub_date is None:
            # Accept entries without a date (treat as recent)
            pub_date = datetime.now(timezone.utc)

        if pub_date < cutoff:
            continue

        title = getattr(entry, "title", "") or ""
        url = getattr(entry, "link", "") or ""
        description = getattr(entry, "summary", "") or ""

        if not title or not url:
            continue

        # Translate Ukrainian titles
        if not is_english(title):
            title = translate_to_english(title)
            time.sleep(0.3)  # polite rate limit

        category = classify(title, description)

        articles.append(
            {
                "title": title.strip(),
                "source": source["name"],
                "url": url.strip(),
                "published_date": pub_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "category": category,
            }
        )

    log.info("  → %d articles from %s", len(articles), source["name"])
    return articles


def deduplicate(articles: list[dict]) -> list[dict]:
    """Remove duplicate URLs, keeping the first occurrence."""
    seen = set()
    unique = []
    for art in articles:
        if art["url"] not in seen:
            seen.add(art["url"])
            unique.append(art)
    return unique


def load_existing() -> dict:
    """Load the existing JSON file, returning an empty structure if absent."""
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"country": COUNTRY, "last_updated": "", "stories": []}


def merge_stories(existing: list[dict], new_articles: list[dict]) -> list[dict]:
    """
    Merge new articles into existing, per category:
    - Keep up to MAX_PER_CATEGORY stories per category.
    - Prefer newer stories; if at capacity, replace the oldest.
    - Never include stories older than MAX_AGE_DAYS.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)

    # Build a per-category dict from existing, filtering stale entries
    by_cat: dict[str, list[dict]] = {cat: [] for cat in CATEGORIES}
    for story in existing:
        cat = story.get("category", "Local Events")
        if cat not in by_cat:
            cat = "Local Events"
        try:
            pub = datetime.strptime(story["published_date"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            continue
        if pub >= cutoff:
            by_cat[cat].append(story)

    # Track all existing URLs to avoid re-inserting duplicates
    existing_urls = {s["url"] for stories in by_cat.values() for s in stories}

    # Insert new articles per category
    for article in new_articles:
        cat = article.get("category", "Local Events")
        if cat not in by_cat:
            cat = "Local Events"
        if article["url"] in existing_urls:
            continue
        existing_urls.add(article["url"])

        bucket = by_cat[cat]
        bucket.append(article)

        if len(bucket) > MAX_PER_CATEGORY:
            # Drop the oldest entry
            bucket.sort(
                key=lambda x: x.get("published_date", ""),
                reverse=True,
            )
            by_cat[cat] = bucket[:MAX_PER_CATEGORY]

    # Flatten and sort by published_date descending
    merged = []
    for cat in CATEGORIES:
        merged.extend(by_cat[cat])

    merged.sort(key=lambda x: x.get("published_date", ""), reverse=True)
    return merged


def save(data: dict) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info("Saved %d stories to %s", len(data["stories"]), OUTPUT_FILE)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=== Ukraine News Fetcher starting ===")

    # 1. Fetch all feeds
    all_new: list[dict] = []
    for source in SOURCES:
        articles = fetch_feed(source)
        all_new.extend(articles)
        time.sleep(1)  # be polite between sources

    all_new = deduplicate(all_new)
    log.info("Total unique new articles fetched: %d", len(all_new))

    # 2. Load existing JSON
    existing_data = load_existing()
    existing_stories = existing_data.get("stories", [])

    # 3. Merge
    merged = merge_stories(existing_stories, all_new)

    # 4. Save
    output = {
        "country": COUNTRY,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stories": merged,
    }
    save(output)
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
