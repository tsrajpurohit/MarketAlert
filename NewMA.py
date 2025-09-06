import os
import re
import requests
import time
import logging
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import feedparser
from pathlib import Path
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------- CONFIG --------------------------
HF_TOKEN = ""  # Hugging Face token
NEWSAPI_KEY = ""
GNEWS_KEY = ""
NEWSDATA_KEY = ""

TELEGRAM_CHAT_ID = ""
TELEGRAM_BOT_TOKEN = ""
WATERMARK = "@Stock_Market_News_Buzz"

CACHE_FILE = Path("daily_news.json")
SENT_FILE = Path("sent_titles.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TODAY = datetime.now().strftime("%Y-%m-%d")
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
FROM_DATE = f"{YESTERDAY}T00:00:00Z"
TO_DATE = f"{TODAY}T23:59:59Z"

client = OpenAI(
    base_url="https://router.huggingface.co/v1",
    api_key=HF_TOKEN,
)

# -------------------------- HELPERS --------------------------
def escape_markdown_v2(text):
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f"\\{char}")
    return text

def fetch_image_from_url(url):
    try:
        resp = requests.get(url, timeout=5)
        soup = BeautifulSoup(resp.text, 'html.parser')
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            return og_image["content"]
        twitter_image = soup.find("meta", attrs={"name": "twitter:image"})
        if twitter_image and twitter_image.get("content"):
            return twitter_image["content"]
    except:
        pass
    return None

def clean_articles(articles):
    seen_titles = set()
    clean_list = []
    for a in articles:
        title = a.get('title', '').strip()
        if title and title not in seen_titles:
            seen_titles.add(title)
            clean_list.append(a)
    return clean_list

def load_json_file(file_path):
    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_json_file(file_path, data):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def add_to_cache(article):
    cache = load_json_file(CACHE_FILE)
    cache.append(article)
    save_json_file(CACHE_FILE, cache)

def already_sent(title):
    sent_titles = load_json_file(SENT_FILE)
    return title in sent_titles

def mark_sent(title):
    sent_titles = load_json_file(SENT_FILE)
    sent_titles.append(title)
    save_json_file(SENT_FILE, sent_titles)

# -------------------------- TELEGRAM --------------------------
def send_telegram(article):
    title = escape_markdown_v2(article.get("title","No Title"))
    desc = escape_markdown_v2(article.get("description",""))
    source = escape_markdown_v2(article.get("source_name","Unknown"))
    published = escape_markdown_v2(article.get("published", TODAY))
    # Final caption (no URL, no source/date)
    caption = f"*{title}*\n\n{desc}\n\n{WATERMARK}"

    try:
        if article.get("image"):
            url_api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            data = {
                "chat_id": TELEGRAM_CHAT_ID,
                "photo": article["image"],
                "caption": caption[:1024],  # Telegram photo caption limit
                "parse_mode": "MarkdownV2"
            }
        else:
            url_api = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            data = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": caption[:4096],  # Telegram text limit
                "parse_mode": "MarkdownV2"
            }

        resp = requests.post(url_api, data=data, timeout=10)

        # Fallback: plain text (no formatting), still safe with slicing
        if resp.status_code == 400:
            logging.warning("Markdown failed, retrying without formatting...")
            plain_caption = f"{article.get('title','No Title')}\n\n{article.get('description','')}\n\n{WATERMARK}"
            if article.get("image"):
                data = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "photo": article["image"],
                    "caption": plain_caption[:1024]
                }
            else:
                data = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": plain_caption[:4096]
                }
            resp = requests.post(url_api, data=data, timeout=10)

        logging.info(f"Sent Telegram: {title[:50]}... Status {resp.status_code}")

    except Exception as e:
        logging.error(f"Failed to send Telegram: {title[:50]}... {e}")

def send_articles_directly(articles, max_workers=5):
    """Send articles to Telegram in parallel (default 5 workers)."""
    to_send = []
    for art in articles:
        title = art.get("title", "No Title")
        if already_sent(title):
            logging.info(f"Skipping duplicate: {title[:50]}...")
            continue
        to_send.append(art)
        mark_sent(title)
        add_to_cache(art)

    if not to_send:
        logging.info("No new articles to send")
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(send_telegram, a) for a in to_send]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error sending article in parallel: {e}")


# -------------------------- FETCH ARTICLES --------------------------
def fetch_newsapi_articles(query="finance OR industry OR business", page_size=10):
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query, "language": "en", "from": YESTERDAY, "to": TODAY,
        "sortBy": "publishedAt", "pageSize": page_size, "apiKey": NEWSAPI_KEY
    }
    try:
        response = requests.get(url, params=params, timeout=10).json()
        articles = clean_articles(response.get("articles", []))
        result = []
        for a in articles:
            result.append({
                "title": a.get("title", "No title"),
                "description": a.get("description", ""),
                "source_name": "NewsAPI",
                "published": a.get("publishedAt", TODAY).split("T")[0],
                "url": a.get("url", ""),
                "image": fetch_image_from_url(a.get("url", ""))
            })
        logging.info(f"Fetched {len(result)} NewsAPI articles")
        return result
    except Exception as e:
        logging.error(f"NewsAPI fetch failed: {e}")
        return []

def fetch_gnews_articles(query="finance OR industry OR business", max_results=10):
    url = f"https://gnews.io/api/v4/search?q={query}&lang=en&in=title,description&from={FROM_DATE}&to={TO_DATE}&max={max_results}&token={GNEWS_KEY}"
    try:
        response = requests.get(url, timeout=10).json()
        articles = clean_articles(response.get("articles", []))
        result = []
        for a in articles:
            result.append({
                "title": a.get("title", "No title"),
                "description": a.get("description", ""),
                "source_name": "GNews",
                "published": a.get("publishedAt", TODAY).split("T")[0],
                "url": a.get("url", ""),
                "image": fetch_image_from_url(a.get("url", ""))
            })
        logging.info(f"Fetched {len(result)} GNews articles")
        return result
    except Exception as e:
        logging.error(f"GNews fetch failed: {e}")
        return []

def fetch_newsdata_articles(query="finance OR industry OR business", max_results=10):
    url = f"https://newsdata.io/api/1/news?apikey={NEWSDATA_KEY}&q={query}&language=en"
    try:
        response = requests.get(url, timeout=10).json()
        articles = response.get("results", [])
        articles = [a for a in articles if a.get("pubDate", "").split(" ")[0] >= YESTERDAY][:max_results]
        result = []
        for a in articles:
            result.append({
                "title": a.get("title", "No title"),
                "description": a.get("description", ""),
                "source_name": "NewsData",
                "published": a.get("pubDate", TODAY).split(" ")[0],
                "url": a.get("link", ""),
                "image": fetch_image_from_url(a.get("link", ""))
            })
        logging.info(f"Fetched {len(result)} NewsData articles")
        return result
    except Exception as e:
        logging.error(f"NewsData fetch failed: {e}")
        return []

def fetch_bs_rss_articles():
    feeds = [
        # Business Standard
        #{"url": "https://www.business-standard.com/rss/mutual-funds.rss", "source": "Business Standard - Mutual Funds"},
        #{"url": "https://www.business-standard.com/rss/ipos.rss", "source": "Business Standard - IPOs"},
        #{"url": "https://www.business-standard.com/rss/capital-market-news.rss", "source": "Business Standard - Capital Market"},
        #{"url": "https://www.business-standard.com/rss/markets-news.rss", "source": "Business Standard - Markets"},

        # Economic Times
        #{"url": "https://economictimes.indiatimes.com/markets/stocks/earnings/rssfeeds/2146842.cms", "source": "ET - Earnings"},
        #{"url": "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146843.cms", "source": "ET - Stocks"},
        #{"url": "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms", "source": "ET - Industry"},

        # LiveMint
        #{"url": "https://www.livemint.com/rss/industry", "source": "LiveMint - Industry"},

        # CNBC
        #{"url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147", "source": "CNBC - Markets"},

        # The Hindu
        {"url": "https://www.thehindu.com/business/Industry/feeder/default.rss", "source": "The Hindu - Industry"},
    ]

    result = []
    for f in feeds:
        try:
            feed = feedparser.parse(f["url"])
            for entry in feed.entries[:10]:
                result.append({
                    "title": entry.get("title", "No title"),
                    "description": entry.get("summary", ""),
                    "source_name": f["source"],
                    "published": entry.get("published", TODAY).split(" ")[0] if entry.get("published") else TODAY,
                    "url": entry.get("link", ""),
                    "image": fetch_image_from_url(entry.get("link", ""))
                })
            logging.info(f"Fetched {len(feed.entries[:10])} items from {f['source']}")
        except Exception as e:
            logging.error(f"RSS fetch failed: {f['url']} {e}")
    return clean_articles(result)


def fetch_scraped_articles():
    sources = [
        {'url': "https://www.moneycontrol.com/news/business/stocks/", 'selector': 'li.clearfix', 'source_name': "Moneycontrol Stocks"},
        {'url': "https://www.moneycontrol.com/news/business/companies/", 'selector': 'li.clearfix', 'source_name': "Moneycontrol Companies"},
        {'url': "https://economictimes.indiatimes.com/markets/stocks/earnings/news", 'selector': 'div.eachStory', 'source_name': "ET Earnings"},
        {'url': "https://economictimes.indiatimes.com/markets/stocks/news", 'selector': 'div.eachStory', 'source_name': "ET Stocks"},
    ]
    all_articles = []
    for s in sources:
        try:
            resp = requests.get(s['url'], timeout=10)
            soup = BeautifulSoup(resp.text, 'html.parser')
            items = soup.select(s['selector'])[:10]
            for item in items:
                title = item.get_text(strip=True)
                link_tag = item.find('a', href=True)
                url = link_tag['href'] if link_tag else None
                image = fetch_image_from_url(url) if url else None
                all_articles.append({
                    "title": title[:200] if title else "No title",
                    "description": title,
                    "source_name": s['source_name'],
                    "published": TODAY,
                    "url": url or "",
                    "image": image
                })
            logging.info(f"Fetched {len(items)} items from {s['source_name']}")
        except Exception as e:
            logging.error(f"Scrape failed: {s['url']} {e}")
    return clean_articles(all_articles)

# -------------------------- DAILY SUMMARY --------------------------
def daily_summary():
    all_articles = load_json_file(CACHE_FILE)
    if not all_articles:
        logging.info("No articles in cache for daily summary")
        return
    news_text = "\n".join([f"{a['title']}\n{a['description']}" for a in all_articles])
    prompt = (
        "Review the following Indian financial, industrial, IPO, mutual fund, "
        "share market, and business news from today. Summarize it in concise "
        "bullet points, keeping source and date. Exclude links.\n\n"
        f"{news_text}"
    )
    try:
        completion = client.chat.completions.create(
            model="deepseek-ai/DeepSeek-V3.1:together",
            messages=[{"role": "user", "content": prompt}],
        )
        summary_text = completion.choices[0].message["content"]
        summary_text = "\n".join([f"- {line.strip()}" for line in summary_text.split("\n") if line.strip()])
        send_telegram({
            "title": "Today's TOP News",
            "description": summary_text,
            "source_name": "Summary",
            "published": TODAY,
            "image": None
        })
        logging.info("Daily summary sent")
    except Exception as e:
        logging.error(f"Hugging Face summary failed: {e}")
    CACHE_FILE.unlink(missing_ok=True)

# -------------------------- MAIN --------------------------
if __name__ == "__main__":
    all_news = []
    all_news += fetch_newsapi_articles()
    all_news += fetch_gnews_articles()
    all_news += fetch_newsdata_articles()
    all_news += fetch_bs_rss_articles()
    all_news += fetch_scraped_articles()

    if not all_news:
        logging.info("No news articles found")
    else:
        send_articles_directly(all_news)

    # Only run daily summary at 10 PM
    now = datetime.now()
    if now.hour == 22:
        daily_summary()
