import requests
from bs4 import BeautifulSoup
import datetime
import time
import os
import json
import logging
import random
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from dateutil import parser
import feedparser

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler('scraper.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(file_handler)

script_directory = os.path.dirname(os.path.abspath(__file__))

# ---------- Helper Functions ----------

def parse_date(date_str):
    try:
        date_str = date_str.replace('Updated On :', '').strip()
        return parser.parse(date_str, fuzzy=True)
    except Exception:
        return datetime.datetime.now()

def extract_date(article):
    date = None
    date_span = article.find('span')
    if date_span:
        date = date_span.get_text(strip=True)
    date_time = article.find('time')
    if date_time:
        date = date_time.get('datetime', None)
    return date

def dynamic_extract(element, tag_names, attribute_name=None):
    for tag_name in tag_names:
        target = element.find(tag_name)
        if target:
            if attribute_name and target.has_attr(attribute_name):
                return target.get(attribute_name, '').strip()
            return target.get_text(strip=True)
    return ''

# ---------- Scrape Moneycontrol / ET ----------

def scrape_news(url, selector):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        articles = soup.select(selector)
        items = []
        for article in articles:
            title = dynamic_extract(article, ['h2', 'h3', 'a', 'span'])
            link = dynamic_extract(article, ['a'], 'href')
            description = dynamic_extract(article, ['p', 'span', 'div'])
            if not title:
                title = description or 'No title'
            if link and not link.startswith('http'):
                link = requests.compat.urljoin(url, link)
            date_str = extract_date(article)
            pub_date = parse_date(date_str) if date_str else datetime.datetime.now()
            items.append({
                'title': title,
                'link': link or '#',
                'description': description,
                'pubDate': pub_date.isoformat()
            })
        return items
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return []

# ---------- Scrape Business Standard via RSS ----------

# For RSS sources (Business Standard)
def scrape_bs_rss(rss_url):
    feed = feedparser.parse(rss_url)
    items = []
    for entry in feed.entries:
        # Parse pubDate
        pub_date = entry.get("published", entry.get("updated", datetime.datetime.now().isoformat()))
        try:
            pub_date_parsed = parser.parse(pub_date)
            pub_date_iso = pub_date_parsed.isoformat()
        except Exception:
            pub_date_iso = datetime.datetime.now().isoformat()

        # Extract image (media:content, media:thumbnail, enclosure, etc.)
        image_url = ""
        if "media_content" in entry and len(entry.media_content) > 0:
            image_url = entry.media_content[0].get("url", "")
        elif "media_thumbnail" in entry and len(entry.media_thumbnail) > 0:
            image_url = entry.media_thumbnail[0].get("url", "")
        elif "links" in entry:
            # Sometimes images are in enclosure links
            for link in entry.links:
                if link.get("rel") == "enclosure" and "image" in link.get("type", ""):
                    image_url = link.get("href", "")
                    break

        items.append({
            "title": entry.get("title", "No Title"),
            "link": entry.get("link", "#"),
            "description": entry.get("summary", ""),
            "pubDate": pub_date_iso,
            "image": image_url
        })

    return items



# ---------- JSON Feed & Telegram ----------

def create_or_update_json_feed(items, output_file):
    output_path = os.path.join(script_directory, output_file)
    today = datetime.datetime.now().date()
    existing_items = []
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
                for item in data.get('items', []):
                    try:
                        if parser.parse(item['pubDate']).date() == today:
                            existing_items.append(item)
                    except Exception:
                        continue
            except Exception:
                pass
    new_items = []
    for item in items:
        try:
            if parser.parse(item['pubDate']).date() == today:
                new_items.append(item)
        except Exception:
            continue
    updated_items = existing_items + new_items
    feed_data = {
        'title': "RSS Feed",
        'link': "https://example.com",
        'description': "RSS Feed Description",
        'lastBuildDate': datetime.datetime.now().isoformat(),
        'items': updated_items
    }
    try:
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(feed_data, file, indent=4)
    except Exception as e:
        logging.error(f"Failed to write JSON feed: {e}")

def send_to_telegram(bot_token, chat_id, message, image_url=None):
    if image_url:
        try:
            response = requests.post(
                f'https://api.telegram.org/bot{bot_token}/sendPhoto',
                data={'chat_id': chat_id, 'caption': message, 'parse_mode': 'Markdown'},
                files={'photo': requests.get(image_url, stream=True).raw}
            )
            response.raise_for_status()
            return
        except Exception as e:
            logging.warning(f"Failed to send image, sending text only: {e}")

    # fallback to text only
    try:
        response = requests.post(
            f'https://api.telegram.org/bot{bot_token}/sendMessage',
            data={'chat_id': chat_id, 'text': message, 'parse_mode': 'Markdown'}
        )
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to send Telegram message: {e}")


def read_sent_ids(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except:
            return set()
    return set()

def write_sent_ids(file_path, ids):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(list(ids), f)

def process_source(source, bot_token, chat_id):
    exclude_keywords = ["KR Choksey", "Lilladher", "motilal", "ICICI Securities",
                        "Sharekhan", "straight session", "Anand Rathi", "Emkay"]
    sent_ids_file_path = os.path.join(script_directory, source['sent_ids_file'])
    sent_ids = read_sent_ids(sent_ids_file_path)
    
    # Scrape source
    try:
        if source.get('rss', False):
            items = scrape_bs_rss(source['url'])
            logging.info(f"[RSS] Found {len(items)} articles at {source['url']}")
        else:
            items = scrape_news(source['url'], source['selector'])
            logging.info(f"[HTML] Found {len(items)} articles at {source['url']}")
    except Exception as e:
        logging.error(f"Failed to scrape {source['url']}: {e}")
        return
    
    # Filter today's items
    today = datetime.datetime.now().date()
    new_items = [item for item in items if parser.parse(item['pubDate']).date() == today]
    logging.info(f"{len(new_items)} new items found today at {source['url']}")
    
    # Apply keyword filter
    filtered_items = [
        item for item in new_items
        if not any(k.lower() in (item['title'] + " " + item['description']).lower() for k in exclude_keywords)
    ]
    logging.info(f"{len(filtered_items)} items remaining after applying exclude_keywords filter")
    
    # Exclude already sent items
    to_send = [item for item in filtered_items if item['link'] not in sent_ids]
    logging.info(f"{len(to_send)} items to send (not sent before)")
    
    # Send messages to Telegram
    for item in to_send:
        message = f"*{item['title']}*\n\n{item['description']}\n\n@Stock_Market_News_Buzz"
        send_to_telegram(bot_token, chat_id, message, item.get('image'))
    
    # Update JSON feed and sent IDs
    if to_send:
        create_or_update_json_feed(to_send, source['output_file'])
        new_ids = set(item['link'] for item in to_send)
        write_sent_ids(sent_ids_file_path, sent_ids.union(new_ids))
        logging.info(f"JSON feed updated and sent IDs recorded for {source['url']}")



# ---------- Main ----------

def main():
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')
    if not bot_token or not chat_id:
        logging.error("Telegram token or chat ID missing")
        return

    sources = [
        # Moneycontrol
        {'url': "https://www.moneycontrol.com/news/business/stocks/", 'selector': 'li.clearfix',
         'output_file': "moneycontrol_rss_feed.json", 'sent_ids_file': 'moneycontrol_sent_ids.json'},
        {'url': "https://www.moneycontrol.com/news/business/companies/", 'selector': 'li.clearfix',
         'output_file': "moneycontrol_companies_rss_feed.json", 'sent_ids_file': 'moneycontrol__companies_sent_ids.json'},
        # ET
        {'url': "https://economictimes.indiatimes.com/markets/stocks/earnings/news", 'selector': 'div.eachStory',
         'output_file': "economictimes_earnings_rss_feed.json", 'sent_ids_file': 'economictimes_earnings_sent_ids.json'},
        {'url': "https://economictimes.indiatimes.com/markets/stocks/news", 'selector': 'div.eachStory',
         'output_file': "economictimes_stocks_rss_feed.json", 'sent_ids_file': 'economictimes_stocks_sent_ids.json'},
    ]

    # Add Business Standard RSS sources
    bs_sources = [
        {'url': "https://www.business-standard.com/rss/industry/news-21705.rss", 'rss': True,
         'output_file': "bs_industry_news_rss_feed.json", 'sent_ids_file': "bs_industry_news_sent_ids.json"},
        {'url': "https://www.business-standard.com/rss/industry/banking-21703.rss", 'rss': True,
         'output_file': "bs_banking_rss_feed.json", 'sent_ids_file': "bs_banking_sent_ids.json"},
        {'url': "https://www.business-standard.com/rss/markets-106.rss", 'rss': True,
         'output_file': "bs_markets_rss_feed.json", 'sent_ids_file': "bs_markets_sent_ids.json"},
        {'url': "https://www.business-standard.com/rss/industry-217.rss", 'rss': True,
         'output_file': "bs_industry_rss_feed.json", 'sent_ids_file': "bs_industry_sent_ids.json"},
        {'url': "https://www.business-standard.com/rss/home_page_top_stories.rss", 'rss': True,
         'output_file': "bs_top_stories_rss_feed.json", 'sent_ids_file': "bs_top_stories_sent_ids.json"},
    ]

    # Merge all sources
    sources.extend(bs_sources)

    logging.info("Starting news scraping process...")
    random.shuffle(sources)
    for source in sources:
        process_source(source, bot_token, chat_id)
    logging.info("Scraping process completed.")


if __name__ == "__main__":
    main()
