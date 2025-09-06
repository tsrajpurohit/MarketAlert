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
from tenacity import retry, stop_after_attempt, wait_exponential
from filelock import FileLock
from urllib.parse import urlparse, urlunparse

# Load environment variables from .env file
load_dotenv()

# Setup logging to log to both console and file with rotation
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler('scraper.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(file_handler)

# Get the directory of the current script
script_directory = os.path.dirname(os.path.abspath(__file__))

# List of User-Agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
]

def normalize_url(url):
    """Normalize URL by removing query parameters and fragments."""
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))

def parse_date(date_str):
    """Parse a date string into a datetime object and return ISO 8601 string."""
    try:
        date_str = date_str.replace('Updated On :', '').strip()
        parsed_date = parser.parse(date_str, fuzzy=True)
        return parsed_date.isoformat()
    except ValueError as e:
        logging.error(f"Date parsing error for date_str '{date_str}': {e}")
        return datetime.datetime.now().isoformat()

def extract_date(article):
    """Extract date from an article, handling both <span> and <time> tags."""
    date = None
    date_span = article.find('span')
    if date_span:
        date = date_span.get_text(strip=True)
    date_time = article.find('time')
    if date_time:
        date = date_time.get('datetime', None)
    return date

def dynamic_extract(element, tag_names, attribute_name=None):
    """Dynamically extract content from an HTML element using a list of possible tags."""
    for tag_name in tag_names:
        target = element.find(tag_name)
        if target:
            if attribute_name and target.has_attr(attribute_name):
                return target.get(attribute_name, '').strip()
            return target.get_text(strip=True)
    return ''

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
def scrape_news(url, selector):
    """Scrape news articles from a given URL and selector."""
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Referer': 'https://www.google.com/'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
        articles = soup.select(selector)
        items = []

        for article in articles:
            title = dynamic_extract(article, ['h2', 'h3', 'a', 'span'])
            link = dynamic_extract(article, ['a'], 'href')
            description = dynamic_extract(article, ['p', 'span', 'div'])

            if not title or not description or not link:
                logging.warning(f"Skipping article with missing data: title='{title}', link='{link}', description='{description}'")
                continue

            if link and not link.startswith('http'):
                link = requests.compat.urljoin(url, link)

            date_str = extract_date(article)
            pub_date = parse_date(date_str) if date_str else datetime.datetime.now().isoformat()

            items.append({
                'title': title,
                'link': link,
                'description': description,
                'pubDate': pub_date
            })

        return items

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}, Response: {getattr(e.response, 'text', '')[:200]}")
        return []

def create_or_update_json_feed(items, output_file):
    """Create or update a JSON feed with current date items."""
    output_path = os.path.join(script_directory, output_file)
    today = datetime.datetime.now().date()

    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as file:
            try:
                existing_data = json.load(file)
                if not isinstance(existing_data, dict) or 'items' not in existing_data:
                    logging.warning(f"Invalid JSON structure in {output_path}. Creating a new feed.")
                    existing_items = []
                else:
                    existing_items = existing_data.get('items', [])
                    filtered_items = []
                    for item in existing_items:
                        try:
                            pub_date = parser.parse(item['pubDate'])
                            if pub_date.date() == today:
                                filtered_items.append(item)
                        except ValueError:
                            logging.warning(f"Skipping item with invalid pubDate: {item.get('pubDate', 'Unknown')}")
                    existing_items = filtered_items
            except json.JSONDecodeError:
                logging.warning(f"Failed to decode JSON from {output_path}. Creating a new feed.")
                existing_items = []
    else:
        existing_items = []

    new_items = [item for item in items if datetime.datetime.fromisoformat(item['pubDate']).date() == today]
    updated_items = existing_items + new_items

    feed_data = {
        'title': "Financial News Feed",
        'link': "https://example.com",
        'description': "Latest financial news from multiple sources",
        'lastBuildDate': datetime.datetime.now().isoformat(),
        'items': updated_items
    }

    try:
        logging.info(f"Creating/Updating JSON feed: {output_path} with {len(updated_items)} items.")
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(feed_data, file, indent=4)
            logging.info(f"JSON feed successfully written to {output_path}.")
    except Exception as e:
        logging.error(f"Failed to write JSON feed to {output_path}: {e}")

def send_to_telegram(bot_token, chat_id, message):
    """Send a message to a Telegram chat."""
    telegram_api_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(telegram_api_url, data=payload, timeout=10)
        response.raise_for_status()
        time.sleep(2)  # Avoid Telegram rate limits
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            logging.warning(f"Telegram rate limit exceeded. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            logging.error(f"HTTP error sending to Telegram: {http_err}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send message to Telegram: {e}")

def read_sent_ids(file_path):
    """Read the set of sent IDs from a file."""
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                return set(json.load(file))
            except json.JSONDecodeError:
                logging.critical(f"Failed to decode JSON from {file_path}. Potential for duplicate messages.")
                return set()
    return set()

def write_sent_ids(file_path, ids):
    """Write a set of sent IDs to a file."""
    with FileLock(f"{file_path}.lock"):
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(list(ids), file)

def process_source(source, bot_token, chat_id):
    """Process a news source by scraping data, sending messages, and updating sent IDs."""
    exclude_keywords = ["KR Choksey", "Lilladher", "motilal", "ICICI Securities", "Sharekhan", "straight session", "Anand Rathi", "Emkay"]

    sent_ids_file_path = os.path.join(script_directory, source['sent_ids_file'])
    sent_ids = read_sent_ids(sent_ids_file_path)
    items = scrape_news(source['url'], source['selector'])
    
    if items:
        today = datetime.datetime.now().date()
        new_items = [item for item in items if datetime.datetime.fromisoformat(item['pubDate']).date() == today]
        
        filtered_items = []
        for item in new_items:
            title_lower = item['title'].lower()
            desc_lower = item['description'].lower()
            if not any(keyword.lower() in title_lower or keyword.lower() in desc_lower for keyword in exclude_keywords):
                filtered_items.append(item)
        
        new_items_to_send = [item for item in filtered_items if normalize_url(item['link']) not in sent_ids]
        logging.info(f"Filtered {len(filtered_items) - len(new_items_to_send)} duplicate articles for {source['url']}")
        
        if new_items_to_send:
            for item in new_items_to_send:
                message = f"*{item['title']}*\n\n{item['description']}\n\n[Read more]({item['link']})"
                send_to_telegram(bot_token, chat_id, message)

            create_or_update_json_feed(new_items_to_send, source['output_file'])
            logging.info(f"JSON feed created/updated successfully: {source['output_file']}")

            new_ids = set(normalize_url(item['link']) for item in new_items_to_send)
            write_sent_ids(sent_ids_file_path, sent_ids.union(new_ids))
            logging.info(f"Sent alerts updated in {sent_ids_file_path}")

def validate_env_vars(bot_token, chat_id):
    """Validate Telegram environment variables."""
    if not bot_token or not bot_token.startswith('bot'):
        logging.error("Invalid Telegram bot token format.")
        return False
    if not chat_id or not chat_id.startswith(('-', '@')):
        logging.error("Invalid Telegram chat ID format.")
        return False
    return True

def main():
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')

    if not validate_env_vars(bot_token, chat_id):
        return

    sources = [
        {
            'url': "https://www.moneycontrol.com/news/business/stocks/",
            'selector': 'li.clearfix',
            'output_file': "moneycontrol_rss_feed.json",
            'sent_ids_file': 'moneycontrol_sent_ids.json'
        },
        {
            'url': "https://www.moneycontrol.com/news/business/companies/",
            'selector': 'li.clearfix',
            'output_file': "moneycontrol_companies_rss_feed.json",
            'sent_ids_file': 'moneycontrol_companies_sent_ids.json'
        },
        {
            'url': "https://economictimes.indiatimes.com/markets/stocks/earnings/news",
            'selector': 'div.eachStory',
            'output_file': "economictimes_earnings_rss_feed.json",
            'sent_ids_file': 'economictimes_earnings_sent_ids.json'
        },
        {
            'url': "https://economictimes.indiatimes.com/markets/stocks/news",
            'selector': 'div.eachStory',
            'output_file': "economictimes_stocks_rss_feed.json",
            'sent_ids_file': 'economictimes_stocks_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/markets/news",
            'selector': 'div.listingstyle_cardlistlist__dfq57.cardlist',
            'output_file': "businessstandard_markets_news_rss_feed.json",
            'sent_ids_file': 'businessstandard_markets_news_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/markets/capital-market-news",
            'selector': 'div.cardlist',
            'output_file': "businessstandard_capital_market_news_rss_feed.json",
            'sent_ids_file': 'businessstandard_capital_market_news_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/topic/ipos",
            'selector': 'div.cardlist',
            'output_file': "businessstandard_ipos_rss_feed.json",
            'sent_ids_file': 'businessstandard_ipos_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/markets/mutual-fund",
            'selector': 'div.cardlist',
            'output_file': "businessstandard_mutual_fund_rss_feed.json",
            'sent_ids_file': 'businessstandard_mutual_fund_sent_ids.json'
        }
    ]

    logging.info("Starting news scraping process...")
    random.shuffle(sources)
    for source in sources:
        process_source(source, bot_token, chat_id)
    logging.info("Scraping process completed.")
    send_to_telegram(bot_token, chat_id, f"Scraping completed. Processed {len(sources)} sources.")

if __name__ == "__main__":
    main()
