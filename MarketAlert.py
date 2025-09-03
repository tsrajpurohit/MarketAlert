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
import feedparser

# Load environment variables from .env file
load_dotenv()

# Setup logging to log to both console and file with rotation
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler('scraper.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(file_handler)

# List of User-Agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
]

# Get the directory of the current script
script_directory = os.path.dirname(os.path.abspath(__file__))

def parse_date(date_str):
    """Parse a date string into a datetime object."""
    try:
        date_str = date_str.replace('Updated On :', '').strip()
        for fmt in [
            '%d %b %Y %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%d/%m/%Y %H:%M',
        ]:
            try:
                return datetime.datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        parsed_date = parser.parse(date_str, fuzzy=True)
        logging.info(f"Parsed date: {parsed_date}")
        return parsed_date
    except ValueError as e:
        logging.error(f"Date parsing error for date_str '{date_str}': {e}")
        return datetime.datetime.now()

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
            text = target.get(attribute_name, '').strip() if attribute_name else target.get_text(strip=True)
            return text.encode('utf-8', errors='replace').decode('utf-8')
    return ''

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=60))
def scrape_news(url, selector):
    """Scrape news articles from a given URL and selector."""
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.google.com/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        encoding = response.encoding if response.encoding and response.encoding != 'ISO-8859-1' else 'utf-8'
        content = response.content.decode(encoding, errors='replace')
        soup = BeautifulSoup(content, 'html.parser')
        logging.info(f"Response content length for {url}: {len(response.content)} bytes")
        articles = soup.select(selector)
        logging.info(f"Found {len(articles)} articles for {url} with selector '{selector}'")

        if len(articles) == 0:
            alternative_selectors = ['div.story', 'div.article', 'div.newsItem', 'li.article', 'div.storyItem', 'div.each-story']
            for alt_selector in alternative_selectors:
                alt_articles = soup.select(alt_selector)
                logging.info(f"Alternative selector '{alt_selector}' found {len(alt_articles)} articles")
            logging.debug(f"HTML snippet: {str(soup)[:500]}...")

        items = []
        for article in articles:
            title = dynamic_extract(article, ['h2', 'h3', 'a', 'span'])
            link = dynamic_extract(article, ['a'], 'href')
            description = dynamic_extract(article, ['p', 'span', 'div'])

            if not title:
                title = description if description else 'No title'

            if not title or not description or not link:
                logging.warning(f"Skipping article with missing data: title='{title}', link='{link}', description='{description}'")
                continue

            if link and not link.startswith('http'):
                link = requests.compat.urljoin(url, link)

            date_str = extract_date(article)
            pub_date = parse_date(date_str) if date_str else datetime.datetime.now()

            items.append({
                'title': title,
                'link': link,
                'description': description,
                'pubDate': pub_date.isoformat(),
                'guid': link
            })

        return items

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
        if isinstance(e, requests.exceptions.HTTPError):
            logging.error(f"Response headers: {e.response.headers}")
        return []

def scrape_rss(url, output_file):
    """Scrape articles from an RSS feed."""
    try:
        feed = feedparser.parse(url)
        items = []
        for entry in feed.entries:
            title = entry.get('title', 'No title').encode('utf-8', errors='replace').decode('utf-8')
            description = entry.get('description', '').encode('utf-8', errors='replace').decode('utf-8')
            pub_date = entry.get('published', datetime.datetime.now().isoformat())
            link = entry.get('link', '#')
            items.append({
                'title': title,
                'link': link,
                'description': description,
                'pubDate': pub_date,
                'guid': link
            })
        logging.info(f"Scraped {len(items)} articles from RSS feed {url}")
        return items
    except Exception as e:
        logging.error(f"Error parsing RSS feed {url}: {e}")
        return []

def create_or_update_json_feed(items, output_file):
    """Create or update a JSON feed with current date items."""
    output_path = os.path.join(script_directory, output_file)
    today = datetime.datetime.now().date()

    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as file:
            try:
                existing_data = json.load(file)
                existing_items = existing_data.get('items', [])
                existing_items = [item for item in existing_items if datetime.datetime.fromisoformat(item['pubDate']).date() == today]
            except json.JSONDecodeError:
                logging.warning(f"Failed to decode JSON from {output_path}. Creating a new feed.")
                existing_items = []
    else:
        existing_items = []

    new_items = items  # Temporarily process all items for debugging
    updated_items = existing_items + new_items

    feed_data = {
        'title': "RSS Feed Title",
        'link': "https://example.com",
        'description': "RSS Feed Description",
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
    """Send a message to Telegram."""
    telegram_api_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    message = f"{message}\n@Stock_Market_News_Buzz"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(telegram_api_url, data=payload, timeout=10)
        response.raise_for_status()
        time.sleep(2)
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            logging.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
        else:
            logging.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send message to Telegram: {e}")

def read_sent_ids(file_path):
    """Read the set of sent IDs from a file."""
    file_path = os.path.join(script_directory, file_path)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                return set(json.load(file))
            except json.JSONDecodeError:
                logging.warning(f"Failed to decode JSON from {file_path}. Returning empty set.")
                return set()
    return set()

def write_sent_ids(file_path, ids):
    """Write a set of sent IDs to a file."""
    file_path = os.path.join(script_directory, file_path)
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(list(ids), file)

def process_source(source, bot_token, chat_id):
    """Process a news source by scraping data, sending messages, and updating sent IDs."""
    exclude_keywords = ["KR Choksey", "Lilladher", "motilal", "ICICI Securities", "Sharekhan", "straight session", "Anand Rathi", "Emkay"]
    logging.info(f"Processing source: {source['url']}")
    sent_ids = read_sent_ids(source['sent_ids_file'])

    if source.get('is_rss'):
        items = scrape_rss(source['url'], source['output_file'])
        if source.get('output_file') == "businessstandard_capital_market_news_rss_feed.json":
            items = [item for item in items if "capital market" in item['title'].lower() or "capital market" in item['description'].lower()]
    else:
        items = scrape_news(source['url'], source['selector'])
    logging.info(f"Scraped {len(items)} articles from {source['url']}")

    if items:
        today = datetime.datetime.now().date()
        new_items = items  # Temporarily disable date filtering for debugging

        filtered_items = new_items  # Temporarily disable keyword filtering
        # filtered_items = []
        # for item in new_items:
        #     title_lower = item['title'].lower()
        #     desc_lower = item['description'].lower()
        #     if any(keyword.lower() in title_lower or keyword.lower() in desc_lower for keyword in exclude_keywords):
        #         logging.info(f"Filtered out: {item['title']} (Reason: Contains excluded keyword)")
        #         continue
        #     filtered_items.append(item)
        logging.info(f"After filtering, {len(filtered_items)} articles remain")

        new_items_to_send = [item for item in filtered_items if item.get('guid', item['link']) not in sent_ids]
        logging.info(f"Sending {len(new_items_to_send)} new articles to Telegram")

        if new_items_to_send:
            for item in new_items_to_send:
                message = f"*{item['title']}*\n\n{item['description']}\n\n{item['link']}"
                send_to_telegram(bot_token, chat_id, message)

            create_or_update_json_feed(new_items_to_send, source['output_file'])
            logging.info(f"JSON feed created/updated successfully: {source['output_file']}")

            new_ids = set(item.get('guid', item['link']) for item in new_items_to_send)
            write_sent_ids(source['sent_ids_file'], sent_ids.union(new_ids))
            logging.info(f"Sent alerts updated in {source['sent_ids_file']}")

def main():
    bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat_id = os.environ.get('TELEGRAM_CHAT_ID')

    if not bot_token or not chat_id:
        logging.error("Telegram bot token or chat ID is not set. Exiting.")
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
            'url': "https://economictimes.indiatimes.com/rssfeeds/1373380680.cms",
            'is_rss': True,
            'output_file': "economictimes_earnings_rss_feed.json",
            'sent_ids_file': 'economictimes_earnings_sent_ids.json'
        },
        {
            'url': "https://economictimes.indiatimes.com/rssfeeds/1373380680.cms",
            'is_rss': True,
            'output_file': "economictimes_stocks_rss_feed.json",
            'sent_ids_file': 'economictimes_stocks_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/rss/markets-106.xml",
            'is_rss': True,
            'output_file': "businessstandard_markets_news_rss_feed.json",
            'sent_ids_file': 'businessstandard_markets_news_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/rss/markets-106.xml",
            'is_rss': True,
            'output_file': "businessstandard_capital_market_news_rss_feed.json",
            'sent_ids_file': 'businessstandard_capital_market_news_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/rss/ipo-132.xml",
            'is_rss': True,
            'output_file': "businessstandard_ipos_rss_feed.json",
            'sent_ids_file': 'businessstandard_ipos_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/rss/mutual-fund-115.xml",
            'is_rss': True,
            'output_file': "businessstandard_mutual_fund_rss_feed.json",
            'sent_ids_file': 'businessstandard_mutual_fund_sent_ids.json'
        }
    ]

    logging.info("Starting news scraping process...")
    random.shuffle(sources)
    total_articles = 0
    for source in sources:
        process_source(source, bot_token, chat_id)
        total_articles += len(scrape_rss(source['url'], source['output_file']) if source.get('is_rss') else scrape_news(source['url'], source['selector']))
    logging.info(f"Scraping process completed. Total articles scraped: {total_articles}")
    send_to_telegram(bot_token, chat_id, f"Scraping completed. Total articles scraped: {total_articles}\n@Stock_Market_News_Buzz")

if __name__ == "__main__":
    main()
