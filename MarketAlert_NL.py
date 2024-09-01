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

# Load environment variables from .env file
load_dotenv()

# Setup logging to log to both console and file with rotation
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler('scraper.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(file_handler)

# Get the directory of the current script to ensure JSON files are created in the same folder
script_directory = os.path.dirname(os.path.abspath(__file__))

def parse_date(date_str):
    """
    Parse a date string into a datetime object.

    Parameters:
    date_str (str): The date string to parse.

    Returns:
    datetime.datetime: The parsed date.
    """
    try:
        # Remove 'Updated On :' prefix
        date_str = date_str.replace('Updated On :', '').strip()
        # Parse date with fuzzy matching
        parsed_date = parser.parse(date_str, fuzzy=True)
        logging.info(f"Parsed date: {parsed_date}")
        return parsed_date
    except ValueError as e:
        logging.error(f"Date parsing error for date_str '{date_str}': {e}")
        return datetime.datetime.now()

def extract_date(article):
    """
    Extract date from an article, handling both <span> and <time> tags.

    Parameters:
    article (BeautifulSoup object): The article element to extract the date from.

    Returns:
    str: The extracted date as a string.
    """
    date = None
    # Extract date from <span> or <time> elements
    date_span = article.find('span')
    if date_span:
        date = date_span.get_text(strip=True)
    date_time = article.find('time')
    if date_time:
        date = date_time.get('datetime', None)
    return date

def dynamic_extract(element, tag_names, attribute_name=None):
    """
    Dynamically extract content from an HTML element using a list of possible tags.

    Parameters:
    element (bs4.element.Tag): The parent element to search within.
    tag_names (list): List of tag names to look for in the element.
    attribute_name (str): The attribute to extract (if needed), defaults to None.

    Returns:
    str: Extracted content or an empty string if none found.
    """
    for tag_name in tag_names:
        target = element.find(tag_name)
        if target:
            if attribute_name and target.has_attr(attribute_name):
                return target.get(attribute_name, '').strip()
            return target.get_text(strip=True)
    return ''


def scrape_news(url, selector):
    """
    Scrape news articles from a given URL and selector.

    Parameters:
    url (str): The URL to scrape.
    selector (str): The CSS selector to locate articles.

    Returns:
    list: A list of dictionaries containing news article data.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        articles = soup.select(selector)
        items = []

        for article in articles:
            # Dynamically extract title, link, and description
            title = dynamic_extract(article, ['h2', 'h3', 'a', 'span'])
            link = dynamic_extract(article, ['a'], 'href')
            description = dynamic_extract(article, ['p', 'span', 'div'])

            # If title is missing, use description as a fallback
            if not title:
                title = description if description else 'No title'

            # Validate and construct link if it is relative
            if link and not link.startswith('http'):
                link = requests.compat.urljoin(url, link)

            date_str = extract_date(article)
            pub_date = parse_date(date_str) if date_str else datetime.datetime.now()

            items.append({
                'title': title,
                'link': link if link else '#',
                'description': description,
                'pubDate': pub_date.isoformat()
            })

        return items

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return []


def create_json_feed(items, output_file):
    """
    Create a JSON feed from a list of items.

    Parameters:
    items (list): A list of dictionaries containing news article data.
    output_file (str): The name of the output JSON file.
    """
    output_path = os.path.join(script_directory, output_file)
    feed_data = {
        'title': "RSS Feed Title",
        'link': "https://example.com",
        'description': "RSS Feed Description",
        'lastBuildDate': datetime.datetime.now().isoformat(),
        'items': items
    }

    try:
        logging.info(f"Creating JSON feed: {output_path} with {len(items)} items.")
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(feed_data, file, indent=4)
            logging.info(f"JSON feed successfully written to {output_path}.")
    except Exception as e:
        logging.error(f"Failed to write JSON feed to {output_path}: {e}")

def send_to_telegram(bot_token, chat_id, message):
    telegram_api_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(telegram_api_url, data=payload)
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:
            logging.warning("Rate limit exceeded. Waiting before retrying...")
            time.sleep(60)
        else:
            logging.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send message to Telegram: {e}")

def read_sent_ids(file_path):
    """
    Read the set of sent IDs from a file.

    Parameters:
    file_path (str): The path to the file containing sent IDs.

    Returns:
    set: A set of sent IDs.
    """
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                return set(json.load(file))
            except json.JSONDecodeError:
                logging.warning(f"Failed to decode JSON from {file_path}. Returning empty set.")
                return set()
    return set()

def write_sent_ids(file_path, ids):
    """
    Write a set of sent IDs to a file.

    Parameters:
    file_path (str): The path to the file where sent IDs should be written.
    ids (set): A set of sent IDs.
    """
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(list(ids), file)

def process_source(source, bot_token, chat_id):
    """
    Process a news source by scraping data, sending messages, and updating sent IDs.

    Parameters:
    source (dict): A dictionary containing the source URL, selector, output file, and sent IDs file.
    bot_token (str): The Telegram bot token.
    chat_id (str): The Telegram chat ID.
    """
    sent_ids_file_path = os.path.join(script_directory, source['sent_ids_file'])
    sent_ids = read_sent_ids(sent_ids_file_path)
    items = scrape_news(source['url'], source['selector'])
    
    if items:
        today = datetime.datetime.now().date()
        new_items = [item for item in items if datetime.datetime.fromisoformat(item['pubDate']).date() == today]
        new_items_to_send = [item for item in new_items if item['link'] not in sent_ids]
        
        if new_items_to_send:
            for item in new_items_to_send:
                message = f"*{item['title']}*\n\n{item['description']}"
                send_to_telegram(bot_token, chat_id, message)

            create_json_feed(new_items_to_send, source['output_file'])
            logging.info(f"JSON feed created successfully: {source['output_file']}")

            new_ids = set(item['link'] for item in new_items_to_send)
            write_sent_ids(sent_ids_file_path, sent_ids.union(new_ids))
            logging.info(f"Sent alerts updated in {sent_ids_file_path}")

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
            'selector': 'div.listingstyle_cardlistlist__dfq57.cardlist',
            'output_file': "businessstandard_capital_market_news_rss_feed.json",
            'sent_ids_file': 'businessstandard_capital_market_news_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/topic/ipos",
            'selector': 'div.listingstyle_cardlistlist__dfq57.cardlist',
            'output_file': "businessstandard_ipos_rss_feed.json",
            'sent_ids_file': 'businessstandard_ipos_sent_ids.json'
        },
        {
            'url': "https://www.business-standard.com/markets/mutual-fund",
            'selector': 'div.listingstyle_cardlistlist__dfq57.cardlist',
            'output_file': "businessstandard_mutual_fund_rss_feed.json",
            'sent_ids_file': 'businessstandard_mutual_fund_sent_ids.json'
        }
    ]

    # Process each source
    for source in sources:
        logging.info(f"Processing source: {source['url']}")
        process_source(source, bot_token, chat_id)
        # Wait a few seconds between requests to avoid overwhelming the server
        time.sleep(random.uniform(1, 3))

if __name__ == "__main__":
    main()
