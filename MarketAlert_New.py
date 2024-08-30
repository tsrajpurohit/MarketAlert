import requests
from bs4 import BeautifulSoup
import datetime
import time
import os
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_news(url, selector):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        articles = soup.select(selector)
        items = []

        for article in articles:
            title_tag = article.find('a')
            title = title_tag.text.strip() if title_tag else 'No title'
            link = title_tag['href'] if title_tag and title_tag.has_attr('href') else '#'
            description_tag = article.find('p')
            description = description_tag.text.strip() if description_tag else title

            items.append({
                'title': title,
                'link': link,
                'description': description,
                'pubDate': datetime.datetime.now().isoformat()
            })

        return items

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return []

def create_json_feed(items, output_file):
    feed_data = {
        'title': "RSS Feed Title",  # Adjust as needed
        'link': "https://example.com",  # Adjust as needed
        'description': "RSS Feed Description",  # Adjust as needed
        'lastBuildDate': datetime.datetime.now().isoformat(),
        'items': items
    }

    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(feed_data, file, indent=4)

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
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return set(json.load(file))
    return set()

def write_sent_ids(file_path, ids):
    with open(file_path, 'w') as file:
        json.dump(list(ids), file)

def main():
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
        }
    ]

    # Hardcoded Telegram bot token and chat ID
    bot_token = '5814838708:AAGMVW2amDqFcdmNMEiAetu0cLlgtMl-Kf8'
    chat_id = '-1001905543659'     # Replace with your actual chat ID

    if not bot_token or not chat_id:
        logging.error("Telegram bot token or chat ID is missing.")
        return

    while True:
        for source in sources:
            sent_ids = read_sent_ids(source['sent_ids_file'])
            items = scrape_news(source['url'], source['selector'])
            
            if items:
                today = datetime.datetime.now().date()
                new_items = [item for item in items if datetime.datetime.fromisoformat(item['pubDate']).date() == today]
                
                if new_items:
                    new_items_to_send = [item for item in new_items if item['link'] not in sent_ids]
                    
                    if new_items_to_send:
                        create_json_feed(new_items_to_send, source['output_file'])
                        logging.info(f"JSON feed created successfully: {source['output_file']}")

                        new_ids = set(item['link'] for item in new_items_to_send)
                        for item in new_items_to_send:
                            message = f"*{item['title']}*\n\n{item['description']}"
                            send_to_telegram(bot_token, chat_id, message)

                        # Update the list of sent item IDs
                        write_sent_ids(source['sent_ids_file'], sent_ids.union(new_ids))
                    
        time.sleep(120)

if __name__ == "__main__":
    main()
