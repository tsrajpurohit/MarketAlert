import requests
from bs4 import BeautifulSoup
import PyRSS2Gen
import datetime
import time
import os

# Function to scrape news from a specified URL
def scrape_news(url, selector):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Use the provided selector to find articles
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
                'pubDate': datetime.datetime.now()
            })

        return items

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return []

# Function to create the RSS feed
def create_rss_feed(items, output_file, feed_title, feed_link, feed_description):
    rss_items = []

    for item in items:
        rss_item = PyRSS2Gen.RSSItem(
            title=item['title'],
            link=item['link'],
            description=item['description'],
            guid=item['link'],
            pubDate=item['pubDate']
        )
        rss_items.append(rss_item)

    rss = PyRSS2Gen.RSS2(
        title=feed_title,
        link=feed_link,
        description=feed_description,
        lastBuildDate=datetime.datetime.now(),
        items=rss_items
    )

    # Write the RSS feed to the file with UTF-8 encoding
    with open(output_file, 'w', encoding='utf-8') as file:
        rss.write_xml(file)

# Function to send messages to Telegram
def send_to_telegram(bot_token, chat_id, message):
    telegram_api_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown'
    }
    try:
        response = requests.post(telegram_api_url, data=payload)
        response.raise_for_status()  # Check for request errors
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:
            print("Rate limit exceeded. Waiting before retrying...")
            time.sleep(60)  # Wait for 60 seconds before retrying
        else:
            print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send message to Telegram: {e}")

# Function to read the last sent item IDs from a file
def read_sent_ids(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return set(file.read().splitlines())
    return set()

# Function to write the sent item IDs to a file
def write_sent_ids(file_path, ids):
    with open(file_path, 'a') as file:
        file.write('\n'.join(ids) + '\n')

def main():
    sources = [
        {
            'url': "https://www.moneycontrol.com/news/business/stocks/",
            'selector': 'li.clearfix',
            'output_file': "moneycontrol_rss_feed.xml",
            'feed_title': "Moneycontrol News RSS Feed",
            'feed_link': "https://www.moneycontrol.com/news/business/stocks/",
            'feed_description': "RSS feed generated from Moneycontrol News website.",
            'sent_ids_file': 'moneycontrol_sent_ids.txt'
        },
        {
            'url': "https://economictimes.indiatimes.com/markets/stocks/earnings/news",
            'selector': 'div.eachStory',
            'output_file': "economictimes_earnings_rss_feed.xml",
            'feed_title': "Economic Times Earnings RSS Feed",
            'feed_link': "https://economictimes.indiatimes.com/markets/stocks/earnings/news",
            'feed_description': "RSS feed generated from Economic Times Earnings news.",
            'sent_ids_file': 'economictimes_earnings_sent_ids.txt'
        },
        {
            'url': "https://economictimes.indiatimes.com/markets/stocks/news",
            'selector': 'div.eachStory',
            'output_file': "economictimes_stocks_rss_feed.xml",
            'feed_title': "Economic Times Stocks RSS Feed",
            'feed_link': "https://economictimes.indiatimes.com/markets/stocks/news",
            'feed_description': "RSS feed generated from Economic Times Stocks news.",
            'sent_ids_file': 'economictimes_stocks_sent_ids.txt'
        }
    ]

    # Hardcoded Telegram bot token and chat ID
    bot_token = '5814838708:AAGMVW2amDqFcdmNMEiAetu0cLlgtMl-Kf8'
    chat_id = '-1001905543659'

    if not bot_token or not chat_id:
        print("Telegram bot token or chat ID is missing.")
        return

    while True:
        for source in sources:
            sent_ids = read_sent_ids(source['sent_ids_file'])
            items = scrape_news(source['url'], source['selector'])
            
            if items:
                today = datetime.datetime.now().date()
                new_items = [item for item in items if item['pubDate'].date() == today and item['link'] not in sent_ids]
                
                if new_items:
                    create_rss_feed(
                        new_items,
                        source['output_file'],
                        source['feed_title'],
                        source['feed_link'],
                        source['feed_description']
                    )
                    print(f"RSS feed created successfully: {source['output_file']}")

                    new_ids = set(item['link'] for item in new_items)
                    for item in new_items:
                        message = f"*{item['title']}*\n\n{item['description']}"
                        send_to_telegram(bot_token, chat_id, message)

                    # Update the list of sent item IDs
                    write_sent_ids(source['sent_ids_file'], new_ids)
                    
        # Wait for 2 minutes before checking again
        time.sleep(120)

if __name__ == "__main__":
    main()
