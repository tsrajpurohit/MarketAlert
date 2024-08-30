import requests
from bs4 import BeautifulSoup
import json
import datetime
import time
import os
from github import Github

# GitHub Configuration
GITHUB_TOKEN = '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'  # Replace with your GitHub token
REPO_NAME = 'tsrajpurohit/MarketAlert'  # Replace with your repo name
BRANCH_NAME = 'main'  # Replace with your branch name

# Initialize GitHub client
def initialize_github():
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(REPO_NAME)
    return repo.get_branch(BRANCH_NAME), repo

# Upload or update file on GitHub
def upload_to_github(repo, path, content, branch):
    try:
        file = repo.get_contents(path, ref=branch.name)
        repo.update_file(file.path, f'Update {file.path}', content, file.sha, branch=branch.name)
    except:
        repo.create_file(path, f'Create {path}', content, branch=branch.name)

# Scrape news from a specified URL
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
        print(f"Error fetching data from {url}: {e}")
        return []

# Create the JSON feed as a string
def create_json_feed(items):
    return json.dumps(items, indent=4)

# Send messages to Telegram
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
            print("Rate limit exceeded. Waiting before retrying...")
            time.sleep(60)
        else:
            print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send message to Telegram: {e}")

# Read the last sent item IDs from a JSON file
def read_sent_ids(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return set(json.load(file))
    return set()

# Write the sent item IDs to a JSON file
def write_sent_ids(file_path, ids):
    with open(file_path, 'w') as file:
        json.dump(list(ids), file)

def main():
    sources = [
        {
            'url': "https://www.moneycontrol.com/news/business/stocks/",
            'selector': 'li.clearfix',
            'output_file': "data/moneycontrol_rss_feed.json",
            'sent_ids_file': 'data/moneycontrol_sent_ids.json'
        },
        {
            'url': "https://economictimes.indiatimes.com/markets/stocks/earnings/news",
            'selector': 'div.eachStory',
            'output_file': "data/economictimes_earnings_rss_feed.json",
            'sent_ids_file': 'data/economictimes_earnings_sent_ids.json'
        },
        {
            'url': "https://economictimes.indiatimes.com/markets/stocks/news",
            'selector': 'div.eachStory',
            'output_file': "data/economictimes_stocks_rss_feed.json",
            'sent_ids_file': 'data/economictimes_stocks_sent_ids.json'
        }
    ]

    # Hardcoded Telegram bot token and chat ID
    bot_token = 'YOUR_TELEGRAM_BOT_TOKEN'
    chat_id = 'YOUR_TELEGRAM_CHAT_ID'

    if not bot_token or not chat_id:
        print("Telegram bot token or chat ID is missing.")
        return

    # Initialize GitHub client
    branch, repo = initialize_github()

    while True:
        for source in sources:
            sent_ids = read_sent_ids(source['sent_ids_file'])
            items = scrape_news(source['url'], source['selector'])
            
            if items:
                today = datetime.datetime.now().date()
                new_items = [item for item in items if datetime.datetime.fromisoformat(item['pubDate']).date() == today and item['link'] not in sent_ids]
                
                if new_items:
                    json_feed = create_json_feed(new_items)
                    
                    # Upload JSON feed to GitHub
                    upload_to_github(
                        repo,
                        source['output_file'],
                        json_feed,
                        branch
                    )
                    print(f"JSON feed created and uploaded successfully: {source['output_file']}")

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
