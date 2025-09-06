import json
from dateutil import parser
import os
import datetime

def fix_json_dates(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                print(f"Failed to decode JSON from {file_path}. Skipping.")
                return
        for item in data.get('items', []):
            try:
                item['pubDate'] = parser.parse(item['pubDate']).isoformat()
            except ValueError:
                print(f"Invalid pubDate in {file_path}: {item.get('pubDate', 'Unknown')}. Setting to current time.")
                item['pubDate'] = datetime.datetime.now().isoformat()
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)
        print(f"Fixed JSON file: {file_path}")

script_directory = os.path.dirname(os.path.abspath(__file__))
json_files = [
    "moneycontrol_rss_feed.json",
    "moneycontrol_companies_rss_feed.json",
    "economictimes_earnings_rss_feed.json",
    "economictimes_stocks_rss_feed.json",
    "businessstandard_markets_news_rss_feed.json",
    "businessstandard_capital_market_news_rss_feed.json",
    "businessstandard_ipos_rss_feed.json",
    "businessstandard_mutual_fund_rss_feed.json"
]
for json_file in json_files:
    fix_json_dates(os.path.join(script_directory, json_file))
