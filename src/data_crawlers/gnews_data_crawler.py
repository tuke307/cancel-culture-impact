from datetime import datetime
import requests
import os
import time as t
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from config import GNEWS_API_KEY, RAW_DATA_PATH, CELEBRITIES
from helpers.file_functions import append_to_csv


def get_gnews_data(
    api_key: str,
    begin_date: datetime,
    end_date: datetime,
    search_term: str,
    page: int = 1,
):
    """
    Get news articles from GNews API.
    """
    url = "https://gnews.io/api/v4/search"
    params = {
        "apikey": api_key,
        "from": begin_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "q": search_term,
        "max": 100,  # this is the maximum
        "expand": "content",
        "sortby": "publishedAt",  # "relevance", "publishedAt
        "page": page,
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json()["articles"]
    else:
        return None


def crawl_gnews_data(
    name: str, search_term: str, start_date: str, end_date: str
) -> None:
    """
    Crawl news articles from GNews API and save them to a CSV file.
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")
    file_path = os.path.join(RAW_DATA_PATH, f"{name}_news.csv")

    print("Crawling GNews data...")
    print(f"Search term: {search_term}")
    print(f"Begin date: {start_date}")
    print(f"End date: {end_date}")

    if os.path.exists(file_path):
        os.remove(file_path)

    page = 1
    total_articles = 0
    while True:
        articles = get_gnews_data(
            api_key=GNEWS_API_KEY,
            begin_date=start_date,
            end_date=end_date,
            search_term=search_term,
            page=page,
        )
        if not articles:
            break

        data = []
        for article in articles:
            data.append(
                {
                    "title": article["title"],
                    "content": article["content"],
                    "published_on": article["publishedAt"],
                    "link": article["source"]["url"],
                    "source": article["source"]["name"],
                }
            )

        append_to_csv(file_path, data)

        total_articles += len(data)
        page += 1

        # Pause for 0.2 second to avoid exceeding the rate limit of 6 requests per second
        t.sleep(0.2)

    print(f"Total articles retrieved: {total_articles}")


if __name__ == "__main__":
    for celebrity in CELEBRITIES:
        crawl_gnews_data(
            celebrity["name"],
            celebrity["search_term"],
            celebrity["start_date"],
            celebrity["end_date"],
        )
