from datetime import datetime, timedelta
import requests
import os
import time as t
import sys
import math

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
        "max": 50,  # this is the maximum
        "expand": "content",
        "sortby": "relevance",  # "relevance", "publishedAt
        "page": page,
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json()
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
    file_path = os.path.join(RAW_DATA_PATH, f"{name}_articles_gnews.csv")

    print("Crawling GNews data...")
    print(f"Search term: {search_term}")
    print(f"Begin date: {start_date}")
    print(f"End date: {end_date}")

    if os.path.exists(file_path):
        os.remove(file_path)

    current_start_date = start_date
    while current_start_date < end_date:
        # 30 days at a time
        current_end_date = current_start_date + timedelta(days=30)
        if current_end_date > end_date:
            current_end_date = end_date

        print(f"range: {current_start_date} to {current_end_date}")

        page = 1
        total_articles = 0
        total_articles_showed = False
        articles_processed = 0

        while True:
            articles = get_gnews_data(
                api_key=GNEWS_API_KEY,
                begin_date=current_start_date,
                end_date=current_end_date,
                search_term=search_term,
                page=page,
            )

            if not articles:
                break

            if not total_articles_showed:
                total_articles = articles["totalArticles"]
                print(f"A total of {total_articles} articles found to crawl")
                total_articles_showed = True

            data = []
            if len(articles["articles"]) == 0:
                break
            for article in articles["articles"]:
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

            articles_processed += len(data)
            print(
                f"Page {page}/{math.ceil(total_articles/50)} crawled. processed articles: {articles_processed}"
            )

            page += 1

            # Pause for 0.2 second to avoid exceeding the rate limit of 6 requests per second
            t.sleep(0.2)

        current_start_date = current_end_date + timedelta(seconds=1)

    print(f"Total articles retrieved: {total_articles}")


if __name__ == "__main__":
    for celebrity in CELEBRITIES:
        crawl_gnews_data(
            celebrity["name"],
            celebrity["search_term"],
            celebrity["start_date"],
            celebrity["end_date"],
        )
