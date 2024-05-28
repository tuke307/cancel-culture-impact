import pandas as pd
from pytrends.request import TrendReq
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from config import RAW_DATA_PATH, CELEBRITIES


def get_trends(keywords: list, timeframes: list) -> pd.DataFrame:
    """
    Get Google Trends data for the given keywords and timeframes
    """
    pytrend = TrendReq()
    trends_list = []

    for timeframe in timeframes:
        while True:
            try:
                pytrend.build_payload(kw_list=keywords, timeframe=timeframe)
                trends_df = pytrend.interest_over_time()
                if trends_df.empty:
                    print(
                        f"No data found for keywords: {keywords} in timeframe: {timeframe}"
                    )
                    break
                trends_list.append(trends_df)
                break
            except Exception as e:
                if "429" in str(e):
                    print("Too many requests. Waiting for 60 seconds...")
                    time.sleep(60)
                else:
                    print(f"An error occurred: {e}")
                    break

    if trends_list:
        trends = pd.concat(trends_list, axis=0)
        return trends
    else:
        return pd.DataFrame()


def process_keywords(keywords: list, timeframes: list) -> pd.DataFrame:
    """
    Process the keywords
    """
    trends_file_path = os.path.join(RAW_DATA_PATH, "google_trends.csv")

    print(f"Processing Google Trends data for keywords: {keywords}...")
    print(f"Timeframes: {timeframes}")

    trends_df = get_trends(keywords, timeframes)

    if not trends_df.empty:
        # Saving Google Trends data to file
        trends_df.to_csv(trends_file_path)
        print(f"Saved Google Trends data to {trends_file_path}")
    else:
        print(f"No trends data to save for keywords: {keywords}")

    return trends_df


def convert_date_format(date_str):
    """
    Convert date from 'YYYY-MM-DDTHH:MM:SSZ' format to 'YYYY-MM-DD' format
    """
    date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    return date.strftime("%Y-%m-%d")


if __name__ == "__main__":
    for celebrity in CELEBRITIES:
        name = celebrity["name"]
        search_term = celebrity["search_term"]
        start_date = convert_date_format(celebrity["start_date"])
        end_date = convert_date_format(celebrity["end_date"])
        comments_file_path = os.path.join(RAW_DATA_PATH, f"{name}_google_trends.csv")

        print(f"Processing {name}...")
        print(f"Search Term: {search_term}")
        print(f"Start Date: {start_date}")
        print(f"End Date: {end_date}")

        timeframes = [f"{start_date} {end_date}"]
        keywords = [search_term]
        trends_df = process_keywords(keywords, timeframes)

        if not trends_df.empty:
            trends_df.to_csv(comments_file_path)
            print(f"Saved trends data for {name} to {comments_file_path}")
        else:
            print(f"No trends data found for {name} within the specified dates.")
