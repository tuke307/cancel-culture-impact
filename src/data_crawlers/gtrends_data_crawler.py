import pandas as pd
import os
import sys
from serpapi import GoogleSearch
from datetime import datetime
from pytrends.request import TrendReq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from config import RAW_DATA_PATH, CELEBRITIES, SERP_API_KEY


def get_trends_pytrends(keyword: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get Google Trends data for the given keyword and timeframe using pytrends
    """
    pytrends = TrendReq(hl="en-US", tz=360)
    timeframe = f"{start_date} {end_date}"
    pytrends.build_payload([keyword], cat=0, timeframe=timeframe, geo="", gprop="")

    # Fetch interest over time
    trends_data = pytrends.interest_over_time()

    if not trends_data.empty:
        # Drop the 'isPartial' column if it exists
        if "isPartial" in trends_data.columns:
            trends_data = trends_data.drop(columns=["isPartial"])
        return trends_data
    else:
        print(
            f"No data found for keyword: {keyword} in timeframe: {start_date} to {end_date}"
        )
        return pd.DataFrame()


def get_trends_serpapi(keyword: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get Google Trends data for the given keyword and timeframe using SERPAPI
    """
    timeframe = [f"{start_date} {end_date}"]

    params = {
        "engine": "google_trends",
        "q": keyword,
        "date": timeframe,
        "api_key": SERP_API_KEY,
    }
    search = GoogleSearch(params)
    results = search.get_dict()

    if "timeline_data" in results.get("interest_over_time", {}):
        trends_data = results["interest_over_time"]["timeline_data"]
        data = []
        for entry in trends_data:
            date = entry["date"]
            value = entry["values"][0]["value"]
            data.append([date, value])
        trends_df = pd.DataFrame(data, columns=["Date", "Value"])
        return trends_df
    else:
        print(f"No data found for keyword: {keyword} in timeframe: {timeframe}")
        return pd.DataFrame()


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

        # trends_df = get_trends_pytrends(search_term, start_date, end_date)
        trends_df = get_trends_serpapi(search_term, start_date, end_date)

        if not trends_df.empty:
            trends_df.to_csv(comments_file_path, index=False)
            print(f"Saved trends data for {name} to {comments_file_path}")
        else:
            print(f"No trends data found for {name} within the specified dates.")
