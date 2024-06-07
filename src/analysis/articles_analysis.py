import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, CELEBRITIES


if __name__ == "__main__":
    for celebrity in CELEBRITIES:
        name = celebrity["name"]
        file_path = os.path.join(RAW_DATA_PATH, f"{name}_articles_gnews.csv")
        articles_df = pd.read_csv(file_path)

        print(f"Processing {name}...")
        print(f"Total articles: {len(articles_df)}")

        articles_df["published_on"] = pd.to_datetime(articles_df["published_on"])
        articles_df = articles_df.sort_values(by="published_on")

        print(articles_df.info())
