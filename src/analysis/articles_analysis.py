import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, CELEBRITIES

if __name__ == "__main__":
    for celebrity in CELEBRITIES:
        name = celebrity["name"]
        file_path = os.path.join(RAW_DATA_PATH, f"{name}_articles.csv")
        articles_df = pd.read_csv(file_path)

        print(f"Processing {name}...")
        print(f"Total articles: {len(articles_df)}")

        # TODO
        senti_scores_df = articles_df.copy()

        # Save sentiment scores
        output_file_path = os.path.join(
            PROCESSED_DATA_PATH, f"{name}_articles_senti.csv"
        )
        senti_scores_df.to_csv(output_file_path, index=False)
