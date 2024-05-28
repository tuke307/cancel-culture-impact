import json
import pandas as pd
import os


def append_to_csv(file_path, articles):
    """
    Append articles to a CSV file
    """
    df = pd.DataFrame(articles)
    if os.path.isfile(file_path):
        df.to_csv(file_path, mode="a", header=False, index=False)
    else:
        df.to_csv(file_path, index=False)


def load_json(file_path):
    """
    Load a JSON file
    """
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Could not find the file at {file_path}. Please ensure the file exists.")
        return None
    except json.JSONDecodeError:
        print(
            f"Could not parse the JSON file at {file_path}. Please ensure the file contains valid JSON."
        )
        return None
