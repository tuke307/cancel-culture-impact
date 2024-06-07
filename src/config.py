from datetime import datetime, time
import os
from helpers.file_functions import load_json
import pandas as pd

# Load environment variables
from dotenv import load_dotenv

load_dotenv()

# API Keys
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")
SERP_API_KEY = os.getenv("SERP_API_KEY")

# File Paths
CWD = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(CWD, "data", "raw")
PROCESSED_DATA_PATH = os.path.join(CWD, "data", "processed")

# Celebrities
config_path = os.path.join(CWD, "config.json")
CELEBRITIES = pd.DataFrame(load_json(config_path))


def prepare_celebrities(df):
    """
    Prepare the celebrities dataframe
    """
    df["name"] = df["name"].str.lower()
    df["search_term"] = df["search_term"].str.lower()

    df["start_date"] = pd.to_datetime(df["start_date"]).dt.tz_convert("UTC")
    df["end_date"] = pd.to_datetime(df["end_date"]).dt.tz_convert("UTC")
    df["cancellation_date"] = pd.to_datetime(df["cancellation_date"]).dt.tz_convert(
        "UTC"
    )

    return df


# Prepare the CELEBRITIES dataframe
CELEBRITIES = prepare_celebrities(CELEBRITIES)
