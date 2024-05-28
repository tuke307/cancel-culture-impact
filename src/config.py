from datetime import datetime, time
import os
import sys

# Load environment variables
from dotenv import load_dotenv

load_dotenv()

# API Keys
GNEWS_API_KEY = os.getenv("GNEWS_API_KEY")

# File Paths
CWD = os.getcwd()
RAW_DATA_PATH = os.path.join(CWD, "data", "raw")
PROCESSED_DATA_PATH = os.path.join(CWD, "data", "processed")
