import pandas as pd
import os

def append_to_csv(file_path, articles):
    """
    Append articles to a CSV file
    """
    df = pd.DataFrame(articles)
    if os.path.isfile(file_path):
        df.to_csv(file_path, mode='a', header=False, index=False)
    else:
        df.to_csv(file_path, index=False)