import os
import pandas as pd
import numpy as np
import requests
import nltk
import json
import sys
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from googletrans import Translator
from tqdm import tqdm
import emoji
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from config import RAW_DATA_PATH

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to config.json
config_path = os.path.join(script_dir, "config.json")

with open(config_path, "r") as f:
    celebrities = json.load(f)

nltk.download("vader_lexicon")
nltk.download("punkt")
nltk.download("wordnet")
nltk.download("stopwords")
nltk.download("averaged_perceptron_tagger")


def get_video_id(
    publish_after: str, publish_before: str, iterations: int, search_term: str
):
    """
    Get the video IDs of the videos
    """

    video_id_list = []
    url = "https://yt.lemnoslife.com/noKey/search"

    for i in range(1, iterations + 1):
        parameters = {
            "part": "id,snippet",
            "maxResults": "50",
            "q": search_term,
            "type": "video",
            "publishedBefore": publish_before,
            "publishedAfter": publish_after,
            "order": "date",
        }
        if i > 1:
            parameters["nextPageToken"] = nextpagetoken

        res = requests.get(url, params=parameters)
        search_list = res.json()
        df = pd.json_normalize(search_list["items"])
        video_id_list.append(df["id.videoId"])

        print(f"Iteration {i}: Found {len(df['id.videoId'])} video IDs")

        if "nextPageToken" in search_list:
            nextpagetoken = search_list["nextPageToken"]

    video_ids = pd.concat(video_id_list, axis=0, ignore_index=True)

    return video_ids


def get_video_like_ratio(video_ids: list):
    """
    Get the like/view ratio of the videos
    """

    like_ratios = []
    url = "https://yt.lemnoslife.com/noKey/videos"

    for i, video_id in enumerate(video_ids):
        parameters = {"part": "id,snippet,statistics", "id": video_id}
        res = requests.get(url, params=parameters)
        video_data = res.json()

        try:
            view_count = int(video_data["items"][0]["statistics"]["viewCount"])
            like_count = int(video_data["items"][0]["statistics"]["likeCount"])
            like_ratios.append(like_count / view_count)
        except (KeyError, IndexError, ZeroDivisionError):
            print(f"Case {i} pass")
            like_ratios.append(np.nan)

        print(f"Status: {i + 1} / {len(video_ids)}")

    like_ratios = pd.Series(like_ratios)
    like_ratios.fillna(like_ratios.mean(), inplace=True)

    return like_ratios.mean()


def get_comments(video_ids: list):
    """
    Get comments from the videos
    """

    list_text = []
    list_date = []
    url = "https://yt.lemnoslife.com/noKey/commentThreads"

    for video_id in video_ids:
        parameters = {"part": "id,snippet", "maxResults": "50", "videoId": video_id}
        res = requests.get(url, params=parameters)
        comment_list = res.json()

        try:
            comment_df = pd.json_normalize(comment_list["items"])
            comment_df["snippet.topLevelComment.snippet.updatedAt"] = comment_df[
                "snippet.topLevelComment.snippet.updatedAt"
            ].apply(lambda x: x[:10])
            comment_df["updateDate"] = pd.to_datetime(
                comment_df["snippet.topLevelComment.snippet.updatedAt"]
            )
            list_text.append(comment_df["snippet.topLevelComment.snippet.textOriginal"])
            list_date.append(comment_df["updateDate"])

            print(f"Video ID {video_id}: Found {len(comment_df)} comments")
        except KeyError:
            pass

    df_text = pd.concat(list_text, axis=0, ignore_index=True)
    df_date = pd.concat(list_date, axis=0, ignore_index=True)
    df = pd.concat([df_text, df_date], axis=1)
    df.columns = ["text", "updateDt"]

    return df


def remove_emoji(df: pd.DataFrame):
    """
    Remove emojis from the text
    """

    for i in tqdm(df.index):
        df.at[i, "text"] = emoji.replace_emoji(df.at[i, "text"], replace="")
    return df


def remove_stopwords(text: str):
    """
    Remove stopwords from the text
    """
    stop_words = set(stopwords.words("english"))
    word_tokens = word_tokenize(text)
    removed_text = [word for word in word_tokens if word not in stop_words]
    return " ".join(removed_text)


def process_celebrities(celebrities: list):
    """
    Process the celebrities
    """

    for celebrity in celebrities:
        name = celebrity["name"]
        search_term = celebrity["search_term"]
        start_date = celebrity["start_date"]
        end_date = celebrity["end_date"]
        file_path = os.path.join(RAW_DATA_PATH, f"{name}_youtube_comments.csv")

        video_ids = get_video_id(start_date, end_date, 1, search_term)
        like_ratio = get_video_like_ratio(video_ids)
        comments_df = get_comments(video_ids)

        # Removing emojis, empty comments, and resetting index
        comments_df = remove_emoji(comments_df)
        comments_df = comments_df[comments_df["text"].str.strip() != ""]
        comments_df.reset_index(drop=True, inplace=True)

        translator = Translator(timeout=None)
        translator.raise_exception = True

        def detect_language(text):
            detection = translator.detect(text)
            time.sleep(0.3)  # Avoiding rate limit
            return detection.lang if detection is not None else "unknown"

        comments_df["source_lang"] = comments_df["text"].apply(detect_language)
        comments_df = comments_df[
            comments_df["source_lang"] != "rw"
        ]  # Remove Kinyarwanda comments
        comments_df.reset_index(drop=True, inplace=True)

        def translate_text(row):
            if row["source_lang"] == "en" or row["source_lang"] == "unknown":
                return row["text"]
            else:
                try:
                    time.sleep(0.3)  # Avoiding rate limit
                    return translator.translate(
                        row["text"], src=row["source_lang"], dest="en"
                    ).text
                except ValueError:
                    print(f"Invalid source language: {row['source_lang']}")
                    return row["text"]

        comments_df["translated_text"] = comments_df.apply(translate_text, axis=1)

        # Saving raw comments to file
        comments_df.to_csv(file_path, index=False)

        # Example outputs
        print(f"Like/View Ratio for {search_term}: {like_ratio}")

    return comments_df


def perform_sentiment_analysis(comments_df):
    """
    Perform sentiment analysis on the comments
    """

    senti_analyzer = SentimentIntensityAnalyzer()
    senti_scores_df = pd.DataFrame()

    # Removing stopwords
    comments_df["text_no_stopwords"] = comments_df["text"].apply(remove_stopwords)

    for i, text in enumerate(comments_df["text_no_stopwords"]):
        senti_scores = senti_analyzer.polarity_scores(text)
        senti_scores_df = pd.concat(
            [senti_scores_df, pd.DataFrame(senti_scores, index=[i])], axis=0
        )

    # Example outputs
    print(f"Sentiment Scores (First 5): \n{senti_scores_df.head()}")

    return senti_scores_df


if __name__ == "__main__":
    comments_df = process_celebrities(celebrities)
    senti_scores_df = perform_sentiment_analysis(comments_df)
