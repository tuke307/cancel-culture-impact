import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from googletrans import Translator
import os
import sys
import time
from tqdm import tqdm
import emoji

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, CELEBRITIES


# Download necessary NLTK data
nltk.download("vader_lexicon")
nltk.download("punkt")
nltk.download("wordnet")
nltk.download("stopwords")
nltk.download("averaged_perceptron_tagger")


def remove_stopwords(text: str) -> str:
    """
    Remove stopwords from the text
    """
    stop_words = set(stopwords.words("english"))
    word_tokens = word_tokenize(text)
    removed_text = [word for word in word_tokens if word not in stop_words]

    return " ".join(removed_text)


def perform_sentiment_analysis(comments_df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform sentiment analysis on the translated comments
    """
    senti_analyzer = SentimentIntensityAnalyzer()
    comments_df["text_no_stopwords"] = comments_df["translated_text"].apply(
        remove_stopwords
    )

    senti_scores_df = pd.DataFrame()
    for i, text in enumerate(comments_df["text_no_stopwords"]):
        senti_scores = senti_analyzer.polarity_scores(text)
        senti_scores_df = pd.concat(
            [senti_scores_df, pd.DataFrame(senti_scores, index=[i])], axis=0
        )

    print(f"Sentiment Scores (First 5): \n{senti_scores_df.head()}")

    return senti_scores_df


def remove_emoji(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove emojis from the text
    """
    for i in tqdm(df.index):
        text = df.at[i, "text"]
        if isinstance(text, str):  # Check if text is a string
            df.at[i, "text"] = emoji.replace_emoji(text, replace="")

    return df


def detect_language(translator: Translator, text: str) -> str:
    """
    Detect the language of the text
    """
    detection = translator.detect(text)
    time.sleep(0.3)  # Avoiding rate limit

    return detection.lang if detection is not None else "unknown"


def translate_text(translator: Translator, row: pd.Series) -> str:
    """
    Translate the text to English
    """
    if row["source_lang"] in ["en", "unknown"]:
        return row["text"]
    try:
        time.sleep(0.3)  # Avoiding rate limit
        return translator.translate(row["text"], src=row["source_lang"], dest="en").text
    except ValueError:
        print(f"Invalid source language: {row['source_lang']}")
        return row["text"]


if __name__ == "__main__":
    for celebrity in CELEBRITIES:
        name = celebrity["name"]
        file_path = os.path.join(RAW_DATA_PATH, f"{name}_youtube_comments.csv")
        comments_df = pd.read_csv(file_path)

        print(f"Processing {name}...")
        print(f"Total comments: {len(comments_df)}")

        # Removing emojis, empty comments, and resetting index
        comments_df = remove_emoji(comments_df)
        comments_df = comments_df[comments_df["text"].str.strip() != ""]
        comments_df.reset_index(drop=True, inplace=True)

        translator = Translator(timeout=None)

        # Detect language
        comments_df["source_lang"] = comments_df["text"].apply(
            lambda x: detect_language(translator, x)
        )
        comments_df = comments_df[
            comments_df["source_lang"] != "rw"
        ]  # Remove Kinyarwanda comments
        comments_df.reset_index(drop=True, inplace=True)

        # Translate text
        comments_df["translated_text"] = comments_df.apply(
            lambda row: translate_text(translator, row), axis=1
        )

        # Perform sentiment analysis
        senti_scores_df = perform_sentiment_analysis(comments_df)

        # Save sentiment scores
        output_file_path = os.path.join(
            PROCESSED_DATA_PATH, f"{name}_youtube_comments_senti.csv"
        )
        senti_scores_df.to_csv(output_file_path, index=False)
