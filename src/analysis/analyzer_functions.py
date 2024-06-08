import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from tqdm import tqdm
import emoji
import pandas as pd
import spacy
import requests
import json


nlp = spacy.load("en_core_web_sm")
stop_words = set(stopwords.words("english"))

nltk.download("vader_lexicon")
nltk.download("punkt")
nltk.download("wordnet")
nltk.download("stopwords")
nltk.download("averaged_perceptron_tagger")


def libre_translate(text):
    """
    Detect language of a title using LibreTranslate API
    """
    try:
        # Send a POST request to the LibreTranslate API
        response = requests.post(
            "http://127.0.0.1:5000/translate",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"q": text, "source": "auto", "target": "en"}),
        )

        # If the request was successful, get the detected language
        if response.status_code == 200:
            return response.json()
        else:
            return "error"
    except Exception as e:
        print(f"Error detecting language for text: '{text}': {e}")
        return "error"


def remove_stopwords(text):
    """
    Remove stopwords from the text
    """
    word_tokens = word_tokenize(text)
    removed_text = [word for word in word_tokens if word not in stop_words]
    return " ".join(removed_text)


def get_lemma(text):
    """
    Lemmatize the text
    """
    if type(text) is list:
        text = " ".join(text)

    text = nlp(text)
    lemmatized = [token.lemma_ for token in nlp(text)]

    return " ".join(lemmatized)


def remove_emojis(text):
    """
    Remove emojis from the text
    """
    return emoji.replace_emoji(text, replace="")
