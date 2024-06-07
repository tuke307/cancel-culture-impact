from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from tqdm import tqdm
import emoji
import pandas as pd
import spacy

nlp = spacy.load("en_core_web_sm")


# %%
def remove_stopwords(text):
    stop_words = set(stopwords.words("english"))
    word_tokens = word_tokenize(text)

    # removing a word if it is in the list of stop words
    removed_text = [word for word in word_tokens if word not in stop_words]
    return " ".join(removed_text)


# %%
def get_lemma(text):

    if type(text) is list:
        text = " ".join(text)

    text = nlp(text)
    lemmatized = [token.lemma_ for token in nlp(text)]

    return " ".join(lemmatized)
