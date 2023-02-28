import pandas as pd
import numpy as np
import spacy
import requests

import requests

API_URL = "https://api-inference.huggingface.co/models/yiyanghkust/finbert-tone"
headers = {"Authorization": "Bearer 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"}


def query(payload):
    """Send a query to the API."""
    response = requests.post(API_URL, headers=headers, json=payload)
    return response.json()


output = query({
    "inputs": "I like you. I love you",
})


def predict_sentiment(data):
    """Predict the sentiment of the text."""
    # Extraire les textes du champ "résumé"
    texts = data['snippet'].tolist()

    # sentiment analysis corpus anglais
    nlp = spacy.load("en_core_web_sm")

    # Prédire les étiquettes de classe
    predictions = []
    for text in texts:
        # creation du corpus
        doc = nlp(text)
        predictions.append(doc.cats)

    # Transformer les prédictions en étiquettes de classe
    labels = []
    for prediction in predictions:
        if 'pos' in prediction and 'neg' in prediction:
            if prediction['pos'] > prediction['neg']:
                labels.append('avis positif')
            else:
                labels.append('avis negatif')
        else:
            labels.append('avis neutre')

    # Ajouter les prédictions au dataframe
    data['sentiment'] = labels
    return data
