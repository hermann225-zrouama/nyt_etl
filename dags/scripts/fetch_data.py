# get data from api
import requests
import json
import time

# get data from api

def get_data(begin_date, end_date, sort="newest", query="finance", api_key=""):
    url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
    params = {
        'api-key': api_key,
        'q': query,
        'begin_date': begin_date,
        'end_date': end_date,
        'sort': sort
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data
