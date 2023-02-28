import pandas as pd
import json
# from pyspark.sql.functions import explode
# from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DateType
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("NYT").getOrCreate()
# from dotenv import load_dotenv, dotenv_values

# load_dotenv()
# env_values = dotenv_values()
# api_key = env_values['NYT_API_KEY']


def keep_usefull_columns(df):
    """Keep only usefull columns from the dataframe."""
    # keywords and multimedia are nested dictionaries
    cols = ['web_url', 'snippet', 'print_page', 'print_section', 'source', 'headline', 'pub_date',
            'document_type', 'news_desk', 'section_name', 'type_of_material', '_id', 'word_count', 'uri']
    good_cols = []
    for col in cols:
        if col in df.columns:
            good_cols.append(col)
    df = df[good_cols]
    return df


def flatten_json(json_obj, prefix=''):
    """Flatten a nested JSON object."""
    flat_dict = {}
    for key, value in json_obj.items():
        if isinstance(value, dict):
            flat_subdict = flatten_json(value, prefix=prefix+key+'_')
            flat_dict.update(flat_subdict)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flat_subdict = flatten_json(
                        item, prefix=prefix+key+str(i)+'_')
                    flat_dict.update(flat_subdict)
        else:
            flat_dict[prefix+key] = value
    return flat_dict


def create_dataframe(data):
    """Create a dataframe from the data."""
    data = keep_usefull_columns(data)
    data = data.to_dict('records')
    # Creer une liste qui va contenir les dictionnaires aplatis
    flat_dicts = []

    # boucler sur chaque article du dictionnaire de données
    for article in data:
        # Aplatir le dictionnaire de l'article
        flat_article = flatten_json(article)
        # Ajouter le dictionnaire aplatit a la liste
        flat_dicts.append(flat_article)

    # Creer un DataFrame a partir de la liste de dictionnaires
    df = pd.DataFrame(flat_dicts)
    return df


def extract_authors(df):
    """Extract authors from the dataframe."""
    # Initialisation d'une liste qui va contenir les données pour le nouveau DataFrame
    author_data = []

    # Itération sur chaque ligne du DataFrame d'articles
    for index, row in df.iterrows():
        # Récupération de l'URI de l'article
        article_uri = row['uri']

        # Récupération de la colonne "byline" sous forme de dictionnaire
        byline = row['byline']

        # Vérification que la colonne "byline" n'est pas vide
        if byline:
            # Si la colonne "byline" n'est pas vide, itération sur chaque auteur
            for author in byline['person']:
                # Récupération du nom complet de l'auteur
                full_name = author['firstname'].lower(
                ) + ' ' + author['lastname'].lower()

                # Ajout d'un dictionnaire contenant les informations de l'auteur et de l'article dans la liste
                author_data.append({
                    'uri': article_uri,
                    'author_name': full_name
                })

    # Création d'un nouveau DataFrame à partir de la liste de dictionnaires
    author_df = pd.DataFrame(author_data)

    return author_df


def extract_keywords(article_df):
    """Extract keywords from the dataframe."""
    keyword_list = []
    for index, row in article_df.iterrows():
        uri = row['uri']
        keywords = row['keywords']
        for keyword in keywords:
            keyword_dict = {
                'uri': uri, 'name': keyword['name'], 'value': keyword['value'], 'rank': keyword['rank'], 'major': keyword['major']}
            keyword_list.append(keyword_dict)

    keyword_df = pd.DataFrame(keyword_list)
    return keyword_df


# def keep_usefull_columns_spark(df):
#     """Keep only useful columns from the dataframe."""
#     # keywords and multimedia are nested dictionaries
#     df = df.select(['web_url', 'snippet', 'print_page', 'print_section', 'source', 'headline', 'pub_date', 'document_type', 'news_desk', 'section_name', 'type_of_material', '_id', 'word_count', 'uri'])
#     return df

# def flatten_json_spark(json_obj, prefix=''):
#     """Flatten a nested JSON object."""
#     flat_dict = {}
#     for key, value in json_obj.items():
#         if isinstance(value, dict):
#             flat_subdict = flatten_json(value, prefix=prefix+key+'_')
#             flat_dict.update(flat_subdict)
#         elif isinstance(value, list):
#             for i, item in enumerate(value):
#                 if isinstance(item, dict):
#                     flat_subdict = flatten_json(item, prefix=prefix+key+str(i)+'_')
#                     flat_dict.update(flat_subdict)
#         else:
#             flat_dict[prefix+key] = value
#     return flat_dict

# def create_dataframe_sprark(data):
#     data = keep_usefull_columns(data)
#     # Create empty list to store flattened dictionaries
#     flat_dicts = []

#     # Loop through each article dictionary in the data list
#     for article in data.collect():
#         # Flatten the article dictionary
#         flat_article = flatten_json(article.asDict())
#         # Append the flattened dictionary to the list of flat dictionaries
#         flat_dicts.append(flat_article)

#     # Create a new dataframe from the list of flat dictionaries
#     df_schema = StructType([
#     StructField("web_url", StringType(), True),
#     StructField("snippet", StringType(), True),
#     StructField("print_page", StringType(), True),
#     StructField("print_section", StringType(), True),
#     StructField("source", StringType(), True),
#     StructField("headline_main", StringType(), True),
#     StructField("headline_kicker", StringType(), True),
#     StructField("headline_content_kicker", StringType(), True),
#     StructField("headline_print_headline", StringType(), True),
#     StructField("headline_name", StringType(), True),
#     StructField("headline_seo", StringType(), True),
#     StructField("headline_sub", StringType(), True),
#     StructField("pub_date", StringType(), True),
#     StructField("document_type", StringType(), True),
#     StructField("news_desk", StringType(), True),
#     StructField("section_name", StringType(), True),
#     StructField("type_of_material", StringType(), True),
#     StructField("_id", StringType(), True),
#     StructField("word_count", StringType(), True),
#     StructField("uri", StringType(), True)
# ])
#     df = spark.createDataFrame(flat_dicts,df_schema)
#     return df

# def create_author_df_spark(df):
#     # Define schema for authors DataFrame
#     author_schema = StructType([
#         StructField("uri", StringType(), True),
#         StructField("author_name", StringType(), True)
#     ])

#     # Initialisation d'une liste qui va contenir les données pour le nouveau DataFrame
#     author_data = []

#     # Itération sur chaque ligne du DataFrame d'articles
#     for row in df.select('uri', 'byline.person').collect():
#         # Récupération de l'URI de l'article
#         article_uri = row['uri']

#         # Récupération de la colonne "byline" sous forme de dictionnaire
#         byline = row['person']

#         # Vérification que la colonne "byline" n'est pas vide
#         if byline:
#             # Si la colonne "byline" n'est pas vide, itération sur chaque auteur
#             for author in byline:
#                 print(author)
#                 # Récupération du nom complet de l'auteur
#                 full_name = author['firstname'].lower() + ' ' + author['lastname'].lower()

#                 # Ajout d'un dictionnaire contenant les informations de l'auteur et de l'article dans la liste
#                 author_data.append({
#                     'uri': article_uri,
#                     'author_name': full_name
#                 })

#     # Création d'un nouveau DataFrame à partir de la liste de dictionnaires
#     author_df = spark.createDataFrame(author_data, schema=author_schema)
#     #  creer une colonne date a partir de la colonne pub_date sans l'heure
#     author_df = author_df.withColumn("date", author_df.pub_date.substr(0,10))

#     return author_df

# def extract_keywords_spark(article_df):
#     keyword_schema = StructType([
#         StructField("uri", StringType(), True),
#         StructField("name", StringType(), True),
#         StructField("value", StringType(), True),
#         StructField("rank", StringType(), True),
#         StructField("major", StringType(), True)
#     ])

#     # Initialisation d'une liste qui va contenir les données pour le nouveau DataFrame
#     keyword_data = []

#     # Itération sur chaque ligne du DataFrame d'articles
#     for row in article_df.select('uri', 'keywords').collect():
#         # Récupération de l'URI de l'article
#         article_uri = row['uri']

#         # Récupération de la colonne "keywords" sous forme de dictionnaire
#         keywords = row['keywords']

#         # Vérification que la colonne "keywords" n'est pas vide
#         if keywords:
#             # Si la colonne "keywords" n'est pas vide, itération sur chaque mot clé
#             for keyword in keywords:
#                 # Ajout d'un dictionnaire contenant les informations du mot clé et de l'article dans la liste
#                 keyword_data.append({
#                     'uri': article_uri,
#                     'name': keyword['name'],
#                     'value': keyword['value'],
#                     'rank': keyword['rank'],
#                     'major': keyword['major']
#                 })

#     # Création d'un nouveau DataFrame à partir de la liste de dictionnaires
#     keyword_df = spark.createDataFrame(keyword_data, schema=keyword_schema)

#     return keyword_df
