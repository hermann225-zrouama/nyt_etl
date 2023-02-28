
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String
import pandas as pd
from scripts.modelisation import predict_sentiment
from scripts.transform_data import extract_keywords
from scripts.transform_data import extract_authors
from scripts.transform_data import create_dataframe
from scripts.fetch_data import get_data
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import sys
sys.path.append('../venv/lib/python3.10/site-packages')

# Connexion à la base de données PostgreSQL sur un autre conteneur Docker
engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

# Définition de la base pour les modèles de table
Base = declarative_base()

# Définition de la table "articles"


class Article(Base):
    __tablename__ = 'articles'
    id = Column(Integer, primary_key=True)
    web_url = Column(String)
    snippet = Column(String)
    source = Column(String)
    headline_main = Column(String)
    headline_kicker = Column(String)
    headline_content_kicker = Column(String)
    headline_print_headline = Column(String)
    headline_name = Column(String)
    headline_seo = Column(String)
    headline_sub = Column(String)
    pub_date = Column(String)
    document_type = Column(String)
    news_desk = Column(String)
    section_name = Column(String)
    type_of_material = Column(String)
    _id = Column(String)
    word_count = Column(String)
    uri = Column(String)
    year = Column(String)
    month = Column(String)
    day = Column(String)
    sentiment = Column(String)


# Création de la base de données et de la table
Base.metadata.create_all(engine)

# Création d'une session pour interagir avec la base de données
Session = sessionmaker(bind=engine)
session = Session()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 27),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

dag = DAG(
    'nyt_financial_etl',
    default_args=default_args,
    description='NYT etl pipeline',
    schedule_interval=timedelta(days=1),
)


def _get_data(**kwargs):
    begin_date = datetime.today() - timedelta(days=1)
    end_date = datetime.today().strftime('%Y%m%d')
    json_data = get_data(api_key="wcVWQda87VRaAawNlhBhNsXkpX4XN9It",
                         begin_date=begin_date, end_date=end_date)
    # save data to csv with append mode
    data = pd.DataFrame(json_data['response']['docs'])
    data.to_csv("/usr/local/airflow/data/nyt_economy_brute.csv",
                mode='a', header=True, index=False)
    kwargs['ti'].xcom_push(key="data", value=json_data['response']['docs'])


def _extract_author_df(**kwargs):
    data = kwargs["ti"].xcom_pull(key="data", task_ids='fetch_data')
    df = pd.DataFrame(data)
    df = extract_authors(df)
    df.to_csv("/usr/local/airflow/data/author_data.csv", index=False)


def _extract_keyword_df(**kwargs):
    data = kwargs["ti"].xcom_pull(key="data", task_ids='fetch_data')
    df = pd.DataFrame(data)
    df = extract_keywords(df)
    df.to_csv("/usr/local/airflow/data/keyword_data.csv", index=False)


def _transform_data(**kwargs):
    data = kwargs["ti"].xcom_pull(key="data", task_ids='fetch_data')
    data = pd.DataFrame(data)
    df = create_dataframe(data)
    # scinder la date en jour, mois, année
    df['year'] = pd.DatetimeIndex(df['pub_date']).year
    df['month'] = pd.DatetimeIndex(df['pub_date']).month
    df['day'] = pd.DatetimeIndex(df['pub_date']).day
    df = predict_sentiment(df)
    # pour chaque ligne du dataframe, on crée un objet Article et on l'ajoute à la session
    for index, row in df.iterrows():
        article = Article(web_url=row['web_url'], snippet=row['snippet'], source=row['source'], headline_main=row['headline_main'], headline_kicker=row['headline_kicker'], headline_content_kicker=row['headline_content_kicker'], headline_print_headline=row['headline_print_headline'], headline_name=row['headline_name'], headline_seo=row['headline_seo'],
                          headline_sub=row['headline_sub'], pub_date=row['pub_date'], document_type=row['document_type'], news_desk=row['news_desk'], section_name=row['section_name'], type_of_material=row['type_of_material'], _id=row['_id'], word_count=row['word_count'], uri=row['uri'], year=row['year'], month=row['month'], day=row['day'], sentiment=row["sentiment"])
        session.add(article)
        session.commit()

    df.to_parquet("/usr/local/airflow/data/nyt_economy.parquet",
                  partition_cols=['year', 'month', 'day'], index=False)


fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=_get_data,
    dag=dag,
)


extract_author_df = PythonOperator(
    task_id='extract_author_df',
    python_callable=_extract_author_df,
    dag=dag,
)

extract_keyword_df = PythonOperator(
    task_id='extract_keyword_df',
    python_callable=_extract_keyword_df,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=_transform_data,
    dag=dag,
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)


start >> fetch_data >> [extract_author_df,
                        extract_keyword_df, transform_data] >> end
