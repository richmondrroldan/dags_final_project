import spacy
from spacytextblob.spacytextblob import SpacyTextBlob
import pandas as pd
import requests
import numpy as np
from dotenv import load_dotenv
import os

from datetime import timedelta
import boto3
from io import StringIO
from os import listdir
from os.path import isfile, join

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task
#from pathy import Bucket
# from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
# from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from datetime import datetime

load_dotenv('.env')

RAPI_KEY = os.getenv("RAPI_KEY")
RAPI_HOST = os.getenv("RAPI_HOST")


### Functions for processing data
def newswebsearch_api(url):
    headers = {
        "X-RapidAPI-Key": RAPI_KEY,
        "X-RapidAPI-Host": RAPI_HOST
    }

    query = "philippine economy"
    page_number = 1
    page_size = 20
    auto_correct = True
    safe_search = False
    with_thumbnails = True
    from_published_date = ""
    to_published_date = ""

    querystring = {"q": query,
                "pageNumber": page_number,
                "pageSize": page_size,
                "autoCorrect": auto_correct,
                "safeSearch": safe_search,
                "withThumbnails": with_thumbnails,
                "fromPublishedDate": from_published_date,
                "toPublishedDate": to_published_date}

    response = requests.get(url, headers=headers, params=querystring).json()

    #print(response)

    total_count = response["totalCount"]
    url = []
    title = []
    description = []
    body = []
    date_published = []
    language= []
    is_safe = []
    provider = []
    id = []

    for web_page in response["value"]:
        url.append(web_page["url"])
        title.append(web_page["title"])
        description.append(web_page["description"])
        body.append(web_page["body"])
        date_published.append(web_page["datePublished"])
        language.append(web_page["language"])
        is_safe.append(web_page["isSafe"])
        provider.append(web_page["provider"]["name"])
        id.append(web_page["id"])

    df = pd.DataFrame({'id': id, 'provider': provider, 'title': title, 'description': description, 'body':body,
            'date_published': date_published, 'url': url, 'language': language, 'is_safe': is_safe})
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = 'news_websearch_' + time_now + '.csv'
    upload_raw_news(csv_body=csv_buffer, uploaded_filename=filename)


def combine_files():
    file_path ='/data/raw/'
    file_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]

    df = pd.DataFrame()
    
    for i in file_list:
        fname = pd.read_csv(file_path + i)
        df = df.append(fname)
    df.drop_duplicates(subset=['id'], inplace=True, keep=False)

    return df


def score_news(df, nlp):
    sentiment = []
    for idx, rows in df.iterrows():
        doc = nlp(rows['body'])
        sent = doc._.blob.polarity
        sent = round(sent, 2)
        sentiment.append(sent)
    
    df['sentiment'] = sentiment
    df['sentiment_rating'] = np.where(df['sentiment'] > 0, "Positive", "Negative")

    return df


### GCP Functions
BUCKET_NAME = "news_sites"
STAGING = "rich/combined/"
RAW = "rich/raw/"

def list_files_gcs(service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    prefix = "rich/raw/"

    result = gcs_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
    return result

def download_files_gcs(remote_files, local_path):
    path_files = remote_files['Contents']

    lst = []
    for idx, x in enumerate(path_files):
        lst.append(x['Key'])
        
    file_list = lst[1:]
    file_names = pd.DataFrame(file_list)
    file_names[['Folder', 'File']] = file_names[0].str.split('/', expand=True)
    
    gcs_client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    for idx, rows in file_names.iterrows():
        gcs_client.download_file(Bucket=BUCKET_NAME, Key=rows[0], Filename=local_path + rows['File'])

def upload_combined_gcs(csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    gcs_resource.Object(BUCKET_NAME, STAGING + uploaded_filename).put(Body=csv_body.getvalue())

def upload_unique_to_gcs(feed_name):
    file_path ='/data/news/'
    file_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]

    df = pd.DataFrame()
    
    for i in file_list:
        fname = pd.read_csv(file_path + i)
        df = df.append(fname)
    df = df.drop_duplicates(subset=['id'])
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = feed_name + '_' + time_now + '.csv'
    upload_combined_gcs(csv_body=csv_buffer, uploaded_filename=filename)

    return df

def upload_raw_news(csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    gcs_resource.Object(BUCKET_NAME, RAW + uploaded_filename).put(Body=csv_body.getvalue())
