from datetime import timedelta
import boto3
import os
from io import StringIO
import pandas as pd
from os import listdir
from os.path import isfile, join
import spacy
from spacytextblob.spacytextblob import SpacyTextBlob
from dotenv import load_dotenv
import os
import requests
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
import numpy as np

nlp = spacy.load('en_core_web_sm')


load_dotenv('.env')

BUCKET_NAME = "news_sites"
STAGING = "rich/combined/"
RAW = "rich/raw/"

url = "https://rapidapi.p.rapidapi.com/api/search/NewsSearchAPI"

nlp = spacy.load("/mode/en_core_web_sb/en_core_web_sm-3.3.0")
nlp.add_pipe('spacytextblob')

RAPI_KEY = os.getenv("RAPI_KEY")
RAPI_HOST = os.getenv("RAPI_HOST")


### Functions for processing data

def newswebsearch_api(url):
    """
        ### NEWS WEBSEARCH RAPIDAPI
        Requires:
            RAPID API KEY
            RAPID API HOST
            Values available upon subscription to RAPIDAPI
        Scrapes results from websearch, scrapes the FF:
            url, title, description, body, date_published,
            language, is_safe, provider, id
        
        Uploads CSV to target GCS BUCKET
    """
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
    """
        ###COMBINE FILES
        Combines downloaded file from GCS Bucket (Raw API response)
        Reuqires 
            function : download_files_gcs()
    """
    file_path ='/data/raw/'
    file_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]

    df = pd.DataFrame()
    
    for i in file_list:
        fname = pd.read_csv(file_path + i)
        df = df.append(fname)
    df.drop_duplicates(subset=['id'], inplace=True, keep=False)

    return df

combined_df = combine_files()

def score_news(df, nlp):
    """
        ### SCORES COMBINED DF
        Scores Combined DF using spacyblobtext
        requires:
            nlp = load spacy model
            df based on --  function : combine_files()
    """
    sentiment = []
    for idx, rows in df.iterrows():
        doc = nlp(rows['body'])
        sent = doc._.blob.polarity
        sent = round(sent, 2)
        sentiment.append(sent)
    
    df['sentiment'] = sentiment
    df['sentiment_rating'] = np.where(df['sentiment'] > 0, "Positive", "Negative")

    return df

def list_files_gcs(service_secret=os.environ.get('SERVICE_SECRET')):
    """
        ### LIST FILES GCS
        lists current files from GCS Bucket
    """
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

remote_files = list_files_gcs()

def download_files_gcs(remote_files, local_path):
    """
        ### Download Files GCS
        downloads files based on file list (list_files_gcs)
        requires:
            function : list_files_gcs()
            input : local_path --inside docker container under : ./data/
    """

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
    """
        ### Upload combined GCS
        requirement for uploading to gcs
    """
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    gcs_resource.Object(BUCKET_NAME, STAGING + uploaded_filename).put(Body=csv_body.getvalue())

scored_df = score_news()
def upload_unique_to_gcs(df):
    """
        ### Upload unique to gcs
        Actual upload
        requires
            df based on -- function : score_news()
            function : upload_combined_gcs()
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = 'news_' + time_now + '.csv'
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

@task(task_id="newsearch_api")
def newsearch_api(ds=None, **kwargs):
    newswebsearch_api(url)
    return True

@task(task_id="download_files_gcs")
def download_files_gcs(ds=None, **kwargs):
    download_files_gcs(remote_files, "/data/raw/")
    return True

@task(task_id="score_df")
def score_df(ds=None, **kwargs):
    score_news(combined_df, nlp)
    return True

with DAG(
    'rich_news_site_scrapers',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        # 'depends_on_past': False,
        # 'email': ['caleb@eskwelabs.com'],
        # 'email_on_failure': False,
        # 'email_on_retry': False,
        # 'retries': 1,
        # 'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='RSS parsers',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 15),
    catchup=False,
    tags=['scrapers'],
) as dag:

    t1 = EmptyOperator(task_id="start_message")

    t_end = EmptyOperator(task_id="end_message")

    (t1 >> newsearch_api()
        >> download_files_gcs()
        >> score_df()
        >> t_end)