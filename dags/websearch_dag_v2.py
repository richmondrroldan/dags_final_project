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
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
# from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from datetime import datetime
import numpy as np

"""
    HELPER FUNCTIONS
"""
load_dotenv('.env')

RAPI_KEY = os.getenv("RAPI_KEY")
RAPI_HOST = os.getenv("RAPI_HOST")

BUCKET_NAME = "dags_finals"
STAGING = "combined/"
RAW = "raw/"

def list_files_gcs(prefix, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    result = gcs_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
    return result

def download_files_gcs(remote_files, local_path):
    path_files = remote_files['Contents']

    lst = []
    for idx, x in enumerate(path_files):
        lst.append(x['Key'])
        
    file_list = lst
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

def upload_unique_to_gcs(df):
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


def to_bigquery():
    prefix = "combined/"
    import_to_bq = list_files_gcs(prefix)
    path_files = import_to_bq['Contents']
    lst = []
    for idx, x in enumerate(path_files):
        lst.append(x['Key'])
        
    to_import = lst[-1:]

    return to_import
file_to_gcbq = to_bigquery()
### Functions for processing data
@task(task_id='websearch_upload')
def websearch_upload(ds=None, **kwargs):
    """
        API TO GCS TASKS
        Using RAPIDAPI to get data from web search
        Requirement:
            RAPIDAPI Subscription to the correct API Provider
                --LINK : https://rapidapi.com/contextualwebsearch/api/web-search/
            RAPIDAPI:
                --KEY
                --HOST
        TASK:
        --Scrapes results from websearch, scrapes the FF:
            url, title, description, body, date_published,
            language, is_safe, provider, id
        
        --Uploads CSV to target GCS BUCKET
    """


    url = "https://rapidapi.p.rapidapi.com/api/search/NewsSearchAPI"
    headers = {
        "X-RapidAPI-Key": Variable.get("RAPID_API_KEY"),
        "X-RapidAPI-Host": "contextualwebsearch-websearch-v1.p.rapidapi.com"
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

    print(response)

    #total_count = response["totalCount"]
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

@task(task_id='download_files')
def download_files():
    """
        ### Download Files GCS
        downloads files based on file list (list_files_gcs)
        requires:
            function : list_files_gcs()
            input : local_path --inside docker container under : ./data/
    """
    prefix = "raw/"
    remote_files = list_files_gcs(prefix)
    local_path = '/data/raw/'
    download_files_gcs(remote_files, local_path)

@task(task_id='combines_scores_uploads')
def combines_scores_uploads():
    """
        ### Combines, scores and upload data to GCS
        requires:
            tasks:
                download_files
                websearch_upload
    """

    file_path ='/data/raw/'
    file_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]

    df = pd.DataFrame()

    for i in file_list:
        fname = pd.read_csv(file_path + i)
        df = df.append(fname)
    df.drop_duplicates(subset=['id'], inplace=True)
    
    nlp = spacy.load("/model/en_core_web_sm/en_core_web_sm-3.3.0")
    nlp.add_pipe('spacytextblob')

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

    scored_df = score_news(df, nlp)

    upload_unique_to_gcs(scored_df)




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

    load_csv_tobq = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_example',
        bucket='dags_finals',
        source_objects=file_to_gcbq,
        destination_project_dataset_table="news.news",
        write_disposition='WRITE_TRUNCATE',
    )


    t_end = EmptyOperator(task_id="end_message")

    (t1 #>> websearch_upload()
        #>> download_files()
        # >> combines_scores_uploads()
        >> load_csv_tobq
        >> t_end)