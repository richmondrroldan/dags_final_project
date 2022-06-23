from binascii import b2a_qp
from datetime import timedelta
from fileinput import filename
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
from airflow.operators.python import BranchPythonOperator
from airflow.models.baseoperator import chain
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
    
def get_latest_combined(prefix):
    gcs_client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    result = gcs_client.list_objects_v2(Bucket="dags_finals", Prefix=prefix, Delimiter='/')

    if "Contents" in result:
        list_all = result['Contents']
        latest = max(list_all, key=lambda x: x['LastModified'])
        return latest['Key']
    else:
        return 'NoExistingFile'

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

def upload_gcs(fpath, csv_body, uploaded_filename, service_secret=os.environ.get('SERVICE_SECRET')):
    gcs_resource = boto3.resource(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )
    gcs_resource.Object(BUCKET_NAME, fpath + uploaded_filename).put(Body=csv_body.getvalue())

def upload_unique_to_gcs(df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = 'news_' + time_now + '.csv'
    fpath = "scored/"
    upload_gcs(fpath=fpath, csv_body=csv_buffer, uploaded_filename=filename)

    return df

def upload_file_to_gcs(remote_file_name, local_file_name):
    gcs_client = boto3.client(
        "s3",
        region_name="auto",
        endpoint_url="https://storage.googleapis.com",
        aws_access_key_id=Variable.get("SERVICE_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("SERVICE_SECRET"),
    )

    gcs_client.upload_file(local_file_name, BUCKET_NAME, remote_file_name)

file_to_gcbq_inv = get_latest_combined("combined/invalid/")
file_to_gcbq_v = get_latest_combined("scored/")

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
            language, provider, id
        
        --Uploads CSV to target GCS BUCKET
    """


    url = "https://rapidapi.p.rapidapi.com/api/search/NewsSearchAPI"
    headers = {
        "X-RapidAPI-Key": Variable.get("RAPID_API_KEY"),
        "X-RapidAPI-Host": "contextualwebsearch-websearch-v1.p.rapidapi.com"
    }

    query = "philippine economy latest"
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

    url = []
    title = []
    description = []
    body = []
    date_published = []
    language= []
    provider = []
    id = []

    for web_page in response["value"]:
        url.append(web_page["url"])
        title.append(web_page["title"])
        description.append(web_page["description"])
        body.append(web_page["body"])
        date_published.append(web_page["datePublished"])
        language.append(web_page["language"])
        provider.append(web_page["provider"]["name"])
        id.append(web_page["id"])

    df = pd.DataFrame({'id': id, 'provider': provider, 'title': title, 'description': description, 'body':body,
            'date_published': date_published, 'url': url, 'language': language})
    
    ### START TESTING
    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    filename = 'news_websearch_' + time_now + '.csv'
    df.to_csv('./data/raw/' + filename, index=False)

    ### END TESTING

@task(task_id='combine_validate')
def combine_validate(ds=None, **kwargs):
    file_path ='/data/raw/'
    file_list = [f for f in listdir(file_path) if isfile(join(file_path, f))]
    df = pd.DataFrame()
    for i in file_list:
        fname = pd.read_csv(file_path + i)
        df = df.append(fname)
    df['body_check'] = np.where(df['body'].isna(), 'invalid', 'valid')
    df['provider_check'] = np.where(df['provider'].isna(), 'invalid', 'valid')
    df['date_check'] = np.where(df['date_published'].isna(), 'invalid', 'valid')
    df['is_valid'] = np.where(((df['body_check'] == 'valid') & (df['date_check'] == 'valid') & 
        (df['provider_check'] == 'valid')), 'valid', 'invalid')

    valid_df = df[df['is_valid'] == 'valid']
    valid_df = valid_df.drop(['provider_check', 'is_valid', 'date_check', 'body_check'], axis=1)

    invalid_df = df[df['is_valid'] == 'invalid']
    invalid_df = invalid_df.drop(['provider_check', 'is_valid', 'date_check', 'body_check'], axis=1)

    invalid_df.drop_duplicates(subset=['id'], inplace=True)
    valid_df.drop_duplicates(subset=['id'], inplace=True)

    time_now = datetime.now().strftime('%Y-%m-%d_%I-%M-%S')
    valid_fname = 'valid_combined_' + time_now + '.csv'
    invalid_fname = 'invalid_combined_' + time_now + '.csv'

    invalid_df.to_csv('./data/invalid/' + invalid_fname, index=False)
    valid_df.to_csv('./data/valid/' + valid_fname, index=False)

@task(task_id='uploads_invalid')
def uploads_invalid(ds=None, **kwargs):
    file_path ='./data/invalid/'
    files = listdir(file_path)
    files.sort(key=lambda x: os.path.getmtime(file_path + x))
    latest_file = files[-1:]
    lfile_str = "".join(latest_file)
    upload_file_to_gcs(local_file_name=file_path + lfile_str, remote_file_name="combined/invalid/" + lfile_str)

@task(task_id='uploads_valid')
def uploads_valid(ds=None, **kwargs):
    file_path ='./data/valid/'
    files = listdir(file_path)
    files.sort(key=lambda x: os.path.getmtime(file_path + x))
    latest_file = files[-1:]
    lfile_str = "".join(latest_file)
    upload_file_to_gcs(local_file_name=file_path + lfile_str, remote_file_name="combined/valid/" + lfile_str)

@task(task_id='scores_uploads')
def scores_uploads(ds=None, **kwargs):
    """
        ### Combines, scores and upload data to GCS
        requires:
            tasks:
                grab file from data/valid
                websearch_upload
    """
    file_path ='./data/valid/'
    files = listdir(file_path)
    files.sort(key=lambda x: os.path.getmtime("./data/valid/" + x))
    latest_file = files[-1:]
    lfile_str = "".join(latest_file)

    df = pd.read_csv(file_path + lfile_str)
    
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
    description='NEWS parsers',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 15),
    catchup=False,
    tags=['scrapers'],
) as dag:

    t1 = EmptyOperator(task_id="start_message", dag=dag)

    load_valid_bq = GCSToBigQueryOperator(
        task_id='load_valid_to_bq',
        bucket='dags_finals',
        source_objects=file_to_gcbq_v,
        allow_quoted_newlines=True,
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'provider', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'body', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'date_published', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'language', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sentiment', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'sentiment_rating', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        autodetect=False,
        destination_project_dataset_table="news.news",
        write_disposition='WRITE_TRUNCATE',
    )
    load_invalid_bq = GCSToBigQueryOperator(
        task_id='load_invalid_to_bq',
        bucket='dags_finals',
        source_objects=file_to_gcbq_inv,
        allow_quoted_newlines=True,
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'provider', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'body', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'date_published', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'language', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        autodetect=False,
        destination_project_dataset_table="news.invalid",
        write_disposition='WRITE_TRUNCATE',
    )
    invalidbq = EmptyOperator(task_id="invalid_in_bq")
    t_combined = EmptyOperator(task_id="combined_validated")

    end_dags = EmptyOperator(task_id="dags_end", dag=dag)

    chain(t1, combine_validate(), t_combined, [uploads_valid(),  uploads_invalid()], [scores_uploads(), load_invalid_bq], [load_valid_bq, invalidbq], end_dags)

