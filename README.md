# Sentiment Analysis Pipeline with Airflow, Docker, and Spacy
The goal of this project is to provide a news dataset with a sentiment score based on `spacytextblob`.
## Data Source
RapidAPI [Web Search API](https://rapidapi.com/contextualwebsearch/api/web-search/)
### Data Source Requirements
- Register to [RapidAPI](https://rapidapi.com/hub)
- Subscribe to the data source linked above
- Use newsSearch endpoint

# Data Engineering Cohort 1 Capostone
Final Project by: Richmond Roldan

# For Local Machine Usage 
After cloning/forking the repository make sure to have a `docker-compose.yaml`.


If `docker-compose.yaml` is existing, run `docker compose up` in the same folder of the ` file to install the docker container locally. Once the container is running you can open Apache Airflow GUI by running  https://localhost:8080 in your browser.

## Things to add in Airflow UI
Navigate to the Admin -> Variables 

Add the ff keys:
- SERVICE_ACCESS_KEY - To be created in your Google Cloud Platform (IAM)
- SERVICE_SECRET - To be created in your Google Cloud Platform (IAM)
- RAPID_API_SECRET - Provided by RAPIDAPI in their website


# Main use 
The DAGs and functions that are used in this project are in `dags/websearch_dag_v2.py`.
## Functions Available:
- API Calls
- GCS Access (Upload/Grab Files)
- GCP BigQuery (GCS to BigQuery)
- Data Validation
- Sentiment Analysis (Spacy, SpacyTextBlob)

You can also increase the number of searched news by updating page size in the query parameters. More details are available in dags/websearch_dag_v2.py.

## Dags Tasks
It receives data from the API Endpoint and uploads it to GCS. Combines and validates the data then splits the dataset into **valid** and **invalid** datasets and uploads it to their respective folders in GCS. **Valid** data are then scored based on `spacytextblob` for sentiment analysis and generate a **scored** file. The results of the sentiment analysis are then uploaded to GCS. Both **scored** and **invalid** datasets are then transferred to GCP BigQuery.

# Challenges and Improvements
Setting up Docker and Airflow is the biggest challenge of this project.

## Project Improvement
Optimizing the current process on the DAGs used. Improving current data validations. Using other NLP(NER, Wordcount) methods to come up with a better and more insightful dataset. Infusing more data sources.

## Areas to learn
Setting up the `docker-compose.yaml`. Getting more familiar with all the technologies used in this project.


