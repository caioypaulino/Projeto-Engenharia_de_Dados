from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 11, 30, 10, 00)
}

def get_data():
    import requests

    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]

    return response

def format_data(response):
    data = {}
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']
    data['gender'] = response['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} {location['city']}, {location['state']}, {location['country']}f"

    return data

def stream_data():
    import json

    response = get_data()
    response = format_data(response)
    print(json.dumps(response, indent=3))


with DAG('user_automation',
         default_args = default_args,
         schedule = '@daily',
         catchup = False) as dag:

    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )

stream_data();