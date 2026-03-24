from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import uuid
from kafka import KafkaProducer
import time
import logging


def get_data():
    res=requests.get("https://randomuser.me/api/")
    res=res.json()
    res=res['results'][0]
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data



def stream_data():


    producer = KafkaProducer(
        bootstrap_servers=["broker:29092"],  # ✅ ใช้ตัวนี้ตัวเดียว
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        retries=5
    )
    curr_time= time.time()

    while True:
        if time.time() > curr_time+60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send("users_created", res)
            producer.flush()

        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue
      


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2026, 3, 23)
}

with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    streaming_task=PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )


#stream_data()
