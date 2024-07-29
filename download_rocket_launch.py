import json
import pathlib

import requests
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import requests.exceptions as requests_exceptions
import os

dag = DAG(
    dag_id='download_rocket_launch',
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

download_launches = BashOperator(
    task_id = 'download_launches',
    bash_command='mkdir -p /opt/airflow/output && curl -o /opt/airflow/output/launches.json -L "https://ll.thespacedevs.com/2.0.0/launch/upcoming"',
    dag=dag
)

def get_pictures():
    pathlib.Path('/opt/airflow/output/images').mkdir(parents=True, exist_ok=True)

    with open('/opt/airflow/output/launches.json') as f:
        launches = json.load(f)
        image_urls = [launch['image'] for launch in launches['results']]

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split('/')[-1]
                target_file = f"/opt/airflow/output/images/{image_filename}"
                with open(target_file, 'wb') as tf:
                    tf.write(response.content)
                print(f"Downloaded {image_filename} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")

            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=get_pictures,
    dag=dag
)

# notify = EmailOperator(
#     task_id="notify",
#     to='gogivamshee25@gmail.com',
#     subject="Pipeline Update",
#     html_content='''
# <h3>Pipeine Update</h3>
# <p>All the images are downloaded</p>
# '''
# )


notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /opt/airflow/output/images/ | wc -l) images."'
    "echo cwd",
    dag=dag
)

download_launches >> get_pictures >> notify