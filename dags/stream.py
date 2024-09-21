import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


def scrape_hespress():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],  # Adjust the Kafka broker address
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        start_url = "https://www.hespress.com/"
        response = requests.get(start_url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            articles = soup.select("div.carousel-item")  # Adjust this selector if necessary

            for article in articles:
                title = article.select_one("h3 a").get("title") if article.select_one("h3 a") else None
                link = article.select_one("h3 a").get("href") if article.select_one("h3 a") else None

                if title:  # Check if the title is not None
                    data = {'title': title, 'link': link}
                    producer.send('hespress', data)  # Replace with your topic name
                    producer.flush()
        else:
            print(f"Failed to retrieve page, status code: {response.status_code}")
    except Exception as e:
        logging.error("Error occurred fetching data from hespress", e)
        exit(1)


def scrape_massae():
    try :
        producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],  # Adjust the Kafka broker address
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        start_url = "https://elmassae24.ma/"
        response = requests.get(start_url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            articles = soup.select("div.jeg_slide_item")  # Adjust this selector if necessary

            for article in articles:
                title = article.select_one("h2.jeg_post_title a").text.strip() if article.select_one("h2.jeg_post_title a") else None
                link = article.select_one("h2.jeg_post_title a")['href'] if article.select_one("h2.jeg_post_title a") else None

                if title:  # Check if the title is not None
                    data = {'title': title, 'link': link}
                    producer.send('massae', data)  # Replace with your topic name
                    producer.flush()
        else:
            print(f"Failed to retrieve page, status code: {response.status_code}")
    except Exception as e:
        logging.error("Error occurred fetching data from massae:", e)
        exit(1)

def scrape_yaoum():
    try :
        producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],  # Adjust the Kafka broker address
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        start_url = "https://alyaoum24.com/"
        response = requests.get(start_url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            articles = soup.select("div.swiper-slide")  # Adjust this selector if necessary

            for article in articles:
                title = article.select_one("div.description-slider h1").text.strip() if article.select_one("div.description-slider h1") else None
                link = article.select_one("a.linkPost")['href'] if article.select_one("a.linkPost") else None

                if title:  # Check if the title is not None
                    data = {'title': title, 'link': link}
                    producer.send('yaoum', data)  # Replace with your topic name
                    producer.flush()
        else:
            print(f"Failed to retrieve page, status code: {response.status_code}")
    except Exception as e:
        logging.error("Error occurred fetching data from yaoum:", e)
        exit(1)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}



# Create the DAG
dag = DAG(
    'scrapy_spiders_dag',
    default_args=default_args,
    description='A DAG to run Scrapy spiders and stream data to Kafka',
    schedule_interval=timedelta(hours=1),  # Runs every hour
    catchup=False
)

# Define tasks to run the scraping functions
run_hespress_task = PythonOperator(
    task_id='run_hespress_task',
    python_callable=scrape_hespress,
    dag=dag,
)

run_massae_task = PythonOperator(
    task_id='run_massae_task',
    python_callable=scrape_massae,
    dag=dag,
)

run_yaoum_task = PythonOperator(
    task_id='run_yaoum_task',
    python_callable=scrape_yaoum,
    dag=dag,
)

run_hespress_spark_job = SparkSubmitOperator(
    task_id="run_hespress_spark_job",
    application="/opt/spark-scripts/hespress_spark_script.py",
    conn_id="spark_default",
    packages="org.apache.hadoop:hadoop-aws:3.2.0",
    task_concurrency=1,
    dag=dag
)

run_massae_spark_job = SparkSubmitOperator(
    task_id="run_massae_spark_job",
    application="/opt/spark-scripts/massae_spark_script.py",
    conn_id="spark_default",
    packages="org.apache.hadoop:hadoop-aws:3.2.0",
    task_concurrency=1,
    dag=dag
)

run_yaoum_spark_job = SparkSubmitOperator(
    task_id="run_yaoum_spark_job",
    application="/opt/spark-scripts/yaoum_spark_script.py",
    conn_id="spark_default",
    packages="org.apache.hadoop:hadoop-aws:3.2.0",
    task_concurrency=1,
    dag=dag
)
# Set task dependencies
run_hespress_task >> run_hespress_spark_job
run_massae_task >> run_massae_spark_job
run_yaoum_task >> run_yaoum_spark_job