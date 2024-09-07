from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# Create the DAG
dag = DAG(
    'scrapy_spiders',
    default_args=default_args,
    description='A DAG to run Scrapy spiders and stream data to Kafka',
    schedule_interval=timedelta(hours=1),  # Runs every hour
)

# Define a task to run the spider
run_spider = BashOperator(
    task_id='run_hespress_spider',
    bash_command='cd /home/ouyassine/Documents/projects/data_engineering_1/news_crawler && scrapy crawl hespress',
    dag=dag,
)

# Define a task to run the spider
run_spider = BashOperator(
    task_id='run_massae_spider',
    bash_command='cd /home/ouyassine/Documents/projects/data_engineering_1/news_crawler && scrapy crawl massae',
    dag=dag,
)

# Define a task to run the spider
run_spider = BashOperator(
    task_id='run_yaoum_spider',
    bash_command='cd /home/ouyassine/Documents/projects/data_engineering_1/news_crawler && scrapy crawl yaoum',
    dag=dag,
)
