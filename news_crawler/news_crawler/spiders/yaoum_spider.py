import scrapy
from kafka import KafkaProducer
import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

class YaoumSpider(scrapy.Spider):
    name = 'yaoum'
    start_urls = ['https://alyaoum24.com/']

    def __init__(self, *args, **kwargs):
        super(YaoumSpider, self).__init__(*args, **kwargs)
        self.producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],  # Adjust the Kafka broker address
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def parse(self, response):
        articles = response.css("div.swiper-slide")

        for article in articles:
            title =  article.css("div.description-slider").css("h1::text").get()
            link = article.css("a.linkPost::attr(href)").get()

            if title:  # Check if the title is not None
                data = {'title': title, 'link': link}
                self.producer.send('yaoum', data)  # Replace with your topic name
                self.producer.flush()

