import scrapy
from kafka import KafkaProducer
import json

class HespressSpider(scrapy.Spider):
    name = 'hespress'
    start_urls = ["https://www.hespress.com/"]

    def __init__(self, *args, **kwargs):
        super(HespressSpider, self).__init__(*args, **kwargs)
        self.producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],  # Adjust the Kafka broker address
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def parse(self, response):
        articles = response.css("div.carousel-item")

        for article in articles:
            title = article.css("h3 a::attr(title)").get()
            link = article.css("h3 a::attr(href)").get()

            if title:  # Check if the title is not None
                data = {'title': title, 'link': link}
                self.producer.send('hespress', data)  # Replace with your topic name
                self.producer.flush()