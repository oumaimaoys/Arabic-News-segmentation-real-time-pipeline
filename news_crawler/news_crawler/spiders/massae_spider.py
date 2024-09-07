import scrapy
from kafka import KafkaProducer
import json

class MassaeSpider(scrapy.Spider):
    name = 'massae'
    start_urls = ['https://elmassae24.ma/']

    def __init__(self, *args, **kwargs):
        super(MassaeSpider, self).__init__(*args, **kwargs)
        self.producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],  # Adjust the Kafka broker address
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def parse(self, response):
        articles = response.css("div.jeg_slide_item")

        for article in articles:
            title = article.css("h2.jeg_post_title").css("a::text").get()
            link = article.css("h2.jeg_post_title").css("a::attr(href)").get()

            if title:  # Check if the title is not None
                data = {'title': title, 'link': link}
                self.producer.send('massae', data)  # Replace with your topic name
                self.producer.flush()