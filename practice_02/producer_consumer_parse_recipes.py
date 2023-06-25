import json
from time import sleep

from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer
import random

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(0,0)
        )
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
    
def parse(markup):
    title = '-'
    submit_by = '-'
    description = '-'
    calories = 0
    ingredients = []
    rec = {}

    try:
        soup = BeautifulSoup(markup, 'lxml')
        # title
        title_section = soup.find(id='article-heading_1-0') # soup.select('.recipe-summary__h1')
        # submitter
        summiter_section = soup.select('.mntl-attribution__item-name') # soup.select('.submitter__name')
        # description
        description_section = soup.find(id='article-subheading_1-0') # soup.find(id='mntl-sc-block_1-0') # soup.select('.submitter__description')
        # ingredients
        ingredients_section = soup.select('.mntl-structured-ingredients__list') # soup.select('.recipe-ingred_txt')

        # calories
        calories_section = soup.select('.mntl-nutrition-facts-summary__table-cell type--dog-bold') # soup.select('.calorie-count')
        if calories_section:
            calories = calories_section.text.strip() # calories_section[0].text.replace('cals', '').strip()

        calories = random.randint(150, 250)

        if ingredients_section:
            for ingredient in ingredients_section:
                ingredient_text = ingredient.text.strip()
                ingredients_list = ingredient_text.split('\n')
                for ing in ingredients_list:
                    if ing != '':
                        ingredients.append(ing.strip())
        
        if description_section:
            description = description_section.text.strip()

        if title_section:
            title = title_section.text.strip()

        if summiter_section:
            submit_by = summiter_section[0].text.strip()
        
        rec = {'title': title, 
               'submitter': submit_by, 
               'description': description,
               'calories': calories,
               'ingredients': ingredients}
        
    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
    finally:
        return json.dumps(rec)
    
if __name__ == '__main__':
    print('Running Consumer..')
    parsed_records = []
    topic_name = 'raw_recipes'
    parsed_topic_name = 'parsed_recipes'

    consumer = KafkaConsumer(topic_name,
                             auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], # api_version=(0, 10),
                             consumer_timeout_ms=1000)
    
    for msg in consumer:
        html = msg.value
        result = parse(html)
        if result:
            parsed_records.append(result)
    consumer.close()
    sleep(5)

    if len(parsed_records) > 0:
        print('Publishing records..')
        producer = connect_kafka_producer()
        for rec in parsed_records:
            publish_message(producer, parsed_topic_name, 'parsed', rec)