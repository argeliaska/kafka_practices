import requests
from bs4 import BeautifulSoup
from time import sleep
from kafka import KafkaProducer

def fetch_raw(recipe_url):
    html = None
    print('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url, headers=headers)
        if r.status_code == 200:
            html = r.text
            print("200", len(r.text), len(html))
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html.strip()

def get_recipes():
    recipes = []
    salad_url = 'https://www.allrecipes.com/recipes/96/salad'
    url = 'https://www.allrecipes.com/recipes/96/salad'
    print('Accessing list')

    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            links = []            
            links = [soup.find(id=f'mntl-card-list-items_2-0-{_}') for _ in range(4, 14) ] # soup.select('.fixed-recipe-card__h3 a')

            idx = 0

            for link in links:
                sleep(2)
                recipe = fetch_raw(link['href'])
                recipes.append(recipe)
                idx += 1
                if idx > 2:
                    break
    except Exception as ex:
        print('Exception in get_recipes')
        print(str(ex))
    finally:
        return recipes


def publish_messages(producer_instance, topic_name, key, value):
    try:
        print(key, len(value))
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
            bootstrap_servers=['localhost:9092'], # api_version=(0,0)
        )
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }

    all_recipes = get_recipes()
    if len(all_recipes) > 0:
        for recipe in all_recipes:
            kafka_producer = connect_kafka_producer()
            publish_messages(kafka_producer, 'raw_recipes', 'raw', recipe.strip())
            if kafka_producer is not None:
                kafka_producer.close()