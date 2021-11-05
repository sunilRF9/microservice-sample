import requests
import pika
import json
from bs4 import BeautifulSoup
from uuid import uuid4
import csv
from datetime import datetime
today = datetime.today().strftime('%Y-%m-%d')
###RMQ Connection

connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
        )
channel = connection.channel()
channel.queue_declare(queue='scraper')

#Read links from CSV
#with open('/home/coutinho/limeroadQA/fk.csv', 'r') as infile:
with open('ffk.csv', 'r') as infile:
  # read the file as a dictionary for each row ({header : value})
  reader = csv.DictReader(infile)
  data = {}
  for row in reader:
    for header, value in row.items():
      try:
        data[header].append(value)
      except KeyError:
        data[header] = [value]

link = data['Flipkart_URL']
prod_id = data['UPID']

final = {}
for k, i in enumerate(link):
    BASE_URL = "https://www.flipkart.com"
    final['id'] = prod_id[k]
    final['url'] = i

    response = requests.get(i)


    #Soup Obj
    soup = BeautifulSoup(response.text, 'lxml')
    #Price
    try:
        price = soup.find('div', class_='_30jeq3 _16Jk6d')
        final['price'] = price.text
    except :
        final['price'] = ''

    #Description
    try:
        desc = soup.find('span', class_='B_NuCI')
        final['title'] = desc.text
    except :
        final['title'] = ''

    #ratings
    try:
        rate = soup.find('div', class_='_3LWZlK _3uSWvT')
        final['rating'] = rate.text
    except :
        final['rating'] = ''

    #Image URL
    #image_url = soup.find_all('img')
    #image_url = image_url[11]['src']
    #print(image_url)


    #Brand
    try:
        brand = soup.find('span', class_='G6XhRU')
        final['brand'] = brand.text
    except :
        final['brand']=''

    #Brand Store URL
    try:
        brand_url = soup.findAll('div', class_='_3GIHBu')
        brand_url = BASE_URL + brand_url[5].find('a')['href']
        final['brand_url'] = brand_url
    except :
        final['brand_url'] = ''

    #Product Description
    try:
        product_desc = soup.find('div', class_='_1AN87F')
        final['prod_desc'] = product_desc.text
    except :
        final['prod_desc']=''

    #Product Meta data
    try:
        prod_keys = []
        prod_meta_keys = soup.find_all('div', class_='col col-3-12 _2H87wv')
        prod_vals = []
        prod_meta_values = soup.find_all('div', class_='col col-9-12 _2vZqPX')

        for i in prod_meta_keys:
            prod_keys.append(i.text)

        for i in prod_meta_values:
            prod_vals.append(i.text)

        prod_meta = dict(zip(prod_keys, prod_vals))
        final['prod_meta'] = [prod_meta]
    except :
        final['prod_meta'] = ''
    print(final)
    print(type(final))
    channel.basic_publish(exchange='', routing_key='scraper', body=json.dumps(final),properties=pika.BasicProperties(delivery_mode=2))
    print('Pushed to Queue')
channel.close()
    #filename = f'flipkart_results_{today}.json'
    #with open(filename, 'a') as f:
    #    f.write(json.dumps(final))
    #    f.write('\n')
