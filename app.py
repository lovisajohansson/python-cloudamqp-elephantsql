import pika, os
import urllib.parse as up
import psycopg2
from datetime import datetime
import json
 
# Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
channel = connection.channel() # start a channel
channel.queue_declare(queue='weather') # Declare a queue

now = datetime.now()
temperature = 38
data = {"time": str(now), "temperature": temperature}

channel.basic_publish(exchange='',
                  routing_key='weather',
                  body=json.dumps(data))

print("[X] Published: " + str(data))

def store(ch, method, properties, body):
  body = json.loads(body)
  print("[X] Received time:" + str(body["time"]) + " and temperature: " + str(body["temperature"])) 
  print(body["time"])
  try:
    up.uses_netloc.append("postgres")
    url = up.urlparse(os.environ["ELEPHANTSQL_URL"])
    connection = psycopg2.connect(database=url.path[1:],
      user=url.username,
      password=url.password,
      host=url.hostname,
      port=url.port
    )
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS weather (id SERIAL PRIMARY KEY, time TIMESTAMP, temperature integer);")

    postgres_insert_query = """ INSERT INTO weather (TIME, TEMPERATURE) VALUES (%s,%s)"""
    record_to_insert = ((body["time"]), int(body["temperature"]))
    cursor.execute(postgres_insert_query, record_to_insert)
    connection.commit()
    count = cursor.rowcount
    print (count, "Record inserted successfully into weather table")

  except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
  finally:
    #closing database connection.
    if(connection):
      connection.close()
      print("PostgreSQL connection is closed")

channel.basic_consume('weather',
                      store,
                      auto_ack=True)

print(' [*] Waiting for messages:')
channel.start_consuming()
connection.close()


