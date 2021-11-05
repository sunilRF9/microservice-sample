import pika

connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
        )
channel = connection.channel()
channel.queue_declare(queue='scraper')


def callback(ch, method, properties, body):
    print(body)
    print("[x] Received %r" %body)

channel.basic_consume(queue='scraper', on_message_callback=callback, auto_ack=True)
print("[*] waiting for events from scraper")
channel.start_consuming()

