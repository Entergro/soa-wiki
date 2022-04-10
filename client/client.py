import os
import pickle
import uuid

import pika


class WikiPathClient(object):

    def __init__(self):
        host = os.environ.get('AMQP_HOST', 'host.docker.internal')

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, data):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=pickle.dumps(data))
        while self.response is None:
            self.connection.process_data_events()
        return pickle.loads(self.response)


wikipath_rpc = WikiPathClient()

print("Введите название статьи, например\n"
      "article_from: Высшая_школа_экономики'\n"
      "article_to: Минск")


print("article_from: ", end='')
url_from = input().split('/')[-1]

print("article_to: ", end='')
url_to = input().split('/')[-1]

print(url_from, url_to)
response = wikipath_rpc.call({'url_from': url_from, 'url_to': url_to})
print(response)
