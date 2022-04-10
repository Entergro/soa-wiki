import os
import pickle

import pika
import requests

session = requests.Session()
url = "https://ru.wikipedia.org/w/api.php"
max_steps = 300

file3 = open('../data.txt', 'w')


def get_links(url_from, url_to):
    prev = dict()

    cur_node = [url_from]
    next_node = set()

    st = 0
    while cur_node and st < max_steps:
        st += 1

        for node in cur_node:
            params = {
                "action": "query",
                "format": "json",
                "titles": node,
                "prop": "links",
                "pllimit": "max"
            }
            response = session.get(url=url, params=params)
            data = response.json()

            pages = data["query"]["pages"]

            for k, v in pages.items():
                if "links" not in v:
                    continue
                for l in v["links"]:
                    title = l["title"]
                    if title not in prev:
                        # file3.write(f'{title}\n')
                        next_node.add(title)
                        prev[title] = node

            if url_to in prev:
                break

            while "continue" in data:
                plcontinue = data["continue"]["plcontinue"]
                params["plcontinue"] = plcontinue

                response = session.get(url=url, params=params, verify=False)
                data = response.json()
                pages = data["query"]["pages"]

                for key, val in pages.items():
                    for link in val["links"]:
                        title = link["title"]
                        if title not in prev:
                            # file3.write(f'{title}\n')
                            next_node.add(title)
                            prev[title] = node
                if url_to in prev:
                    break

        if url_to in prev:
            break
        cur_node = list(next_node)
        next_node = set()

    path = ''
    deep = 1
    cur = url_to
    if url_to in prev:
        while url_from != cur:
            deep += 1
            path = f'{cur} - {path}'
            cur = prev[cur]
        path = f"{url_from} - {path[:-3]}"
    return path, deep


host = os.environ.get('AMQP_HOST', 'host.docker.internal')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=host, port=5672))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')


def on_request(ch, method, props, body):
    data = pickle.loads(body)

    print(f" [.] {data}")
    res = get_links(data['url_from'], data['url_to'])

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id= \
                                                         props.correlation_id),
                     body=pickle.dumps(res))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()
