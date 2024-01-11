import pika, sys
import json


def callback(ch, method, properties, body):
    dados = json.loads(body)
    print(f"\n-------------------------------------\n {method.routing_key}")
    print(f"{dados['caller']}")
    print(f"{dados['msg']}")
    
# SG_ETL 1
#url        = 'amqps://tahccxql:AprsNPNbTfmE_sdwvAfiFTt3zrII5TbC@jackal.rmq.cloudamqp.com/tahccxql'
# SG_ETL 2
#url        = 'amqps://jmjolxse:K8oN6w37ukxl-_W8Nm3W1WteZpQ1KdpB@jackal.rmq.cloudamqp.com/jmjolxse'  
# Sistema SG_ETL - Tough Tiger - 10M msg/month, 100 conn, 1500 queues w 100k msgs
url        = 'amqps://mylvcdwm:H9VEH5jbwfkbNCoZuWt3NPesIPwegn9o@jackal.rmq.cloudamqp.com/mylvcdwm'

params     = pika.URLParameters(url)

connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.exchange_declare(exchange     = 'topic_softgo', 
                        exchange_type = 'topic',
                        auto_delete   = True)

result = channel.queue_declare('geraLogs', exclusive=False)
queue_name = result.method.queue

binding_keys = sys.argv[1:]
if not binding_keys:
    channel.queue_bind(exchange='topic_softgo', queue=queue_name, routing_key='#.sg_intel_vendas.#')

else:
    for binding_key in binding_keys:
        channel.queue_bind(
            exchange='topic_softgo', queue=queue_name, routing_key=binding_key)

print(' [*] Waiting for logs. To exit press CTRL+C')

channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
