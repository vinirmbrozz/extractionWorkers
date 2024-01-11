import json, time, sys
import pika

# CONFIGURACAO DO EXCHANGE
# EXCHANGE TIPO DIRECT
# Nesta versao, exchange nao mantem conexao com rabbitmq para envio de msg
class QueueExchange:
    def __init__(self, service:str, rabbit_callback: object = None):
        self.service     = service
        self.credentials = pika.PlainCredentials(username='usramqp', password='Qc6ZxJSBrhRVsrY')
        self.callback    = rabbit_callback
        try:
            if rabbit_callback is not None:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host= 'rabbitmq', credentials= self.credentials ))
                self.channelIn  = self.connection.channel() # start main channel
                self.channelIn.queue_declare(queue= self.service)
                self.channelIn.basic_consume(queue= self.service, 
                                            auto_ack= True,
                                            on_message_callback= self.callback)
        except Exception as ex:
            raise ValueError(f"TROUBLE CNX RABBITMQ: {ex}")

    def sendMsg(self, message: dict, rkey: str = 'queue_controller'):
        trials      = 5
        waitTime    = 60
        messageSent = False
        print(f"MESSAGE INTERNAL RABBITMQ: {rkey}")
        while not messageSent and trials:
            try:
                print(f"TRIALS {trials}")
                messagejs = json.dumps(message)
                connection = pika.BlockingConnection(pika.ConnectionParameters(host= 'rabbitmq', credentials= self.credentials ))
                channelOut = connection.channel() # start a channel
                channelOut.queue_declare(queue = rkey)
                channelOut.basic_publish(
                    exchange   = '', 
                    routing_key= rkey, 
                    body       = messagejs, 
                    properties = pika.BasicProperties(
                                    content_type = 'text/plain',
                                    delivery_mode= pika.DeliveryMode.Transient))
                messageSent = True
                connection.close()
            except Exception as ex:
                print(f"TROUBLE CNX RABBITMQ REMAIN ATTEMPT: {trials}")
                print(ex)
                sys.stdout.flush()
                time.sleep(waitTime)
                trials -= 1

    def start_consuming(self):
        if self.callback is not None:
            self.channelIn.start_consuming()
