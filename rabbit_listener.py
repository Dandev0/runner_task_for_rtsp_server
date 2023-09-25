import logging
import pika
import time
from ffmpeg_runner import Ffmpeg_rtsp
import ast
from config import RABBITMQ_LOGIN, RABBITMQ_IP, RABBITMQ_PASSWORD, RABBITMQ_PORT


class Rabbit_base:
    def __init__(self, message: str = None):
        self.credentials = pika.PlainCredentials(username=RABBITMQ_LOGIN, password=RABBITMQ_PASSWORD)
        self.parameters = pika.ConnectionParameters(host=RABBITMQ_IP, port=RABBITMQ_PORT, virtual_host='/',
                                                    credentials=self.credentials)
        self.connection = None
        self.channel = None
        self.message = message

    def connect(self):
        try:
            if not self.connection or self.connection.is_closed:
                self.connection = pika.BlockingConnection(self.parameters)
                self.channel = self.connection.channel()
                if self.connection:
                    logging.warning('Connection to RabbitMQ is UP!')
                self.get_message()

        except pika.exceptions.AMQPConnectionError:
            logging.warning('Error:\npika.exceptions.AMQPConnectionError\nReconnect to RabbitMQ')
            time.sleep(3)
            self.connect()

        except pika.exceptions.ConnectionClosedByBroker:
            logging.warning('Error:\npika.exceptions.ConnectionClosedByBroker\nReconnect to RabbitMQ')
            time.sleep(3)
            self.connect()

        except pika.exceptions.ConnectionWrongStateError as error:
            logging.warning('Error:\npika.exceptions.ConnectionWrongStateError\nReconnect to RabbitMQ')
            time.sleep(3)
            self.connect()


class Rabbit_listener(Rabbit_base):
    @staticmethod
    def pr(ch, method, properties, data):
        try:
            str_data = data.decode('utf-8')
            data = ast.literal_eval(str_data)
            Ffmpeg_rtsp(rtsp_url=data['rtsp_url'], output_url=data['output_url']).add_rtsp_camera_to_rtsp_server()
        except KeyError:
            Ffmpeg_rtsp(rtsp_url=data['rtsp_url']).add_rtsp_camera_to_rtsp_server()

    def get_message(self):
        try:
            self.channel.queue_declare(queue='/dev-queue')
            self.channel.basic_consume(queue='dev-queue',
                                       auto_ack=True,
                                       on_message_callback=self.pr)
            self.channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            logging.warning('Error:\npika.exceptions.AMQPConnectionError\nReconnect to RabbitMQ')
            time.sleep(3)
            self.connect()

        except pika.exceptions.ConnectionClosedByBroker:
            logging.warning('Error:\npika.exceptions.ConnectionClosedByBroker\nReconnect to RabbitMQ')
            time.sleep(3)
            self.connect()

        except pika.exceptions.ConnectionWrongStateError as error:
            logging.warning('Error:\npika.exceptions.ConnectionWrongStateError\nReconnect to RabbitMQ')
            time.sleep(3)
            self.connect()


if __name__ == '__main__':
    Rabbit_listener().connect()
