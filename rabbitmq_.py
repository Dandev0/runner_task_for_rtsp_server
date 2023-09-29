import logging
import pika
import time
from ffmpeg_runner import Ffmpeg_rtsp, Create_process_to_ffmpeg, Terminate_task_for_ffmpeg
import ast
from config import RABBITMQ_LOGIN, RABBITMQ_IP, RABBITMQ_PASSWORD, RABBITMQ_PORT, RABBITMQ_QUEUE_LISTENER, RABBITMQ_QUEUE_SENDER, SERVICE_NAME
from logger_ import LOGGER, extra_name
import time

logger = LOGGER


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
                    return self.connection

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
            command = data['command']
            if command == 'start':

                Create_process_to_ffmpeg(rtsp_url=data['data']['rtsp_url'],
                                         output_url=data['data']['output_url']).create_process_to_ffmpeg()
            elif command == 'stop':
                Terminate_task_for_ffmpeg(output_url=data['data']['output_url']).Kill_process()

        except KeyError:
            return logger.error(msg=f'{SERVICE_NAME}: Отправленные данные не валидны!',
                               extra={extra_name: f'Сервис получил следующие данные: {data}'})

    def get_message(self):
        try:
            self.connect()
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


class Rabbit_sender(Rabbit_base):
    def send_message(self):
        self.connect()
        if self.connection:
            self.channel.basic_publish(exchange='',
                                       routing_key='log_queue', body=self.message)


if __name__ == '__main__':
    Rabbit_listener().get_message()
