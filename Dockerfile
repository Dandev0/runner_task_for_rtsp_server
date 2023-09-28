FROM python:3.10
COPY requirements.txt /app/
WORKDIR /app
RUN apt-get update
RUN apt-get install -y ffmpeg
RUN pip install -r requirements.txt
ENV RABBITMQ_LOGIN=test
ENV RABBITMQ_PASSWORD=test
ENV RABBITMQ_IP=46.146.229.116
ENV RABBITMQ_PORT=5672
ENV RABBITMQ_QUEUE_LISTENER = 'dev-queue'
ENV RABBITMQ_QUEUE_SENDER = 'log_queue'
COPY . /app
WORKDIR /app
CMD ["python3", "rabbitmq_.py"]
