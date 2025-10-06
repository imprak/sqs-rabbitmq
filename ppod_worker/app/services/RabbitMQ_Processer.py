from jsonschema import validate
from jsonschema.exceptions import ValidationError
import json
import pika
import os

from dotenv import load_dotenv

load_dotenv()
from ant_worker import logger

# RMQ_HOST = "10.168.171.75"
# RMQ_PORT = "5672" #8083
# RMQ_USER = "admin"
# RMQ_PASSWORD = "admin123"

RMQ_HOST = os.getenv("RMQ_HOST")
RMQ_PORT = os.getenv("RMQ_PORT")
RMQ_USER = os.getenv("RMQ_USER")
RMQ_PASSWORD = os.getenv("RMQ_PASSWORD")
RMQ_URL = f"amqp://{RMQ_USER}:{RMQ_PASSWORD}@{RMQ_HOST}:{RMQ_PORT}//"
RMQ_Vhost = "/"
RMQ_EXCHANGE_TYPE = "direct"


# PPOD_Service_Context_Default_schema = os.getenv("PPOD_Service_Context_Default_schema")

# RMQ_EXCHANGE_NAME = "ril-PPOD-Service-Context-Exchange"
# RMQ_QUEUE_NAME = "ril-PPOD-Service-Context"
# RMQ_ROUTING_KEY = "ril-PPOD-Service-Context-routing"
# PPOD_Service_Context_Default_schema = {
#    "type": "object",
#    "properties": {"ppodServiceContextId": {"type": "string"}, "ppodServiceContextName": {"type": "string"}},
#    "required": ["ppodServiceContextId", "ppodServiceContextName"],
# }


class RabbitMQMessageProcesser:
    def __init__(
        self,
        RMQ_EXCHANGE_NAME,
        RMQ_QUEUE_NAME,
        RMQ_ROUTING_KEY,
        Default_schema,
        Processed_Data,
    ):
        if not isinstance(Default_schema, dict):
            raise TypeError(
                f"Default schema must be dict, got {type(Default_schema).__name__} instead."
            )

        credentials = pika.PlainCredentials(RMQ_USER, RMQ_PASSWORD)
        parameters = pika.ConnectionParameters(
            host=RMQ_HOST,
            port=RMQ_PORT,
            virtual_host=RMQ_Vhost,
            credentials=credentials,
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.default_schema = Default_schema
        self.RMQ_EXCHANGE_NAME = RMQ_EXCHANGE_NAME
        self.RMQ_QUEUE_NAME = RMQ_QUEUE_NAME
        self.RMQ_ROUTING_KEY = RMQ_ROUTING_KEY
        self.Processed_Data = Processed_Data

    def publish_message(self):
        try:
            self.channel.exchange_declare(
                exchange=self.RMQ_EXCHANGE_NAME, exchange_type=RMQ_EXCHANGE_TYPE
            )
            self.channel.queue_declare(queue=self.RMQ_QUEUE_NAME, durable=True)
            self.channel.queue_bind(
                exchange=self.RMQ_EXCHANGE_NAME,
                queue=self.RMQ_QUEUE_NAME,
                routing_key=self.RMQ_ROUTING_KEY,
            )

            for msg in self.Processed_Data:
                message = json.dumps(msg).encode("utf-8")
                self.channel.basic_publish(
                    exchange=self.RMQ_EXCHANGE_NAME,
                    routing_key=self.RMQ_ROUTING_KEY,
                    body=message,
                )
            logger.info(f"message published to queue {self.RMQ_QUEUE_NAME}")
        except Exception as err:
            logger.error(
                f"Failed to publish message to {self.RMQ_QUEUE_NAME} due to error: {err}"
            )


# processed_data = []
# print(processed_data)
# rabbitMQ_message_processer = RabbitMQMessageProcesser(RMQ_EXCHANGE_NAME, RMQ_QUEUE_NAME, RMQ_ROUTING_KEY, PPOD_Service_Context_Default_schema, processed_data)
# rabbitMQ_message_processer.publish_message()
