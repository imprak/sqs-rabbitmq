#!/usr/bin/env python
# coding: utf-8

import time, os, json
import boto3
from botocore.config import Config
import concurrent.futures

from dotenv import load_dotenv

load_dotenv()

from ant_worker.services import RabbitMQMessageProcesser
from ant_worker.logging_mechanism import logger

aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
aws_session_token = os.getenv("aws_session_token")
region_name = os.getenv("region_name")


class SQSMessageConsumer:

    @staticmethod
    def boto3_conn_Handler(region_name):
        sqs_client = boto3.client(
            "sqs",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
            config=Config(retries={"max_attempts": 10}),
        )
        return sqs_client

    @staticmethod
    def consume_sqs_messages_loop(
        queue_url,
        region_name="ca-central-1",
        max_messages=10,
        poll_duration=60,
        max_messages_to_consume=None,
    ):
        # Ideally load credentials from environment or IAM role, NOT hardcoded here!
        sqs_client = SQSMessageConsumer.boto3_conn_Handler(region_name)

        start_time = time.time()
        all_msgs = []

        while (time.time() - start_time) < poll_duration:
            # if you reached max messages, WIll break it.
            if (
                max_messages_to_consume is not None
                and len(all_msgs) >= max_messages_to_consume
            ):
                break

            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,  # min(max_messages, max_messages_to_consume - len(all_msgs) if max_messages_to_consume else max_messages)
                WaitTimeSeconds=10,  # long polling
                VisibilityTimeout=60,  # seconds
            )

            messages = response.get("Messages", [])
            if not messages:
                continue

            for msg in messages:
                # print(f"Consumed message: {msg['Body']}")
                all_msgs.append(msg["Body"])

                # Delete message after processing to avoid duplicates
                # sqs_client.delete_message(
                #    QueueUrl=queue_url,
                #    ReceiptHandle=msg['ReceiptHandle']
                # )

                if (
                    max_messages_to_consume is not None
                    and len(all_msgs) >= max_messages_to_consume
                ):
                    break

        return all_msgs

    @staticmethod
    def get_approximate_number_of_messages(queue_url, region_name="ca-central-1"):
        sqs_client = SQSMessageConsumer.boto3_conn_Handler(region_name)

        response = sqs_client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )

        return int(response["Attributes"].get("ApproximateNumberOfMessages", 0))

    # queue_url = "https://sqs.ca-central-1.amazonaws.com/385980464129/ppodanddaas.fifo"
    # data = get_approximate_number_of_messages(queue_url, "ca-central-1")
    # print(data)

    @staticmethod
    def SQS_Message_Consumer(
        queue_url, RMQ_EXCHANGE_NAME, RMQ_QUEUE_NAME, RMQ_ROUTING_KEY, Default_schema
    ):
        try:
            start_time = time.time()

            # region_name = "ca-central-1"

            data = SQSMessageConsumer.get_approximate_number_of_messages(
                queue_url, region_name
            )
            target_total = min(data, 5000)

            num_consumers = 10  # adjust based on cluster size
            target_per_consumer = target_total // num_consumers

            def safe_consume(index):
                return SQSMessageConsumer.consume_sqs_messages_loop(
                    queue_url,
                    region_name,
                    max_messages=10,
                    poll_duration=60,
                    max_messages_to_consume=target_per_consumer,
                )

            # 🔄 Run consumers in parallel (on driver)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=num_consumers
            ) as executor:
                results = list(executor.map(safe_consume, range(num_consumers)))

            # 🔃 Flatten list of lists
            all_messages = [json.loads(item) for sublist in results for item in sublist]

            # all_messages = [
            #    json.loads(msg['Message']) for msg in all_messages if msg.get("Message") is not None
            # ]
            all_messages = [
                json.loads(msg["Message"]) if msg.get("Message") is not None else msg
                for msg in all_messages
            ]

            # print(all_messages_dicts)
            # print(len(all_messages_dicts))

            rabbitMQ_message_processer = RabbitMQMessageProcesser(
                RMQ_EXCHANGE_NAME,
                RMQ_QUEUE_NAME,
                RMQ_ROUTING_KEY,
                Default_schema,
                all_messages,
            )
            rabbitMQ_message_processer.publish_message()

            end_time = time.time()

            logger.info(f"Consumed  messages in {end_time - start_time:.2f} seconds")

        except Exception as ex:
            logger.error(f"Error: {ex}")


sqs_Message_Consumer = SQSMessageConsumer()


"""
if __name__ == "__main__":
    PPOD_queue_url = os.getenv("PPOD_queue_url")
    RMQ_EXCHANGE_NAME = os.getenv("PPOD_RMQ_EXCHANGE_NAME")
    RMQ_QUEUE_NAME = os.getenv("PPOD_RMQ_QUEUE_NAME")
    RMQ_ROUTING_KEY = os.getenv("PPOD_RMQ_ROUTING_KEY")
    
    calling_sqs = sqs_Message_Consumer.SQS_Message_Consumer(
        PPOD_queue_url, 
        RMQ_EXCHANGE_NAME,
        RMQ_QUEUE_NAME,
        RMQ_ROUTING_KEY,
        PPOD_Service_Context_Default_schema
    )
"""
