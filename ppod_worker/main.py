#!/usr/bin/env python
# coding: utf-8

import os

from dotenv import load_dotenv

load_dotenv()


PPOD_queue_url = os.getenv("PPOD_queue_url")
RMQ_EXCHANGE_NAME = os.getenv("PPOD_RMQ_EXCHANGE_NAME")
RMQ_QUEUE_NAME = os.getenv("PPOD_RMQ_QUEUE_NAME")
RMQ_ROUTING_KEY = os.getenv("PPOD_RMQ_ROUTING_KEY")
region_name = os.getenv("region_name")

# Add the project root to the Python path
# project_root = os.getenv("Project_Root")
# sys.path.append(project_root)

# from Spark_Initiation import spark

from ant_worker.SQS_Processer import sqs_Message_Consumer


PPOD_Service_Context_Default_schema = {
    "type": "object",
    "properties": {
        "ppodServiceContextId": {"type": "string"},
        "ppodServiceContextName": {"type": "string"},
    },
    "required": ["ppodServiceContextId", "ppodServiceContextName"],
}


def task_caller():

    calling_sqs = sqs_Message_Consumer.SQS_Message_Consumer(
        PPOD_queue_url,
        RMQ_EXCHANGE_NAME,
        RMQ_QUEUE_NAME,
        RMQ_ROUTING_KEY,
        PPOD_Service_Context_Default_schema,
    )


while True:
    task_caller()
