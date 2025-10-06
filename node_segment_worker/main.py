#!/usr/bin/env python
# coding: utf-8

import os

from dotenv import load_dotenv

load_dotenv()


NODE_queue_url = os.getenv("NODE_queue_url")
RMQ_EXCHANGE_NAME = os.getenv("Node_RMQ_EXCHANGE_NAME")
RMQ_QUEUE_NAME = os.getenv("Node_RMQ_QUEUE_NAME")
RMQ_ROUTING_KEY = os.getenv("Node_RMQ_ROUTING_KEY")
region_name = os.getenv("region_name")

# Add the project root to the Python path
# project_root = os.getenv("Project_Root")
# sys.path.append(project_root)

from ant_worker.SQS_Processer import sqs_Message_Consumer


Node_Segment_Service_Context_Default_schema = {
    "type": "object",
    "properties": {
        "nodeSegmentServiceContextId": {"type": "string"},
        "Type": {"type": "string"},
        "nodeSegmentServiceContextName": {"type": "string"},
        "networkType": {"type": "string"},
        "refDaasServiceContextId": {"type": "string"},
    },
    "required": [
        "nodeSegmentServiceContextId",
        "Type",
        "nodeSegmentServiceContextName",
        "networkType",
        "refDaasServiceContextId",
    ],
}


# def task_caller(**kwargs):
def task_caller():

    calling_sqs = sqs_Message_Consumer.SQS_Message_Consumer(
        NODE_queue_url,
        RMQ_EXCHANGE_NAME,
        RMQ_QUEUE_NAME,
        RMQ_ROUTING_KEY,
        Node_Segment_Service_Context_Default_schema,
    )


while True:
    task_caller()
