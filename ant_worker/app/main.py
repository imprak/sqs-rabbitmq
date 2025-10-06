#!/usr/bin/env python
# coding: utf-8


from datetime import datetime
import time, os, sys, json
from apscheduler.schedulers.background import BackgroundScheduler
import logging

from dotenv import load_dotenv

load_dotenv()

ANT_queue_url = os.getenv("ANT_queue_url")
RMQ_EXCHANGE_NAME = os.getenv("ANT_RMQ_EXCHANGE_NAME")
RMQ_QUEUE_NAME = os.getenv("ANT_RMQ_QUEUE_NAME")
RMQ_ROUTING_KEY = os.getenv("ANT_RMQ_ROUTING_KEY")
region_name = os.getenv("region_name")

# Add the project root to the Python path
# project_root = os.getenv("Project_Root")
# sys.path.append(project_root)

# from Spark_Initiation import spark

from app.SQS_Processer import sqs_Message_Consumer


ANT_Service_Context_Default_schema = {
    "type": "object",
    "properties": {
        "networkType": {"type": "string"},
        "antServiceContextName": {"type": "string"},
        "CPEType": {"type": "string"},
        "antModel": {"type": "string"},
        "platformType": {"type": "string"},
        "antVendor": {"type": "string"},
        "refNodeSegmentName": {"type": "string"},
        "antServiceContextId": {"type": "string"},
    },
    "required": [
        "networkType",
        "antServiceContextName",
        "CPEType",
        "antModel",
        "platformType",
        "antVendor",
        "refNodeSegmentName",
        "antServiceContextId",
    ],
}


def task_caller():
    calling_sqs = sqs_Message_Consumer.SQS_Message_Consumer(
        ANT_queue_url,
        RMQ_EXCHANGE_NAME,
        RMQ_QUEUE_NAME,
        RMQ_ROUTING_KEY,
        ANT_Service_Context_Default_schema,
    )


while True:
    task_caller()


# scheduler = BackgroundScheduler()

# scheduler.add_job(task_caller, 'interval', seconds=30)
# scheduler.start()

# Your program can continue running here
# import time
# while True:
# time.sleep(1)
