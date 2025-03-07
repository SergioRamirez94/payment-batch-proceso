import os
import boto3
import numpy as np
import json
import threading
import logging

logging.basicConfig(level=logging.INFO)

SQS_RESQUEST_RELATION_ACCOUNTS =  os.getenv("SQS_RESQUEST_RELATION_ACCOUNTS")
SQS_RESPONSE_BATCH_TRANSACTION= os.getenv("SQS_RESPONSE_BATCH_TRANSACTION")
sqs_client = boto3.client("sqs")


def custom_serializer(obj):
    if isinstance(obj, (np.integer, np.floating)):
        return obj.item()  # Convierte a tipos nativos
    raise TypeError(f"Type {type(obj)} not serializable")


def send_message_to_response_queue(QueueUrl: str, message_body):
    try:
        sqs_client.send_message(QueueUrl=QueueUrl, MessageBody=json.dumps(message_body, default=custom_serializer))
        logging.info("Message sent to response queue.")
    except Exception as e:
        logging.error(f"Error sending message to response queue: {str(e)}")
        raise


def send_notification_create_account(body):
    try:
        if SQS_RESQUEST_RELATION_ACCOUNTS:
            t = threading.Thread(target=send_message_to_response_queue, args=(SQS_RESQUEST_RELATION_ACCOUNTS, body))
            t.start()
        else:
            logging.error("Queue URL not set. Check environment variables.")
    except Exception as e:
        logging.error(f"Error sending message to SQS: {str(e)}")


def send_notification_finish_transactions(body):
    try:
        if SQS_RESPONSE_BATCH_TRANSACTION:
            t = threading.Thread(target=send_message_to_response_queue, args=(SQS_RESPONSE_BATCH_TRANSACTION, body))
            t.start()
        else:
            logging.error("Queue URL not set. Check environment variables.")
    except Exception as e:
        logging.error(f"Error sending message to SQS: {str(e)}")