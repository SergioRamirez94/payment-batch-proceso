import boto3
import logging
import os
import json
from utils.bucket_management import download_excel_from_s3
from services.account_service import create_accounts_batch
from services.wallet_service import create_wallets_batch, get_wallet_by_account_id
from services.currency_service import get_currency_by_iso_code_or_throw
from services.transaction_service import create_transaction_batch
from database.models.accounts import Tenant
from database.models.transactions import TransactionGroup, TransactionType
from database.database import SesionLocalWallet
from decimal import Decimal
from utils.bucket_management import save_excel_to_s3

sqs_client = boto3.client("sqs")


SQS_REQUEST_BATCH_TRANSACTION = os.getenv("SQS_REQUEST_BATCH_TRANSACTION")

class Message:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs) 

    def __str__(self):
        return str(self.__dict__)

def get_message_data(message) ->Message:
    body = json.loads(message["Body"])
    keys = [
        "account_id", "file_path", "currency", "user_id", 
        "batch_id", "amount_percentage", "user_percentage", 
        "process", "integration", "concept"
    ]
    message_data = {key: body.get(key) for key in keys}
    return Message(**message_data)



def process_message(message):
    db_session = SesionLocalWallet()
    message_data = get_message_data(message)
    currency = get_currency_by_iso_code_or_throw(db_session, message_data.currency)
    wallet = get_wallet_by_account_id(db_session, message_data.account_id, currency)
    tenant = Tenant[message_data.integration]
    transaction_type = TransactionType.transfer
    transaction_group =  TransactionGroup.bonus if message_data.process == "bonuses" else TransactionGroup.treasury
    concept = message_data.concept if message_data.process == "bonuses" else ""
    fee_percent = Decimal(str(message_data.amount_percentage)) + Decimal(str(message_data.user_percentage))
    try:
        df = download_excel_from_s3(message_data.file_path)
        if any(df['account_id'].isna()):
            df = create_accounts_batch(db_session, df, currency, tenant)
            save_excel_to_s3(df, message_data.file_path)
        if any(df['wallet_id'].isna()):
            df = create_wallets_batch(db_session, df, currency)
            save_excel_to_s3(df, message_data.file_path)

        df = create_transaction_batch(message_data.batch_id, db_session, df, message_data.user_id, wallet, currency, concept, fee_percent, transaction_type, transaction_group)
        save_excel_to_s3(df, f"results/{message_data.batch_id}_results.xlsx")
        #TODO rollback amount
        return True
    except Exception as e:

        return True



def process_queue():
    #try:
    while True:
        response = sqs_client.receive_message(
            QueueUrl=SQS_REQUEST_BATCH_TRANSACTION,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1
        )
        if "Messages" not in response:
            continue
        for message in response["Messages"]:
            result = process_message(message)
            if result:
                sqs_client.delete_message(
                    QueueUrl=SQS_REQUEST_BATCH_TRANSACTION,
                    ReceiptHandle=message["ReceiptHandle"]
                )
            logging.info("Message processed and removed from the queue.")
    #except Exception as e:
     #   logging.error(f"General error while processing the queue: {str(e)}")

if __name__ == "__main__":
    process_queue()
