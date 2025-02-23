import boto3
import logging
import os
import json
from utils.bucket_management import download_excel_from_s3
from services.account_service import create_accounts_batch
from services.wallet_service import (
    create_wallets_batch,
    get_wallet_by_account_id,
)
from services.currency_service import get_currency_by_iso_code_or_throw
from services.transaction_service import TransactionMaker
from database.models.accounts import Tenant
from database.database import SesionLocalWallet
from utils.bucket_management import save_excel_to_s3
from exceptions.insufficient_funds_error import InsufficientFundsError
from utils.sqs_notification import send_notification_finish_transactions

sqs_client = boto3.client("sqs")


SQS_REQUEST_BATCH_TRANSACTION = os.getenv("SQS_REQUEST_BATCH_TRANSACTION")


class Message:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(self.__dict__)


def get_message_data(message) -> Message:
    body = json.loads(message["Body"])
    keys = [
        "account_id",
        "file_path",
        "currency",
        "user_id",
        "batch_id",
        "amount_percentage",
        "user_percentage",
        "process",
        "integration",
        "concept",
    ]
    message_data = {key: body[key] for key in keys}
    return Message(**message_data)


def process_message(message):
    db_session = SesionLocalWallet()
    message_data = get_message_data(message)
    tenant = Tenant[message_data.integration]
    currency = get_currency_by_iso_code_or_throw(db_session, message_data.currency)
    wallet = get_wallet_by_account_id(db_session, message_data.account_id, currency)
    try:
        df = download_excel_from_s3(message_data.file_path)
        if any(df["account_id"].isna()):
            df = create_accounts_batch(db_session, df, currency, tenant)
            save_excel_to_s3(df, message_data.file_path)
        if any(df["wallet_id"].isna()):
            df = create_wallets_batch(db_session, df, currency)
            save_excel_to_s3(df, message_data.file_path)
        transaction_maker = TransactionMaker(
            db_session,
            df,
            message_data, 
            wallet,
            currency,
            tenant
        )
        transaction_maker.excute()
        transaction_maker.rollback_money()
        save_excel_to_s3(transaction_maker.df, f"results/{message_data.batch_id}_results.xlsx")
        has_failures = "FAILED" in df["status_transaction"].values
        total_amount_transfer = df[df["status_transaction"]=='SUCCESSFUL']['amount'].sum()
        total_accounts_transfer = len(df[df["status_transaction"]=='SUCCESSFUL'])
        response_message = {
            "batch_id": message_data.batch_id,
            "status": "INCOMPLETE" if has_failures else "COMPLETE",
            "total_amount_transfer":total_amount_transfer,
            "s3_file_path": f"results/{message_data.batch_id}_results.xlsx",
            "total_accounts_transfer":total_accounts_transfer
        }
        send_notification_finish_transactions(response_message)
        return True
    except Exception as e:

        return True


def process_queue():
    try:
        while True:
            response = sqs_client.receive_message(
                QueueUrl=SQS_REQUEST_BATCH_TRANSACTION,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=60,
            )
            if "Messages" not in response:
                continue
            for message in response["Messages"]:
                result = process_message(message)
                if result:
                    sqs_client.delete_message(
                        QueueUrl=SQS_REQUEST_BATCH_TRANSACTION,
                        ReceiptHandle=message["ReceiptHandle"],
                    )
                logging.info("Message processed and removed from the queue.")
    except Exception as e:
       logging.error(f"General error while processing the queue: {str(e)}")


if __name__ == "__main__":
    process_queue()
