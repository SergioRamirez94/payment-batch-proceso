import boto3
import logging
import pandas as pd
from typing import Any, Dict, Optional
from immudb.client import ImmudbClient
from io import BytesIO
import os
import uuid
import json
import numpy as np
from database.database import execute_sql, query_executer
import numpy as np

logging.basicConfig(level=logging.INFO)

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
SQS_REQUEST_BATCH_TRANSACTION = os.getenv("SQS_REQUEST_BATCH_TRANSACTION")
SQS_RESPONSE_BATCH_TRANSACTION = os.getenv("SQS_RESPONSE_BATCH_TRANSACTION")

IMMUDB_USER = os.getenv("IMMUDB_USER")
IMMUDB_PASSWORD = os.getenv("IMMUDB_PASSWORD")
IMMUDB_HOST = os.getenv("IMMUDB_HOST")
DATABASE_BEU = os.getenv("DATABASE_BEU")
DATABASE_TIKIN = os.getenv("DATABASE_TIKIN")
TABLE_ACCOUNTS = os.getenv("TABLE_ACCOUNTS")
TABLE_WALLETS = os.getenv("TABLE_WALLETS")
TABLE_TRANSACTIONS = os.getenv("TABLE_TRANSACTIONS")
PENDING_TRANSACTION = os.getenv("PENDING_TRANSACTION")


ID_ACCOUNT_BEU = os.getenv("ID_ACCOUNT_BEU")
ID_ACCOUNT_TIKIN = os.getenv("ID_ACCOUNT_TIKIN")

DICT_ACCOUNT = {
    "beu": ID_ACCOUNT_BEU,
    "tikin": ID_ACCOUNT_TIKIN,
}


s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")

def custom_serializer(obj):
    if isinstance(obj, (np.integer, np.floating)):
        return obj.item()  # Convierte a tipos nativos
    raise TypeError(f"Type {type(obj)} not serializable")

def get_wallet_intregration(
    integration: str, currency: str
) -> Optional[Dict[str, Any]]:
    """
    Retrieves the wallet information for the integration account based on the given currency.

    Args:
        integration (str): The integration type (e.g., "beu" or "tikin").
        currency (str): The currency of the wallet.

    Returns:
        Optional[Dict[str, Any]]: The wallet details or None if not found.
    """
    account_integration_id = DICT_ACCOUNT[integration]
    response = query_executer(
        f"SELECT id FROM {TABLE_WALLETS} WHERE account_id = '{account_integration_id}' AND currency = '{currency}'",integration
    )
    return response[0] if response else None


def download_excel_from_s3(s3_key: str) -> pd.DataFrame:
    try:
        logging.info(f"Downloading file {s3_key} from S3...")
        obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        file_bytes = obj['Body'].read()
        data = pd.read_excel(file_bytes)
        logging.info("File downloaded successfully.")
        return data
    except Exception as e:
        logging.error(f"Error downloading file from S3: {str(e)}")
        raise

def split_dataframe(df, group_size=100):
    groups = [df.iloc[i:i + group_size] for i in range(0, len(df), group_size)]
    return groups

def check_amount_account(integration, account_id, currency):
    query = f"SELECT id, balance FROM {TABLE_WALLETS} WHERE account_id = '{account_id}' AND currency = '{currency}'"
    response = query_executer(query, integration)
    return response[0] if response else None

def block_amount(integration, wallet_id, amount, batch_id):
    query = f"""
        BEGIN TRANSACTION;
        UPDATE wallets SET balance = balance - {amount} WHERE id = '{wallet_id}';
        COMMIT;
    """
    execute_sql(query, integration)

def rollback_failed_transactions(integration, df, wallet_id_from, percentage_fee):
    failed_transactions = df[df['status_transaction'] == "FAILED"]
    if not failed_transactions.empty:
        total_refund = failed_transactions['amount'].sum()
        total_refund = percentage_fee*total_refund
        query = f"UPDATE wallets SET balance = balance + {total_refund} WHERE id = '{wallet_id_from}';"
        try:
            execute_sql(query, integration)
            logging.info(f"Refunded {total_refund} to wallet {wallet_id_from}.")
        except Exception as e:
            logging.error(f"Error refunding funds: {str(e)}")
            raise

def disperse_funds(integration, batch_id, account_id: str, df, currency: str, user_id: str, amount_percentage, user_percentage, process):
    try:
        df_transactions = df[df['account_id'].notna()].copy()
        response = check_amount_account(integration, account_id, currency)
        if response is None:
            raise ValueError("Account or wallet not found.")
        wallet_id_from, balance = response
        if 'status_transaction' in df_transactions.columns:
            df_transactions = df_transactions[df_transactions['status_transaction'] =='FAILED']
        total_amont = df_transactions['amount'].sum()
        if balance < total_amont:
            raise ValueError("Insufficient funds.")
        
        percentage_fee = (user_percentage + amount_percentage)/100
        total_fee = total_amont*percentage_fee
        block_amount(integration, wallet_id_from, total_fee, batch_id)
        
        groups = split_dataframe(df_transactions, group_size=100)
        integration_wallet_id = get_wallet_intregration(integration, currency)
        if integration_wallet_id is None:
            raise ValueError("Integration account no exist.")
        integration_wallet_id =  integration_wallet_id[0]
        for group in groups:
            sql_transaction = "BEGIN TRANSACTION;\n"
            for _, row in group.iterrows():
                wallet_id = row['wallet_id']
                amount = row['amount']
                fee = amount*percentage_fee
                sql_transaction += (
                    f"UPDATE wallets SET balance = balance + {amount} WHERE id = '{wallet_id}';\n"
                    f"UPDATE wallets SET balance = balance + {fee} WHERE id = '{integration_wallet_id}';\n"
                )
                
                sql_transaction += (
                    f"""INSERT INTO {TABLE_TRANSACTIONS} (
                            transaction_id, user_id, transaction_type, transaction_group, 
                            source_wallet_id, destination_wallet_id, currency, amount, 
                            fee_fixed, fee_variable_percent, exchange_rate, 
                            related_transaction_id, status, timestamp_create
                        ) VALUES (
                            '{uuid.uuid4()}', '{user_id}', 'transfer', '{process}', 
                            '{wallet_id_from}', '{wallet_id}', '{currency}', {amount}, 
                            0.0, {percentage_fee*100}, NULL, '{batch_id}', 'completed', NOW()
                        );\n"""
                )
                sql_transaction += (
                    f"""INSERT INTO {TABLE_TRANSACTIONS} (
                            transaction_id, user_id, transaction_type, transaction_group, 
                            source_wallet_id, destination_wallet_id, currency, amount, 
                            fee_fixed, fee_variable_percent, exchange_rate, 
                            related_transaction_id, status, timestamp_create
                        ) VALUES (
                            '{uuid.uuid4()}', '{user_id}', 'fee_transfer', '{process}', 
                            '{wallet_id_from}', '{integration_wallet_id}', '{currency}', {fee}, 
                            0.0, 0.0, NULL, '{batch_id}', 'completed', NOW()
                        );\n"""
                )
            sql_transaction += "COMMIT;"
            try:
                execute_sql(sql_transaction, integration)
                df.loc[group.index, "status_transaction"] = "SUCCESSFUL"
            except Exception as e:
                logging.error(f"Error executing SQL transaction: {str(e)}")
                df.loc[group.index, "status_transaction"] = "FAILED"
        df.loc[df['account_id'].isna(), 'status_transaction'] = "FAILED"
        rollback_failed_transactions(integration, df, wallet_id_from, percentage_fee)
        return df
    except Exception as e:
        logging.error(f"Error dispersing funds: {str(e)}")
        raise

def send_message_to_response_queue(QueueUrl: str, message_body: Dict[str, Any]):
    try:
        sqs_client.send_message(QueueUrl=QueueUrl, MessageBody=json.dumps(message_body, default=custom_serializer))
        logging.info("Message sent to response queue.")
    except Exception as e:
        logging.error(f"Error sending message to response queue: {str(e)}")
        raise

def save_excel_to_s3(df: pd.DataFrame, s3_key: str):
    try:
        excel_buffer = BytesIO()
        df.to_excel(excel_buffer, index=False)
        excel_buffer.seek(0)
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=excel_buffer)
        logging.info(f"File saved to S3 with key: {s3_key}")
    except Exception as e:
        logging.error(f"Error saving file to S3: {str(e)}")
        raise

def create_accounts(df, currency, integration, s3_key):

    df_account_to_create = df[df['account_id'].isna()]
    df_account_to_create['account_id'] = df_account_to_create['account_id'].apply(lambda x: uuid.uuid4())
    df_account_to_create['wallet_id'] = df_account_to_create['wallet_id'].apply(lambda x: uuid.uuid4())
    
    groups = split_dataframe(df_account_to_create, group_size=300)
    for group in groups:
        sql_transaction = "BEGIN TRANSACTION;\n"
        for index, row in group.iterrows():
            account_id = row['account_id']
            wallet_id = row['wallet_id']
            user_name = row['user_name']
            sql_transaction += (f"""
                INSERT INTO {TABLE_ACCOUNTS} (id, user_name, business_url, typeAccount, currency_preference, created_at) 
                VALUES ('{str(account_id)}', '{user_name}', '{user_name}.{integration}.is', 'personal', '{currency}', NOW());
                INSERT INTO {TABLE_WALLETS} (id, wallet_name, account_id, currency, balance, created_at) 
                VALUES ('{wallet_id}', '{currency}', '{str(account_id)}', '{currency}', 0, NOW());\n"""
            )
        sql_transaction += "COMMIT;"
        try:
            execute_sql(sql_transaction, integration)
            df_account_to_create.loc[group.index, "create_account"] = "SUCCESSFUL"
        except Exception as e:
            df_account_to_create.loc[group.index, "create_account"] = "FAILED"
            logging.error(f"Error executing SQL create accounts: {str(e)}")
    
    df_account_to_create = df_account_to_create[['user_name', 'account_id', 'wallet_id', 'create_account']]
    df = df.merge(df_account_to_create, how='left', on='user_name', suffixes=('', '_new'))
    df.loc[df['create_account'] == "FAILED", ['account_id', 'wallet_id']] = np.nan
    df['account_id'] = df['account_id'].fillna(df['account_id_new'])
    df['wallet_id'] = df['wallet_id'].fillna(df['wallet_id_new'])
    df = df[['user_name', 'account_id', 'wallet_id', 'amount']]
    save_excel_to_s3(df, s3_key)
    return 

def process_message(message):
    body = json.loads(message["Body"])
    logging.info(f"Message received: {body}")
    account_id = body.get("account_id")
    s3_key = body.get("file_path")
    currency = body.get("currency")
    user_id = body.get("user_id")
    batch_id = body.get("batch_id")
    amount_percentage = body.get("amount_percentage")
    user_percentage = body.get("user_percentage")
    process = body.get("user_percentage")
    integration = body.get("integration")
    if not account_id or not s3_key:
        logging.error("Invalid message in queue. Skipping...")
        return False
    try:
        df = download_excel_from_s3(s3_key)
        if any(df['account_id'].isna()):
            df = create_accounts(df, currency, integration, s3_key)
        df = disperse_funds(integration, batch_id, account_id, df, currency, user_id, amount_percentage, user_percentage, process)
        output_key = f"results/{batch_id}_results.xlsx"
        save_excel_to_s3(df, output_key)
        has_failures = "FAILED" in df["status_transaction"].values
        total_amount_transfer = df[df["status_transaction"]=='SUCCESSFUL']['amount'].sum()
        total_accounts_transfer = len(df[df["status_transaction"]=='SUCCESSFUL'])
        response_message = {
            "batch_id": batch_id,
            "status": "INCOMPLETE" if has_failures else "COMPLETE",
            "total_amount_transfer":total_amount_transfer,
            "s3_file_path": output_key,
            "total_accounts_transfer":total_accounts_transfer
        }
        send_message_to_response_queue(SQS_RESPONSE_BATCH_TRANSACTION, response_message)
        return True
    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")
        response_message = {
            "batch_id": batch_id,
            "status": "FAILED",
            "error": str(e)
        }
        send_message_to_response_queue(SQS_RESPONSE_BATCH_TRANSACTION, response_message)
        return True

def process_queue():
    try:
        while True:
            response = sqs_client.receive_message(
                QueueUrl=SQS_REQUEST_BATCH_TRANSACTION,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )
            if "Messages" not in response:
                logging.info("No messages in the queue.")
                continue
            for message in response["Messages"]:
                result = process_message(message)
                if result:
                    sqs_client.delete_message(
                        QueueUrl=SQS_REQUEST_BATCH_TRANSACTION,
                        ReceiptHandle=message["ReceiptHandle"]
                    )
                logging.info("Message processed and removed from the queue.")
    except Exception as e:
        logging.error(f"General error while processing the queue: {str(e)}")

if __name__ == "__main__":
    process_queue()
