import os
import pandas as pd
import boto3
from io import BytesIO
import logging

logging.basicConfig(level=logging.INFO)

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client("s3")

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