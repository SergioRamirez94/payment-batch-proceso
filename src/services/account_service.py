from database.models.accounts import Account, Tenant
from database.models.country import Country
from database.models.currency import Currency
import coolname
from sqlalchemy.orm import Session
import random
import pandas as pd
import uuid
from sqlalchemy.exc import SQLAlchemyError
from services.wallet_service import create_wallets
import logging
import numpy as np
from utils.sqs_notification import send_notification_create_account

logging.basicConfig(level=logging.INFO)

GROUP_SIZE=100


def create_accounts(group: pd.DataFrame, currency:Currency, tenant:Tenant):
    list_accounts = []
    for _, row in group.iterrows():
        list_accounts.append(
            Account(
                account_id = row['account_id'],
                user_name = row['user_name'],
                tenant = tenant.value,
                business_url = f'https//{tenant.name}.pro/{row["user_name"]}',
                account_type = 0,
                currency_id = currency.currency_id,
                country_country_id = currency.country[0].country_id
            )
        )
    return list_accounts


def suggest_name(user_name:str, db_session:Session, tenant: Tenant):
    try:
        suggested_name_account = user_name
        counter = 1
        while True and counter < 1000:
            counter += 1
            account = db_session.query(Account).filter(Account.user_name == suggested_name_account, Account.tenant == tenant.value).first()
            if account is None:
                return suggested_name_account
            random_number = random.randint(100, 999)
            suggested_name_account = f"{user_name}{random_number}"
        return None
    except Exception as e:
        logging.error(f"Error suggesting account name: {str(e)}")
        raise

def create_account_name(db_session:Session, tenant: Tenant):
    user_name = f"${coolname.generate_slug(2)}"
    suggested_account_name = suggest_name(user_name, db_session, tenant)
    if suggested_account_name is None:
        raise ValueError("Error suggesting account name.")
    return suggested_account_name


def create_accounts_batch(db_session:Session, df: pd.DataFrame, currency:Currency, tenant:Tenant):

    df_account_to_create = df[df['account_id'].isna()]
    df_account_to_create['account_id'] = df_account_to_create['account_id'].apply(lambda x: uuid.uuid4())
    df_account_to_create['wallet_id'] = df_account_to_create['wallet_id'].apply(lambda x: uuid.uuid4())
    df_account_to_create['user_name'] = df_account_to_create.apply(lambda x: create_account_name(db_session, tenant), axis=1)
    groups = [df_account_to_create.iloc[i:i + GROUP_SIZE] for i in range(0, len(df), GROUP_SIZE)]
    for group in groups:
        try:

            list_accounts = create_accounts(group, currency, tenant)
            list_wallets =  create_wallets(group, currency)
            db_session.add_all(list_accounts)
            db_session.add_all(list_wallets)
            db_session.commit()
            df_account_to_create.loc[group.index, "create_account"] = "SUCCESSFUL"
        except SQLAlchemyError as e:
            db_session.rollback()
            df_account_to_create.loc[group.index, "create_account"] = "FAILED"
            logging.error(f"Error executing SQL create accounts: {str(e)}")
    
    df_account_to_create = df_account_to_create[['identifier', 'account_id', 'wallet_id', 'create_account']]
    df = df.merge(df_account_to_create, how='left', on='identifier', suffixes=('', '_new'))
    df.loc[df['create_account'] == "FAILED", ['account_id', 'wallet_id']] = np.nan
    df['account_id'] = df['account_id'].fillna(df['account_id_new'])
    df['wallet_id'] = df['wallet_id'].fillna(df['wallet_id_new'])
    df = df[['identifier', 'account_id', 'wallet_id', 'amount', 'amount_fee']]

    df_account_created =df_account_to_create[df_account_to_create['create_account'] == "SUCCESSFUL"]
    df_account_created["platform"] = tenant.name
    df_account_created["account_id"] = df_account_created["account_id"].apply(lambda x: str(x))
    df_account_created["identifier"] = df_account_created["identifier"].apply(lambda x: str(x))
    data = df_account_created[['identifier', "platform","account_id" ]].to_dict('records')
    body = {"data": data}
    send_notification_create_account(body)
    
    return df
    