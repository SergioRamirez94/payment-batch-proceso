from database.models.wallets import Wallet
from database.models.currency import Currency
from database.models.accounts import Tenant
import pandas as pd
import uuid
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import logging
from decimal import Decimal
from typing import List
import os
from exceptions.insufficient_funds_error import InsufficientFundsError
import numpy as np

logging.basicConfig(level=logging.INFO)

GROUP_SIZE = 300

ID_ACCOUNT_BEU = os.getenv("ID_ACCOUNT_BEU")
ID_ACCOUNT_TIKIN = os.getenv("ID_ACCOUNT_TIKIN")

DICT_ACCOUNT = {
    "beu": ID_ACCOUNT_BEU,
    "tikin": ID_ACCOUNT_TIKIN,
}


def get_wallets_by_ids(db_session:Session, wallets_id) ->List[Wallet]:
    wallets =  db_session.query(Wallet).filter(Wallet.wallet_id.in_(wallets_id)).all()
    return wallets


def get_wallet_by_account_id(db_session:Session, account_id, currency:Currency) ->Wallet:
    return db_session.query(Wallet).filter(Wallet.account_account_id==account_id, Wallet.currency_currency_id == currency.currency_id).first()


def create_wallets(group: pd.DataFrame, currency:Currency):
    list_wallets = []
    for _, row in group.iterrows():
        list_wallets.append(
            Wallet(
                wallet_id = row['wallet_id'],
                wallet_name = currency.iso_code,
                currency_currency_id = currency.currency_id,
                balance = 0.0,
                account_account_id = row['account_id']
            )
        )
    return list_wallets


def create_wallets_batch(db_session:Session, df: pd.DataFrame, currency:Currency):

    df_wallet_to_create = df[(df['account_id'].notna())&(df['wallet_id'].isna())]
    df_wallet_to_create['wallet_id'] = df_wallet_to_create['wallet_id'].apply(lambda x: uuid.uuid4())
    groups = [df.iloc[i:i + GROUP_SIZE] for i in range(0, len(df), GROUP_SIZE)]

    for group in groups:
        try:
            
            list_wallets =  create_wallets(group, currency)
            db_session.add_all(list_wallets)
            db_session.commit()
            df_wallet_to_create.loc[group.index, "create_wallet"] = "SUCCESSFUL"
        except SQLAlchemyError as e:
            db_session.rollback()
            df_wallet_to_create.loc[group.index, "create_wallet"] = "FAILED"
            logging.error(f"Error executing SQL create accounts: {str(e)}")

    df_wallet_to_create = df_wallet_to_create[['identifier', 'account_id', 'wallet_id', 'create_wallet']]
    df = df.merge(df_wallet_to_create, how='left', on='identifier', suffixes=('', '_new'))
    df.loc[df['create_wallet'] == "FAILED", ['wallet_id']] = np.nan
    df['wallet_id'] = df['wallet_id'].fillna(df['wallet_id_new'])
    df = df[['identifier', 'account_id', 'wallet_id', 'amount', 'amount_fee']]
    return df


def get_integration_wallet_or_create(db_session:Session, tenant:Tenant, currency:Currency):
    account_tenant_id = DICT_ACCOUNT[tenant.name]
    return get_wallet_by_account_id(db_session, account_tenant_id, currency)


   
def validate_funds(wallet, amount):
    if wallet.balance < amount:
        raise InsufficientFundsError(wallet.balance, amount)


def add_funds_to_wallet(wallet: Wallet, amount: float) -> Wallet:
    balance = Decimal(wallet.balance) 
    amount = Decimal(amount)  
    new_balance = balance + amount
    wallet.balance = new_balance 
    return wallet

def subtract_funds_from_wallet(wallet: Wallet, amount: Decimal) -> Wallet:
    balance = Decimal(wallet.balance) 
    new_balance = balance - amount
    wallet.balance = new_balance
    return wallet