from database.models.transactions import Transaction, TransactionGroup, TransactionType
from database.models.wallets import Wallet
from services.wallet_service import validate_funds
from services.wallet_service import get_wallets_by_ids, add_funds_to_wallet, subtract_funds_from_wallet
from sqlalchemy.orm import Session
from database.models.currency import Currency
from decimal import Decimal
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import logging

logging.basicConfig(level=logging.INFO)

GROUP_SIZE = 300


def get_amount_to_transfer(df, wallet_id):
    return Decimal(str(round(df[df["wallet_id"] == str(wallet_id)]["amount"].values[0],2)))


def create_transaction(
    batch_id:str,
    source_wallet: Wallet,
    group: pd.DataFrame,
    currency: Currency,
    concept: str,
    fee_percent: Decimal,
    transaction_type: TransactionType,
    transaction_group: TransactionGroup,
    user_id,
):
    transactions = []
    for _, row in group.iterrows():
        decimal_amount = Decimal(str(row["amount"])) 
        total_transaction_fee = round(
            decimal_amount * Decimal(fee_percent)/Decimal("100"), 2
        )
        transactions.append(
            Transaction(
                user_id=user_id,
                amount=decimal_amount ,
                concept=concept,
                destination_wallet_id=row["wallet_id"],
                source_wallet_id=source_wallet.wallet_id,
                currency_currency_id=currency.currency_id,
                transaction_type=transaction_type.value,
                transaction_group=transaction_group.value,
                variable_fee_percentage=fee_percent,
                total_transaction_fee=total_transaction_fee,
                total_amount_deducted=total_transaction_fee + decimal_amount,
                status ='completed',
                relate_transaction_id = batch_id
            )
        )
    return transactions

def create_transaction_batch(
    batch_id:str, 
    db_session: Session,
    df: pd.DataFrame,
    user_id, 
    source_wallet:Wallet, 
    currency: Currency,
    concept: str,
    fee_percent: Decimal, 
    transaction_type: TransactionType,
    transaction_group: TransactionGroup,
):

    df_transactions = df[(df["wallet_id"].notna())].copy()
    amount_to_transfer = df_transactions["amount_fee"].apply(lambda x: Decimal(str(round(x,2)))).sum()
    validate_funds(source_wallet, amount_to_transfer)
    subtract_funds_from_wallet(source_wallet, amount_to_transfer)
    if "status_transaction" in df_transactions.columns:
        df_transactions = df_transactions[
            df_transactions["status_transaction"] == "FAILED"
        ]
    groups = [df.iloc[i : i + GROUP_SIZE] for i in range(0, len(df), GROUP_SIZE)]
    for group in groups:
        try:
            wallets_id = group["wallet_id"].unique().tolist()
            wallets = get_wallets_by_ids(db_session, wallets_id)
            wallets = [
                add_funds_to_wallet(
                    wallet, get_amount_to_transfer(group, wallet.wallet_id)
                )
                for wallet in wallets
            ]
            transactions = create_transaction(
                batch_id,
                source_wallet,
                group,
                currency,
                concept,
                fee_percent,
                transaction_type,
                transaction_group,
                user_id
            )
            db_session.add_all(transactions)
            db_session.commit()
            df.loc[group.index, "create_account"] = "SUCCESSFUL"
        except SQLAlchemyError as e:
            db_session.rollback()
            df.loc[group.index, "create_account"] = "FAILED"
            logging.error(f"Error executing SQL create accounts: {str(e)}")
    return df