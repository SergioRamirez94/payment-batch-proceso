from database.models.transactions import Transaction, TransactionGroup, TransactionType
from database.models.wallets import Wallet
from services.wallet_service import validate_funds
from services.wallet_service import get_wallets_by_ids, add_funds_to_wallet, subtract_funds_from_wallet, get_integration_wallet_or_create
from sqlalchemy.orm import Session
from database.models.currency import Currency
from decimal import Decimal
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import List

logging.basicConfig(level=logging.INFO)

GROUP_SIZE = 300


class TransactionMaker:


    def __init__(self,  db_session: Session,
                df: pd.DataFrame,
                message_data,
                source_wallet:Wallet, 
                currency: Currency,
                tenant):
        self.db_session = db_session
        self.df = df
        self.transaction_type = TransactionType.transfer
        self.transaction_group = (
            TransactionGroup.bonus
            if message_data.process == "bonuses"
            else TransactionGroup.treasury
        )
        self.concept = message_data.concept if message_data.process == "bonuses" else ""
        self.fee_percent = Decimal(str(message_data.amount_percentage)) + Decimal(
            str(message_data.user_percentage)
        )
        self.source_wallet = source_wallet
        self.currency = currency
        self.tenant = tenant
        self.wallet_integration = get_integration_wallet_or_create(db_session, tenant, currency)
        self.user_id = message_data.user_id
        self.batch_id = message_data.batch_id
        self.amount_to_transfer = self.df["amount_fee"].apply(lambda x: Decimal(str(round(x,2)))).sum()


    def get_amount_to_transfer(self, wallet_id):
        return Decimal(str(round(self.df[self.df["wallet_id"].astype(str) == str(wallet_id)]["amount"].values[0],2)))


    def create_transaction(self, sub_group):
        
        transactions = []
        for _, row in sub_group.iterrows():
            decimal_amount = Decimal(str(row["amount"])) 
            total_transaction_fee = round(
                decimal_amount * Decimal(self.fee_percent)/Decimal("100"), 2
            )
            transactions.append(
                Transaction(
                    user_id=self.user_id,
                    amount=decimal_amount ,
                    concept=self.concept,
                    destination_wallet_id=row["wallet_id"],
                    source_wallet_id=self.source_wallet.wallet_id,
                    currency_currency_id=self.currency.currency_id,
                    transaction_type=self.transaction_type.value,
                    transaction_group=self.transaction_group.value,
                    variable_fee_percentage=self.fee_percent,
                    total_transaction_fee=total_transaction_fee,
                    total_amount_deducted=total_transaction_fee + decimal_amount,
                    status ='completed',
                    relate_transaction_id = self.batch_id
                )
            )
        return transactions


    def add_funds_to_wallets(self, sub_group)->List[Wallet]:
        wallets_id = sub_group["wallet_id"].unique().tolist()
        wallets = get_wallets_by_ids(self.db_session, wallets_id)
        wallets = [
            add_funds_to_wallet(
                wallet, self.get_amount_to_transfer(wallet.wallet_id)
            )
            for wallet in wallets
        ]


    def excute(self) -> List[Transaction]:
        
        df_transactions = self.df[(self.df["wallet_id"].notna())].copy()
        if "status_transaction" in df_transactions.columns:
            df_transactions = df_transactions[
                df_transactions["status_transaction"].isin(["FAILED"])
            ]
        else:
            self.df["status_transaction"] = "PENDING"
        amount_to_transfer = df_transactions["amount_fee"].apply(lambda x: Decimal(str(round(x,2)))).sum()
        validate_funds(self.source_wallet, amount_to_transfer)
        subtract_funds_from_wallet(self.source_wallet, amount_to_transfer)
        groups = [self.df.iloc[i : i + GROUP_SIZE] for i in range(0, len(self.df), GROUP_SIZE)]
        for group in groups:
            try:
                self.add_funds_to_wallets(group)
                transactions = self.create_transaction(group)
                add_funds_to_wallet(self.wallet_integration, sum([t.total_transaction_fee for t in transactions]))
                self.db_session.add_all(transactions)
                self.db_session.commit()
                self.df.loc[group.index, "status_transaction"] = "SUCCESSFUL"
            except SQLAlchemyError as e:
                self.db_session.rollback()
                self.df.loc[group.index, "status_transaction"] = "FAILED"
                logging.error(f"Error executing SQL create accounts: {str(e)}")


    def rollback_money(self):
        df_transactions = self.df[self.df["status_transaction"].isin(["FAILED"])]
        if df_transactions.empty:
            return
        amount_to_transfer = df_transactions["amount_fee"].apply(lambda x: Decimal(str(round(x,2)))).sum()
        add_funds_to_wallet(self.source_wallet, amount_to_transfer)
        self.db_session.commit()