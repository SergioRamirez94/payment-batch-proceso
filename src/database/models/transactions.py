from ..database import BaseWallet
from sqlalchemy import Column, String, ForeignKey, Numeric, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
import uuid
from enum import Enum
from datetime import datetime


class  TransactionType(Enum):
    swap = "swap"
    withdraw = "withdraw"
    transfer = "transfer"
    add_funds = "add_funds"
    fee_transfer = "fee_transfer"
    bonus =  "bonus"

class  TransactionGroup(Enum):
    bonus= "bonus"
    treasury= "treasury"


class Transaction(BaseWallet):
    __tablename__ = 'transaction'
    transaction_id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(PG_UUID(as_uuid=True))
    amount = Column(Numeric(precision=10, scale=2))
    concept = Column(String)
    destination_wallet_id = Column(PG_UUID(as_uuid=True), ForeignKey('wallet.wallet_id'), nullable=False)
    source_wallet_id = Column(PG_UUID(as_uuid=True), ForeignKey('wallet.wallet_id'), nullable=False)
    currency_currency_id = Column(PG_UUID(as_uuid=True), ForeignKey('currency.currency_id'), nullable=False)
    transaction_type = Column(String)
    transaction_group = Column(String)
    variable_fee_percentage  = Column(Numeric(precision=10, scale=3))
    total_transaction_fee = Column(Numeric(precision=10, scale=2))
    total_amount_deducted = Column(Numeric(precision=10, scale=2))
    created_at = Column(TIMESTAMP, default=datetime.utcnow)
    update_at = Column(TIMESTAMP, default=datetime.utcnow, onupdate=datetime.utcnow)
    status = Column(String)
    relate_transaction_id = Column(PG_UUID(as_uuid=True))

    @property
    def transaction_type_enum(self):
        return TransactionType(self.transaction_type)

    @transaction_type_enum.setter
    def transaction_type_enum(self, value):
        self.transaction_type = value.value

    @property
    def transaction_group_enum(self):
        return TransactionGroup(self.transaction_group)

    @transaction_group_enum.setter
    def transaction_group_enum(self, value):
        self.transaction_group = value.value