from ..database import BaseWallet as Base
from sqlalchemy import Column, String, ForeignKey, Numeric, DateTime
import uuid
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import relationship
import datetime


class Wallet(Base):
    __tablename__ = 'wallet'

    wallet_id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    wallet_name = Column(String, nullable=False)
    currency_currency_id = Column(PG_UUID(as_uuid=True), ForeignKey('currency.currency_id'), nullable=False)
    currency = relationship('Currency', back_populates='wallets')
    balance = Column(Numeric(precision=10, scale=2))
    account_account_id = Column(PG_UUID(as_uuid=True), ForeignKey('account.account_id'), nullable=False)
    account = relationship('Account', back_populates='wallets')
    created_at = Column(DateTime, default=datetime.datetime.utcnow)