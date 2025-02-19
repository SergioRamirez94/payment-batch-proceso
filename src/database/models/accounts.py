from ..database import BaseWallet as Base
import uuid
from sqlalchemy import Column, String, ForeignKey, Boolean
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import relationship
from enum import Enum as EnumClass
from sqlalchemy import Column, String, Enum, Integer
from sqlalchemy.types import Enum as SAEnum

class AccountType(EnumClass):
    personal = 0
    bussiness = 1

class Tenant(EnumClass):
    beu = 0
    tikin = 1

class Account(Base):

    __tablename__ = 'account'
    account_id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_name = Column(String)
    account_type  = Column(Integer, nullable=False)
    business_url = Column(Integer, nullable=False)
    tenant = Column(Integer, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    is_verified = Column(Boolean, nullable=False, default=False)
    wallets = relationship('Wallet', back_populates='account')

    @property
    def account_type_enum(self):
        return AccountType(self.account_type)

    @account_type_enum.setter
    def account_type_enum(self, value):
        self.account_type = value.value

    @property
    def tenant_enum(self):
        return Tenant(self.tenant)

    @tenant_enum.setter
    def tenant_enum(self, value):
        self.tenant = value.value
