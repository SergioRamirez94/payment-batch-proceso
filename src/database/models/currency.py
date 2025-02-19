from ..database import BaseWallet as Base
import uuid
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import relationship

class Currency(Base):
    __tablename__ = 'currency'
    currency_id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    iso_code = Column(String, nullable=False)
    wallets = relationship("Wallet", back_populates="currency")