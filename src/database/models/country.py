from ..database import BaseWallet as Base
import uuid
from sqlalchemy import Column, String, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import relationship

class Country(Base):
    __tablename__ = 'country'
    country_id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    code = Column(String, nullable=False)
    country_name = Column(String, nullable=False)
    currency_currency_id = Column(PG_UUID(as_uuid=True), ForeignKey('currency.currency_id'))  # Cambio aquí
    currency = relationship('Currency', back_populates='country')