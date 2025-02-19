
from database.models.currency import Currency
from sqlalchemy.orm import Session

def get_currency_by_iso_code_or_throw(db: Session, is_code: str) -> Currency:
    currency = db.query(Currency).filter(Currency.iso_code == is_code).first()
    if not currency:
        raise Exception("Currency not found")
    return currency