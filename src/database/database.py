import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


PG_WALLETS_URL = os.environ['PG_WALLETS_URL']

engine_wallets = create_engine(PG_WALLETS_URL)

SesionLocalWallet = sessionmaker(autocommit = False, autoflush =False, bind=engine_wallets)

BaseWallet = declarative_base()