from scrapy.utils.project import get_project_settings
from sqlalchemy import (
    Text, Integer)
from sqlalchemy import create_engine, Column
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(get_project_settings().get("CONNECTION_STRING"))


def create_table(engine):
    Base.metadata.create_all(engine)


class StockInfo(Base):
    __tablename__ = "stockInfo"
    # 股票代碼
    code = Column('code', Text(), primary_key=True)
    # 開盤價
    opening_price = Column('code', Text())
    # 最高價
    highest_price = Column('code', Text())
    # 最低價
    lowest_price = Column('code', Text())
    # 收盤價
    closing_price = Column('code', Text())
    # 成交量
    amount = Column('code', Text())


