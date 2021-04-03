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


class Stocks(Base):
    __tablename__ = "stockCode"
    code = Column('code', Text(), primary_key=True)
    name = Column('name', Text())
    marketType = Column('marketType', Text())
    stockType = Column('stockType', Text())
    industryType = Column('industryType', Text())
    issuanceDate = Column('issuanceDate', Text())
