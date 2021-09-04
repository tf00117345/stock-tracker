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


class StockPERatio(Base):
    __tablename__ = "stockPERatio"
    # 股票代碼
    code = Column('code', Text(), primary_key=True)
    yearQuarter = Column('yearQuarter', Text(), primary_key=True)
    # 年度
    year = Column('year', Text())
    # 季別
    quarter = Column('quarter', Text())
    # 法人預估本益比
    corporate_estimated_pe = Column('corporate_estimated_pe', Text())
    # 本益比(近4季)
    pe_quarter = Column('pe_quarter', Text())
    # 本益比(季低)
    pe_high = Column('pe_high', Text())
    # 本益比(季高)
    pe_low = Column('pe_low', Text())

