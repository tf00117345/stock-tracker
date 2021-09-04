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


class StockProfit(Base):
    __tablename__ = "stockProfit"
    # 股票代碼
    code = Column('code', Text(), primary_key=True)
    yearQuarter = Column('yearQuarter', Text(), primary_key=True)
    # 年度
    year = Column('year', Text())
    # 季別
    quarter = Column('quarter', Text())
    # 營業收入 (百萬元)
    operating_revenue = Column('operating_revenue', Text())
    # 營業成本 (百萬元)
    operating_cost = Column('operating_cost', Text())
    # 營業毛利 (百萬元)
    gross_profit = Column('gross_profit', Text())
    # 毛利率 (百分比)
    gross_profit_margin = Column('gross_profit_margin', Text())
    # 營業利益 (百萬元)
    operating_profit = Column('operating_profit', Text())
    # 營益率 (百分比)
    operating_profit_margin = Column('operating_profit_margin', Text())
    # 業外收支 (百萬元)
    non_operating_revenue = Column('non_operating_revenue', Text())
    # 稅前淨利 (百萬元)
    pre_tax_income = Column('pre_tax_income', Text())
    # 稅後淨利 (百萬元)
    net_income = Column('net_income', Text())
    # eps
    eps = Column('eps', Text())

