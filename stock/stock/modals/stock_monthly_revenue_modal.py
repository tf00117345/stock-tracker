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


class StockMonthlyRevenue(Base):
    __tablename__ = "stockMonthlyRevenue"
    code = Column('code', Text(), primary_key=True)
    year = Column('year', Text(), primary_key=True)
    month = Column('month', Text(), primary_key=True)
    operating_revenue = Column('operating_revenue', Text())
    mom = Column('mom', Text())
    same_month_last_year = Column('same_month_last_year', Text())
    yoy = Column('yoy', Text())
    cumulative_revenue = Column('cumulative_revenue', Text())
    cumulative_revenue_yoy = Column('cumulative_revenue_yoy', Text())
