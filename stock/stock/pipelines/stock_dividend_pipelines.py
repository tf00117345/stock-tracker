from scrapy.exceptions import DropItem
from sqlalchemy.orm import sessionmaker

from ..modals.stock_dividend_modal import db_connect, create_table, StockDividend


# import logging


class StockDividendPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        self.Session = sessionmaker(bind=self.engine)

    def process_item(self, item, spider):

        if StockDividend.__tablename__ not in self.engine.table_names():
            create_table(self.engine)

        session = self.Session()

        if session.query(StockDividend). \
                filter(StockDividend.code == str(item['code'])). \
                filter(StockDividend.year == str(item['year'])). \
                first():
            session.close()
            raise DropItem("Duplicate item found: %s" % item["code"])

        try:
            session.add(StockDividend(**item))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item
