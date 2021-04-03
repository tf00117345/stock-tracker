from scrapy.exceptions import DropItem
from sqlalchemy.orm import sessionmaker
# import logging
from ..modals.stock_code_modal import db_connect, create_table, Stocks


class StockCodePipeline(object):
    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        self.engine = db_connect()
        self.Session = sessionmaker(bind=self.engine)

    def process_item(self, item, spider):
        if Stocks.__tablename__ not in self.engine.table_names():
            create_table(self.engine)

        session = self.Session()

        if session.query(Stocks). \
                filter(Stocks.code == str(item['code'])). \
                first():
            session.close()
            raise DropItem("Duplicate item found: %s" % item["code"])

        try:
            session.add(Stocks(**item))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item
