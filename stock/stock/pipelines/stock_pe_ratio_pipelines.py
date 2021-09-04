from scrapy.exceptions import DropItem
from sqlalchemy.orm import sessionmaker

from ..modals.stock_pe_ratio import db_connect, create_table, StockPERatio


class StockPERatioPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        self.Session = sessionmaker(bind=self.engine)

    def process_item(self, item, spider):

        if StockPERatio.__tablename__ not in self.engine.table_names():
            create_table(self.engine)

        session = self.Session()

        if session.query(StockPERatio). \
                filter(StockPERatio.code == str(item['code'])). \
                filter(StockPERatio.yearQuarter == str(item['yearQuarter'])). \
                first():
            session.close()
            raise DropItem("Duplicate item found: %s" % item["code"])

        try:
            session.add(StockPERatio(**item))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item
