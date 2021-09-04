from scrapy.exceptions import DropItem
from sqlalchemy.orm import sessionmaker

from ..modals.stock_monthly_revenue_modal import db_connect, create_table, StockMonthlyRevenue


class StockMonthlyRevenuePipeline(object):
    def __init__(self):
        self.engine = db_connect()
        self.Session = sessionmaker(bind=self.engine)

    def process_item(self, item, spider):

        if StockMonthlyRevenue.__tablename__ not in self.engine.table_names():
            create_table(self.engine)

        session = self.Session()

        if session.query(StockMonthlyRevenue). \
                filter(StockMonthlyRevenue.code == str(item['code'])). \
                filter(StockMonthlyRevenue.year == str(item['year'])). \
                filter(StockMonthlyRevenue.month == str(item['month'])). \
                first():
            session.close()
            raise DropItem("Duplicate item found: %s" % item["code"])

        try:
            session.add(StockMonthlyRevenue(**item))
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item
