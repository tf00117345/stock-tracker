import scrapy
import pandas as pd
from sqlalchemy.orm import sessionmaker

from ..modals.stock_code_modal import db_connect, Stocks
from ..utils import numberutil


class StockMonthlyRevenueSpider(scrapy.Spider):
    """
        獲取股票的每月營收
        資料來自富邦
    """

    name = "stock_monthly_revenue"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
        'ITEM_PIPELINES': {
            'stock.pipelines.stock_monthly_revenue_pipelines.StockMonthlyRevenuePipeline': 300,
        }
    }

    def __init__(self):
        self.engine = db_connect()
        self.session = sessionmaker(bind=self.engine)

    def start_requests(self):
        session = self.session()
        for code in session.query(Stocks.code):
            yield self.createRequest(code.code)

    def createRequest(self, stockCode):
        url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zc/zch/zch_{code}.djhtm'
        return scrapy.Request(
            url=url.format(code=stockCode),
            callback=self.parse,
            cb_kwargs={'stockCode': stockCode},
        )

    def parse(self, response, stockCode):
        dataFrameList = pd.read_html(io=response.text, flavor='bs4')
        if not self.validate(stockCode, dataFrameList):
            return
        dataFrame = dataFrameList[2]
        dataFrame = dataFrame.drop(index=[0, 1, 2, 3, 4, 5])

        for index, row in dataFrame.iterrows():
            StockMonthlyRevenueItem = {
                "code": stockCode,
                "year": numberutil.toInt(row.get(0).split('/')[0]) + 1911,
                "month": numberutil.toInt(row.get(0).split('/')[1]),
                "operating_revenue": (row.get(1)),
                "mom": (row.get(2)),
                "same_month_last_year": (row.get(3)),
                "yoy": (row.get(4)),
                "cumulative_revenue": (row.get(5)),
                "cumulative_revenue_yoy": (row.get(6)),
            }
            yield StockMonthlyRevenueItem

    def validate(self, stockCode, dataFrameList):
        if len(dataFrameList) != 3:
            self.logger.error('網頁格式錯誤, code=' + stockCode)
            return False
        dataFrame = dataFrameList[2]
        testSeries = dataFrame.iloc[5]
        headers = ''
        for column, value in testSeries.items():
            headers = headers + str(value)
        if '年/月營收月增率去年同期年增率累計營收年增率nan' != headers:
            self.logger.error('錯誤表頭, code=' + stockCode)
            return False
        return True
