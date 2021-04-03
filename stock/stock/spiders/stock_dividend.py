import pandas as pd
import scrapy
from sqlalchemy.orm import sessionmaker

from ..modals.stock_code_modal import db_connect, Stocks


class StockDividendSpider(scrapy.Spider):
    """
        獲取股票的歷史股利政策
        資料來自富邦
    """

    name = "stock_dividend"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
        'ITEM_PIPELINES': {
            'stock.pipelines.stock_dividend_pipelines.StockDividendPipeline': 300,
        }
    }

    def __init__(self):
        self.engine = db_connect()
        self.session = sessionmaker(bind=self.engine)

    def start_requests(self):
        # urls = [
        #     'https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcc/zcc_2330.djhtm',
        # ]
        #
        # for url in urls:
        #     yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'stockCode': 2330})

        session = self.session()
        for code in session.query(Stocks.code):
            yield self.createRequest(code.code)

    def createRequest(self, stockCode):
        url = 'https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcc/zcc_{code}.djhtm'
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
        dataFrame = dataFrame.drop(index=[0, 1, 2, 3])
        for index, row in dataFrame.iterrows():
            StockDividendItem = {
                "code": stockCode,
                "year": row.get(0),
                "cash": row.get(3),
                "stocks": row.get(6),
            }
            yield StockDividendItem

    def validate(self, stockCode, dataFrameList):
        if len(dataFrameList) != 3:
            self.logger.error('網頁格式錯誤, code=' + stockCode)
            return False
        dataFrame = dataFrameList[2]
        testSeries = dataFrame.iloc[3]
        headers = ''
        for column, value in testSeries.items():
            headers = headers + value
        if '股利所屬年度盈餘發放公積發放小計盈餘配股公積配股小計合計員工配股率(%)' != headers:
            self.logger.error('錯誤表頭, code=' + stockCode)
            return False
        return True
