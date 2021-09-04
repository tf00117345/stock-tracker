import scrapy
import pandas as pd
from sqlalchemy.orm import sessionmaker

from ..modals.stock_code_modal import db_connect, Stocks
from ..utils import numberutil


class StockPERatioSpider(scrapy.Spider):
    """
        獲取股票的每月營收
        資料來自富邦
    """

    name = "stock_profit"

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED': False,
        'ITEM_PIPELINES': {
            'stock.pipelines.stock_pe_ratio_pipelines.StockPERatioPipeline': 300,
        }
    }

    def __init__(self):
        self.engine = db_connect()
        self.session = sessionmaker(bind=self.engine)

    def start_requests(self):
        yield self.createRequest('2330')

        # session = self.session()
        # for code in session.query(Stocks.code):
        #     yield self.createRequest(code.code)

    def createRequest(self, stockCode):

        s = BeautifulSoup(r.text, "lxml")
        table = s.find("table", class_="tzxq")

        url = 'https://www.cmoney.tw/finance/f00032.aspx?s={code}'
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
        dataFrame = dataFrame.drop(index=[0, 1, 2])

        for index, row in dataFrame.iterrows():
            StockPERatioItem = {
                # 股票代碼
                'code': stockCode,
                'yearQuarter': row.get(0),
                # 年度
                'year': numberutil.toInt(row.get(0).split('.')[0]) + 1911,
                # 季別
                'quarter': numberutil.toInt(row.get(0).split('.')[1].replace('Q', '')),
            }
            yield StockPERatioItem

    def validate(self, stockCode, dataFrameList):
        if len(dataFrameList) != 4:
            self.logger.error('網頁格式錯誤, code=' + stockCode)
            return False
        dataFrame = dataFrameList[2]
        testSeries = dataFrame.iloc[2]
        headers = ''
        for column, value in testSeries.items():
            headers = headers + value
        if '季別營業收入營業成本營業毛利毛利率營業利益營益率業外收支稅前淨利稅後淨利EPS(元)' != headers:
            self.logger.error('錯誤表頭, code=' + stockCode)
            return False
        return True
