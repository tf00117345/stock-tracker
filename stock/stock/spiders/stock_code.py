import scrapy
import pandas as pd

from scrapy.exceptions import CloseSpider
from dateutil.parser import *


# 獲取所有上市和上櫃的證券代號及名稱


class AllStockCodeSpider(scrapy.Spider):
    # Spider 的名字必須是唯一
    name = "stocks"

    # Spider 自己的客製化設定
    custom_settings = {
        # 每個下載間隔幾秒
        'DOWNLOAD_DELAY': 1,
        # 可以同時跑幾個 request
        'CONCURRENT_REQUESTS': 1,
        # 關掉 cookie 避免被發現是爬蟲
        'COOKIES_ENABLED': False,
        'ITEM_PIPELINES': {
            'stock.pipelines.stock_code_pipelines.StockCodePipeline': 300,
        }
    }

    # 初始化方法
    def __init__(self, *args, **kwargs):
        super(AllStockCodeSpider, self).__init__(*args, **kwargs)

    # Spider 必須回傳 Requests 給 Engine 統一發送
    def start_requests(self):
        urls = [
            'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=1'
            '&industry_code=&Page=1&chklike=Y',
            'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=2&issuetype=4'
            '&industry_code=&Page=1&chklike=Y',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        htmlStr = response.body.decode('MS950')
        dataFrameList = pd.read_html(htmlStr)
        dataFrame = dataFrameList[0]
        dataFrame.columns = dataFrame.iloc[0]
        dataFrame = dataFrame.drop(0)

        self.validateDataFrameColumns(dataFrame)

        for row in dataFrame.itertuples():
            StockCodeItem = {
                "code": row[3],
                "name": row[4],
                "marketType": row[5],
                "stockType": row[6],
                "industryType": row[7],
                "issuanceDate": parse(row[8])
            }
            yield StockCodeItem

    # 驗證表頭是否正確
    def validateDataFrameColumns(self, dataFrame):
        headerArray = dataFrame.columns.values
        headerCombine = ''
        for header in headerArray:
            headerCombine = headerCombine + header + ','

        assertHeaders = '頁面編號,國際證券編碼,有價證券代號,有價證券名稱,市場別,有價證券別,產業別,公開發行/上市(櫃)/發行日,CFICode,備註,'
        if assertHeaders != headerCombine:
            self.logger.error('錯誤表頭=' + headerCombine)
            # 直接關掉 Spider 因為大概是要重寫了
            raise CloseSpider('錯誤表頭')
