import scrapy


class StockCodeItem(scrapy.Item):
    code = scrapy.Field()
    name = scrapy.Field()
    marketType = scrapy.Field()
    stockType = scrapy.Field()
    industryType = scrapy.Field()
    issuanceDate = scrapy.Field()


class StockDividendItem(scrapy.Item):
    # 股票代碼
    code = scrapy.Field()
    # 年度
    year = scrapy.Field()
    # 現金股利
    cash = scrapy.Field()
    # 股票股利
    stocks = scrapy.Field()
