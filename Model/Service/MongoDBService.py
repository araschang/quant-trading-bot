from pymongo import MongoClient


class MongoDBService(object):
    def __init__(self):
        self.client = MongoClient('localhost', 27017)

    def _livePriceConn(self):
        db = self.client['quant']
        return db['live_price']

    def _memberInfoConn(self):
        db = self.client['quant']
        return db['member_info']

    def _transactionConn(self):
        db = self.client['quant']
        return db['transaction']

    def _trendConn(self):
        db = self.client['quant']
        return db['trend']

    def _lastTradeTimeConn(self):
        db = self.client['quant']
        return db['last_trade_time']

    def _accountConn(self):
        db = self.client['quant']
        return db['account']

    def _strategyConn(self):
        db = self.client['quant']
        return db['strategy']