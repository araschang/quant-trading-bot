from Base.ConfigReader import Config
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
    
    def _signalTimeConn(self):
        db = self.client['quant']
        return db['signal_time']
    
    def _lastTradeConn(self):
        db = self.client['quant']
        return db['last_trade']
    
    def _transactionConn(self):
        db = self.client['quant']
        return db['transaction']
    
    def _positionConn(self):
        db = self.client['quant']
        return db['position']
    
    def _trendConn(self):
        db = self.client['quant']
        return db['trend']
    
    def _lastSignalConn(self):
        db = self.client['quant']
        return db['last_signal']