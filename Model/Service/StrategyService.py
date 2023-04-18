from Indicators.YuanIndicator import YuanIndicator
from Base.ConfigReader import Config
from Model.Service.MongoDBService import MongoDBService

class Connector(object):
    def __init__(self):
        self.config = Config()

class StrategyService(Connector):
    def __init__(self):
        super().__init__()
        config = self.config
        self.api_key = config["Binance"]["api_key"]
        self.api_secret = config["Binance"]["api_secret"]
        self.target_lst = ['BTC/USDT', 'ETH/USDT']
        mongo = MongoDBService()
        self._strategyConn = mongo._strategyConn()

    def YuanIndicatorGenerator(self):
        for i in range(len(self.target_lst)):
            indicator = YuanIndicator(self.target_lst[i], self.api_key, self.api_secret)
            ohlcv = indicator.getOHLCV('3m')
            time = ohlcv['TIME'].iloc[-1]
            mean_vol = indicator.cleanData2GenerateMeanVolume(ohlcv)
            atr = indicator.ATR(ohlcv)
            slope = ohlcv['CLOSE'].iloc[-1] - ohlcv['CLOSE'].iloc[-10]
            data_mongo = {
                'SYMBOL': self.target_lst[i],
                'STRATEGY': 'YuanCopyTrade',
                'TIME': time,
                'MEAN_VOLUME': mean_vol,
                'ATR': atr,
                'SLOPE': slope,
            }
            self._strategyConn.insert_one(data_mongo)
            cursor = list(self._strategyConn.find({'SYMBOL': self.symbol, 'STRATEGY': 'YuanCopyTrade'}))
            if len(cursor) > 1:
                self._strategyConn.delete_one({'_id': cursor[0]['_id']})
