import pandas as pd
import ccxt
from Base.ConfigReader import Config
from Base.Service.DiscordService import DiscordService
from Model.Service.MongoDBService import MongoDBService

class Connector(object):
    def __init__(self):
        self.config = Config()

class BTCHedge(Connector):
    def __init__(self, exchange, api_key, api_secret):
        super().__init__()
        self.api_key = api_key
        self.api_secret = api_secret
        self.exchange_name = exchange
        self.discord = DiscordService()
        self.mongo = MongoDBService()
        config = self.config['Binance']
        if self.api_key == config['api_key']:
            self.name = 'Aras'
        else:
            self.name = 'Yuan'
        # if exchange == 'binance':
        #     self.exchange = ccxt.binanceusdm({
        #         'apiKey': self.api_key,
        #         'secret': self.api_secret,
        #         'enableRateLimit': True,
        #         'option': {
        #             'defaultMarket': 'future',
        #         },
        #     })

    def signalGenerator(self):
        data = self.mongo._strategyConn().find_one({'SYMBOL': 'BTC/USDT', 'STRATEGY': 'YuanCopyTrade'})
        live_price = self.mongo._livePriceConn().find_one({'SYMBOL': 'BTCUSDT'})
        volume = live_price['VOLUME']
        time = live_price['TIME']
        mean_vol = data['MEAN_VOLUME']
        if volume > mean_vol * 8:
            last_signal = self.mongo._lastSignal().find_one({'STRATEGY': 'BTCHedge'})
            if last_signal is None:
                self.mongo._lastSignal().insert_one({'STRATEGY': 'BTCHedge', 'TIME': time})
                last_signal_time = 123
            else:
                last_signal_time = last_signal['TIME']
            if last_signal_time == time:
                return
            up_trend = list(self.mongo._allMarketConn.find().sort('PCT_CHANGE', -1).limit(3))
            down_trend = list(self.mongo._allMarketConn.find().sort('PCT_CHANGE', 1).limit(3))
            up0 = up_trend[0]['SYMBOL']
            up1 = up_trend[1]['SYMBOL']
            up2 = up_trend[2]['SYMBOL']
            down0 = down_trend[0]['SYMBOL']
            down1 = down_trend[1]['SYMBOL']
            down2 = down_trend[2]['SYMBOL']
            self.discord.btc_hedge(f'📈BUY: {up0}, {up1}, {up2}\n📉SELL: {down0}, {down1}, {down2}')
            self.mongo._lastSignal().update_one({'STRATEGY': 'BTCHedge'}, {'$set': {'TIME': time}}, upsert=True)
