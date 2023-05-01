import pandas as pd
import ccxt
from Base.ConfigReader import Config
from Base.Service.DiscordService import DiscordService
from Model.Service.MongoDBService import MongoDBService
from Indicators.YuanIndicator import YuanIndicator

class Connector(object):
    def __init__(self):
        self.config = Config()

class BTCHedge(Connector):
    def __init__(self, exchange, api_key, api_secret, order_qty):
        super().__init__()
        self.api_key = api_key
        self.api_secret = api_secret
        self.exchange_name = exchange
        self.order_qty = order_qty
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
        transaction = list(self.mongo._transactionConn().find({'API_KEY': self.api_key, 'STRATEGY': 'BTCHedge', 'IS_CLOSE': 0}))
        if len(transaction) > 0:
            return
        volume = live_price['VOLUME']
        time = live_price['TIME']
        mean_vol = data['MEAN_VOLUME']
        if volume > mean_vol * 10:
            last_signal = self.mongo._lastSignal().find_one({'STRATEGY': 'BTCHedge'})
            if last_signal is None:
                self.mongo._lastSignal().insert_one({'STRATEGY': 'BTCHedge', 'TIME': time})
                last_signal_time = 123
            else:
                last_signal_time = last_signal['TIME']
            if last_signal_time == time:
                return
            up_trend = list(self.mongo._allMarketConn().find().sort('PCT_CHANGE', -1).limit(3))
            down_trend = list(self.mongo._allMarketConn().find().sort('PCT_CHANGE', 1).limit(3))
            up_lst = []
            up_price_lst = []
            down_lst = []
            down_price_lst = []
            for i in range(3):
                up_lst.append(up_trend[i]['SYMBOL'])
                up_price_lst.append(up_trend[i]['CLOSE'])
                down_lst.append(down_trend[i]['SYMBOL'])
                down_price_lst.append(down_trend[i]['CLOSE'])

            exchange = ccxt.binanceusdm({
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'enableRateLimit': True,
                'option': {
                    'defaultMarket': 'future',
                },
            })
            per_order = self.order_qty / 6
            for i in range(3):
                amount = round(per_order / up_price_lst[i], 3)
                exchange.create_market_order(up_lst[i], 'buy', amount)
                self.mongo._transactionConn().insert_one({'API_KEY': self.api_key, 'STRATEGY': 'BTCHedge', 'SYMBOL': up_lst[i], 'AMOUNT': amount, 'PRICE': up_price_lst[i], 'SIDE': 'BUY', 'IS_CLOSE': 0, 'TIME': time})
            for i in range(3):
                amount = round(per_order / down_price_lst[i], 3)
                exchange.create_market_order(down_lst[i], 'sell', amount)
                self.mongo._transactionConn().insert_one({'API_KEY': self.api_key, 'STRATEGY': 'BTCHedge', 'SYMBOL': down_lst[i], 'AMOUNT': amount, 'PRICE': down_price_lst[i], 'SIDE': 'SELL', 'IS_CLOSE': 0, 'TIME': time})

            self.discord.btcHedgeSendMessage(f'{self.name} Open position\nBUY: {up_lst[0]}, {up_lst[1]}, {up_lst[2]}\nSELL: {down_lst[0]}, {down_lst[1]}, {down_lst[2]}')
            self.mongo._lastSignal().update_one({'STRATEGY': 'BTCHedge'}, {'$set': {'TIME': time}}, upsert=True)
