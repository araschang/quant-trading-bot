import pandas as pd
import ccxt
from Base.ConfigReader import Config
from Base.Service.DiscordService import DiscordService
from Model.Service.MongoDBService import MongoDBService

class Connector(object):
    def __init__(self):
        self.config = Config()

class YuanIndicator(Connector):
    def __init__(self, symbol, exchange, api_key, api_secret, strategy):
        super().__init__()
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.exchange_name = exchange
        self.strategy = strategy
        self.discord = DiscordService()
        self.mongo = MongoDBService()
        config = self.config['Binance']
        if self.api_key == config['api_key']:
            self.name = 'Aras'
        else:
            self.name = 'Yuan'
        if exchange == 'binance':
            self.exchange = ccxt.binanceusdm({
                'apiKey': self.api_key,
                'secret': self.api_secret,
                'enableRateLimit': True,
                'option': {
                    'defaultMarket': 'future',
                },
            })
        elif exchange == 'bybit':
            self.exchange = ccxt.bybit({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'enableRateLimit': True,
        })

    def getOHLCV(self, timeframe):
        ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe, limit=100)
        df = pd.DataFrame(ohlcv, columns=['TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])
        df['OPEN'] = df['OPEN'].astype(float)
        df['HIGH'] = df['HIGH'].astype(float)
        df['LOW'] = df['LOW'].astype(float)
        df['CLOSE'] = df['CLOSE'].astype(float)
        df['VOLUME'] = df['VOLUME'].astype(float)
        df['TIME'] = df['TIME'].astype(int)
        return df

    def cleanData2GenerateMeanVolume(self, ohlcv_df):
        volume = ohlcv_df['VOLUME']
        Q1 = volume.quantile(0.25)
        Q3 = volume.quantile(0.75)
        IQR = Q3 - Q1
        volume_mean = volume[(volume >= Q1 - 1.5 * IQR) & (volume <= Q3 + 1.5 * IQR)].mean()
        return volume_mean

    def checkSignal(self, mean_volume, ohlcv_df):
        if self.exchange_name == 'binance':
            symbol = self.symbol.split('/')
            symbol = symbol[0] + symbol[1]
        elif self.exchange_name == 'bybit':
            symbol = self.symbol

        if ohlcv_df['VOLUME'].iloc[-1] >= mean_volume * 9:
            slope = ohlcv_df['CLOSE'].iloc[-1] - ohlcv_df['CLOSE'].iloc[-10]
            # trend = self.getTrend()

            if slope <= 0:
            # if slope <= 0 and trend == 'up':
                return 'buy'

            # elif slope > 0 and trend == 'down':
            elif slope > 0:
                return 'sell'
            else:
                return ''

    def openPosition(self, side, assetPercent, now_price: float, now_time, atr) -> None:
        try:
            last_close = self.getLastTradeData()[0]
        except:
            self.insertLastTradeData(123)
            last_close = self.getLastTradeData()[0]
        last_close_time = int(last_close['TIME'])
        isnt_same_as_previous_close = (now_time != last_close_time)
        if not isnt_same_as_previous_close:
            return

        transaction = self.getOpenPosition()
        if len(transaction) > 0:
            return

        if self.exchange_name == 'binance':
            if self.symbol[:3] == 'BTC':
                round_digit = 1
            elif self.symbol[:3] == 'ETH':
                round_digit = 2
        elif self.exchange_name == 'bybit':
            if self.symbol[:3] == 'BTC':
                round_digit = 1
            elif self.symbol[:3] == 'ETH':
                round_digit = 2

        wallet_balance = float(self.exchange.fetch_balance()['info']['totalWalletBalance'])
        amount = round(wallet_balance * assetPercent / now_price, 3)
        order = self.exchange.create_market_order(self.symbol, side, amount)['info']
        orderId = order['orderId']
        orderType = order['type']
        time = int(order['updateTime'])
        price = float(order['avgPrice'])
        self.insertTransationData(time, orderId, orderType, side, amount, price, atr, 0)
        self.discord.sendMessage(f'**{self.symbol}** {self.name} {side.upper()} {amount} at {now_price}')

        if side == 'buy':
            stop_loss_side = 'sell'
            stop_loss_price = round(now_price - (1.5 * atr), round_digit)
            take_profit_price = round(now_price + (4 * atr), round_digit)
        else:
            stop_loss_side = 'buy'
            stop_loss_price = round(now_price + (1.5 * atr), round_digit)
            take_profit_price = round(now_price - (4 * atr), round_digit)

        try:
            if self.exchange_name == 'binance':
                self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price, 'closePosition': True})['info']['orderId']
                self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'takeProfitPrice': take_profit_price, 'closePosition': True})['info']['orderId']
        except Exception:
            self.exchange.cancel_all_orders(self.symbol)
            now_price = self.getLivePrice()
            if side == 'buy':
                stop_loss_price = round(now_price - (1.5 * atr), round_digit)
                take_profit_price = round(now_price + (4 * atr), round_digit)
            else:
                stop_loss_price = round(now_price + (1.5 * atr), round_digit)
                take_profit_price = round(now_price - (4 * atr), round_digit)

            if self.exchange_name == 'binance':
                self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price, 'closePosition': True})
                self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'takeProfitPrice': take_profit_price, 'closePosition': True})

    def changeStopLoss(self, price):
        transaciton = self.getOpenPosition()[0]
        side = transaciton['SIDE']
        order_info = self.exchange.fetch_open_orders(self.symbol)
        if side == 'buy':
            stop_loss_side = 'sell'
        else:
            stop_loss_side = 'buy'

        for i in range(len(order_info)):
            if order_info[i]['info']['type'] == 'STOP_MARKET':
                orderId = order_info[i]['info']['orderId']
                break

        self.exchange.cancel_order(orderId, self.symbol)
        self.exchange.create_market_order(self.symbol, stop_loss_side, 1, params={'stopLossPrice': price, 'closePosition': True})

    def checkIfChangeStopLoss(self, now_price):
        if self.exchange_name == 'binance':
            position = self.getOpenPosition()
            has_position = len(position) > 0
            if has_position:
                transaction = position[0]
                price = float(transaction['PRICE'])
                atr = float(transaction['ATR'])
                stoploss_stage = int(transaction['STOPLOSS_STAGE'])
                amount = float(transaction['AMOUNT'])
                side = transaction['SIDE']
                if side == 'buy':
                    if now_price >= round(price + 1 * atr, 4) and stoploss_stage == 0:
                        stoploss_price = round(price + 0.0009 * price, 2)
                        self.changeStopLoss(stoploss_price)
                        self.discord.sendMessage(f'**{self.symbol}** {self.name} Stoploss Stage 1, Protect Original Price')
                        self.updateTransationData('STOPLOSS_STAGE', 1)

                    elif now_price >= round(price + 3 * atr, 4) and stoploss_stage == 1:
                        self.exchange.create_market_order(self.symbol, 'sell', round(amount / 2, 3))
                        self.discord.sendMessage(f'**{self.symbol}** {self.name} Stoploss Stage 2, Sell Half')
                        stoploss_price = round(price + 2 * atr, 2)
                        try:
                            self.changeStopLoss(stoploss_price)
                        except:
                            self.exchange.create_market_order(self.symbol, 'sell', round(amount / 2, 3))
                        self.updateTransationData('STOPLOSS_STAGE', 2)

                else:
                    if now_price <= round(price - 1 * atr, 4) and stoploss_stage == 0:
                        stoploss_price = round(price - 0.0009 * price, 2)
                        self.changeStopLoss(stoploss_price)
                        self.discord.sendMessage(f'**{self.symbol}** {self.name} Stoploss Stage 1, Protect Original Price')
                        self.updateTransationData('STOPLOSS_STAGE', 1)

                    elif now_price <= round(price - 3 * atr, 4) and stoploss_stage == 1:
                        self.exchange.create_market_order(self.symbol, 'buy', round(amount / 2, 3))
                        self.discord.sendMessage(f'**{self.symbol}** {self.name} Stoploss Stage 2, Buy Half')
                        stoploss_price = round(price - 2 * atr, 2)
                        try:
                            self.changeStopLoss(stoploss_price)
                        except:
                            self.exchange.create_market_order(self.symbol, 'buy', round(amount / 2, 3))
                        self.updateTransationData('STOPLOSS_STAGE', 2)

            else:
                self.exchange.cancel_all_orders(self.symbol)

    def checkIfThereIsStopLoss(self):
        if self.exchange_name == 'binance':
            position = self.getOpenPosition()
            has_position = len(position) > 0
            if has_position:
                if len(self.exchange.fetch_open_orders(self.symbol)) < 2:
                    self.exchange.cancel_all_orders(self.symbol)
                    transaction = position[0]
                    price = self.getLivePrice()
                    atr = float(transaction['ATR'])
                    side = transaction['SIDE']
                    amount = float(transaction['AMOUNT'])
                    if side == 'buy':
                        stop_side = 'sell'
                        stop_loss_price = round(price - 1.5 * atr, 2)
                        take_profit_price = round(price + 4 * atr, 2)
                    else:
                        stop_side = 'buy'
                        stop_loss_price = round(price + 1.5 * atr, 2)
                        take_profit_price = round(price - 4 * atr, 2)
                    self.exchange.create_market_order(self.symbol, stop_side, amount, params={'stopLossPrice': stop_loss_price, 'closePosition': True})
                    self.exchange.create_market_order(self.symbol, stop_side, amount, params={'takeProfitPrice': take_profit_price, 'closePosition': True})

            else:
                if len(self.exchange.fetch_open_orders(self.symbol)) != 0:
                    self.exchange.cancel_all_orders(self.symbol)

    def checkTrend(self):
        ohlcv_df = self.getOHLCV('4h')
        ohlcv_df['SHORT_MA'] = ohlcv_df['CLOSE'].ewm(com=20, min_periods=20).mean()
        ohlcv_df['LONG_MA'] = ohlcv_df['CLOSE'].ewm(com=40, min_periods=40).mean()
        db = self.mongo._trendConn()
        if ohlcv_df['SHORT_MA'].iloc[-1] < ohlcv_df['LONG_MA'].iloc[-1]:
            trend = {'SYMBOL': self.symbol, 'TREND': 'down'}
            db.insert_one(trend)
            cursor = list(db.find({'SYMBOL': self.symbol}))
            if len(cursor) > 1:
                db.delete_one({'_id': cursor[0]['_id']})

        else:
            trend = {'SYMBOL': self.symbol, 'TREND': 'up'}
            db.insert_one(trend)
            cursor = list(db.find({'SYMBOL': self.symbol}))
            if len(cursor) > 1:
                db.delete_one({'_id': cursor[0]['_id']})

    def insertTransationData(self, time, orderId, orderType, side, amount, price, ATR, stoploss_stage):
        db = self.mongo._transactionConn()
        data = {
            'TIME': time,
            'API_KEY': self.api_key,
            'SYMBOL': self.symbol,
            'ORDER_ID': orderId,
            'ORDER_TYPE': orderType,
            'SIDE': side,
            'AMOUNT': amount,
            'PRICE': price,
            'ATR': ATR,
            'STRATEGY': self.strategy,
            'STOPLOSS_STAGE': stoploss_stage,
            'CLOSE_PRICE': 0,
            'IS_CLOSE': 0,
        }
        db.insert_one(data)

    def getTransactionData(self):
        db = self.mongo._transactionConn()
        return list(db.find({'API_KEY': self.api_key, 'SYMBOL': self.symbol, 'STRATEGY': self.strategy}))

    def updateTransationData(self, column, param):
        db = self.mongo._transactionConn()
        db.update_one({'API_KEY': self.api_key, 'SYMBOL': self.symbol, 'STRATEGY': self.strategy, 'IS_CLOSE': 0}, {'$set': {f'{column}': param}})

    def getLivePrice(self):
        db = self.mongo._livePriceConn()
        symbol = self.symbol.replace('/', '')
        return float(list(db.find({'SYMBOL': symbol}).sort('TIME', -1).limit(1))[0]['CLOSE'])

    def getLastTradeData(self):
        db = self.mongo._transactionConn()
        return list(db.find({'API_KEY': self.api_key, 'SYMBOL': self.symbol, 'STRATEGY': self.strategy, 'IS_CLOSE': 1}).sort('TIME', -1).limit(1))

    def getOpenPosition(self):
        db = self.mongo._transactionConn()
        return list(db.find({'API_KEY': self.api_key, 'SYMBOL': self.symbol, 'STRATEGY': self.strategy, 'IS_CLOSE': 0}).sort('TIME', -1).limit(1))

    def getTrend(self):
        db = self.mongo._trendConn()
        return str(list(db.find({'SYMBOL': self.symbol}))[-1]['TREND'])

    def insertLastTradeData(self, time):
        db = self.mongo._lastTradeConn()
        data = {
            'API_KEY': self.api_key,
            'SYMBOL': self.symbol,
            'STRATEGY': self.strategy,
            'IS_CLOSE': 1,
            'TIME': time
        }
        db.insert_one(data)

    def ATR(self, DF, n=14):
        df = DF.copy()
        df['H-L'] = df['HIGH'] - df['LOW']
        df['H-PC'] = abs(df['HIGH'] - df['CLOSE'].shift(1))
        df['L-PC'] = abs(df['LOW'] - df['CLOSE'].shift(1))
        df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1, skipna=False)
        df['ATR'] = df['TR'].ewm(span=n, min_periods=n).mean()
        return df['ATR']
