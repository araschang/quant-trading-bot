import pandas as pd
import ccxt
import os
from Base.ConfigReader import Config
from Application.Api.Service.DiscordService import DiscordService

class Connector(object):
    def __init__(self):
        self.config = Config()

class YuanIndicator(Connector):
    def __init__(self, symbol, exchange, api_key=None, api_secret=None):
        super().__init__()
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.exchange_name = exchange
        self.discord = DiscordService()
        config = self.config['Binance']
        if exchange == 'binance':
            self.exchange = ccxt.binanceusdm({
                'apiKey': config['api_key'],
                'secret': config['api_secret'],
                'enableRateLimit': True,
                'option': {
                    'defaultMarket': 'future',
                },
            })
        elif exchange == 'bybit':
            self.exchange = ccxt.bybit({
            'apiKey': self.api_key,
            'secret': self.api_secret,
        })

    def getOHLCV(self, timeframe):
        '''
        Get OHLCV data from exchange
        Return a dataframe with OHLCV data
        '''
        ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe, limit=100)
        df = pd.DataFrame(ohlcv, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        return df
    
    def cleanData2GenerateMeanVolume(self, ohlcv_df):
        '''
        Clean data to generate mean volume
        Return a dataframe with mean volume
        '''
        volume = ohlcv_df['volume'].astype(float)
        Q1 = volume.quantile(0.25)
        Q3 = volume.quantile(0.75)
        IQR = Q3 - Q1
        volume_mean = volume[(volume >= Q1 - 1.5 * IQR) & (volume <= Q3 + 1.5 * IQR)].mean()
        return volume_mean
    
    def checkSignal(self, mean_volume, ohlcv_df):
        '''
        Check signal
        Return a dataframe with signal
        '''
        if self.exchange_name == 'binance':
            symbol = self.symbol.split('/')
            symbol = symbol[0] + symbol[1]
        elif self.exchange_name == 'bybit':
            symbol = self.symbol

        if ohlcv_df['volume'].iloc[-1] >= mean_volume * 8:
            slope = ohlcv_df['close'].iloc[-1] - ohlcv_df['close'].iloc[-10]
            # trend = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTrend.csv')).iloc[0, 0]
            try:
                check_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
            except Exception as e:
                ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                check_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
            # if slope <= 0 and trend == 'up':
            if slope <= 0:
                if str(check_df['time'].iloc[-1]) != str(ohlcv_df['time'].iloc[-1]):
                    ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                    self.discord.sendMessage(f'**{symbol}** BUY!')
                    return 'buy'
                else:
                    return ''
            # elif slope > 0 and trend == 'down':
            else:
                if str(check_df['time'].iloc[-1]) != str(ohlcv_df['time'].iloc[-1]):
                    ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                    self.discord.sendMessage(f'**{symbol}** SELL!')
                    return 'sell'
                else:
                    return ''
    
    def openPosition(self, side, amount, leverage: int, now_price: float, stoplossMny) -> None:
        '''
        Open position
        Return a dataframe with open position
        '''
        if side != 'buy' and side != 'sell':
            return
        try: # bybit will raise error if leverage is the same
            self.exchange.set_leverage(leverage, self.symbol)
        except:
            pass
        self.exchange.create_market_order(self.symbol, side, amount)

        if self.exchange_name == 'binance':
            if self.symbol == 'BTC/USDT':
                round_digit = 1
            elif self.symbol == 'ETH/USDT':
                round_digit = 2
        elif self.exchange_name == 'bybit':
            if self.symbol == 'BTCUSDT':
                round_digit = 1
            elif self.symbol == 'ETHUSDT':
                round_digit = 2
        
        # stop_loss_price = round(now_price - ((amount * now_price / leverage) * 0.8 / amount), round_digit) 是否改用？
        if side == 'buy':
            stop_loss_side = 'sell'
            stop_loss_price = round(now_price - (stoplossMny / amount), round_digit)
        else:
            stop_loss_side = 'buy'
            stop_loss_price = round(now_price + (stoplossMny / amount), round_digit)
            
        try:
            if self.exchange_name == 'binance':
                self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price, 'closePosition': True})
            elif self.exchange_name == 'bybit':
                if stop_loss_side == 'buy':
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'takeProfitPrice': stop_loss_price})
                else:
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price})
            return 'Open position success'
        except Exception as e:
            now_price = float(self.getOHLCV('3m')['close'].iloc[-1])
            if side == 'buy':
                stop_loss_price = round(now_price - (stoplossMny / amount), round_digit)
            else:
                stop_loss_price = round(now_price + (stoplossMny / amount), round_digit)
            
            if self.exchange_name == 'binance':
                self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price, 'closePosition': True})
            elif self.exchange_name == 'bybit':
                if stop_loss_side == 'buy':
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'takeProfitPrice': stop_loss_price})
                else:
                    self.exchange.create_market_order(self.symbol, stop_loss_side, amount, params={'stopLossPrice': stop_loss_price})
            return 'Open position success'
    
    def changeStopLoss(self, price):
        '''
        Change stop loss
        Return a dataframe with change stop loss
        '''
        # bybit
        # binance need to be added
        order_info = self.exchange.fetch_open_orders(self.symbol)[0]
        orderId = order_info['info']['orderId']
        side = order_info['info']['side']
        if side == 'Buy':
            stop_loss_side = 'buy'
        else:
            stop_loss_side = 'sell'
        self.exchange.cancel_order(orderId, self.symbol)
        self.exchange.create_market_order(self.symbol, stop_loss_side, 1, params={'stopLossPrice': price, 'closePosition': True})
        
    def checkIfThereIsStopLoss(self):
        if self.exchange_name == 'binance':
            position = self.exchange.fetch_positions([str(self.symbol)])
            has_position = position[0]['info']['positionAmt']
            if float(has_position) != 0: # if there is position
                if len(self.exchange.fetch_open_orders(self.symbol)) == 0: # if there is no stop loss
                    ohlcv = self.getOHLCV('3m')
                    if position[0]['info']['positionAmt'] > 0: # if position is long
                        low_price_now = float(ohlcv['low'].iloc[-1])
                        low_price_3min_ago = float(ohlcv['low'].iloc[-2])
                        if low_price_now < low_price_3min_ago:
                            stop_loss_price = low_price_now
                            stop_loss_side = 'sell'
                        else:
                            stop_loss_price = low_price_3min_ago
                            stop_loss_side = 'sell'
                    else: # if position is short
                        high_price_now = float(ohlcv['high'].iloc[-1])
                        high_price_3min_ago = float(ohlcv['high'].iloc[-2])
                        if high_price_now > high_price_3min_ago:
                            stop_loss_price = high_price_now
                            stop_loss_side = 'buy'
                        else:
                            stop_loss_price = high_price_3min_ago
                            stop_loss_side = 'buy'
                    self.exchange.create_market_order(self.symbol, stop_loss_side, 1, params={'stopLossPrice': stop_loss_price, 'closePosition': True})

        elif self.exchange_name == 'bybit':
            position = self.exchange.fetch_positions(self.symbol)
            if len(position) != 0: # if there is position,  need to be debugged!!!!!
                if len(self.exchange.fetch_open_orders(self.symbol)) == 0: # if there is no stop loss
                    ohlcv = self.getOHLCV('3m')
                    if position[0]['info']['side'] == 'Buy': # if position is long
                        low_price_now = float(ohlcv['low'].iloc[-1])
                        low_price_3min_ago = float(ohlcv['low'].iloc[-2])
                        if low_price_now < low_price_3min_ago:
                            stop_loss_price = low_price_now
                            stop_loss_side = 'sell'
                        else:
                            stop_loss_price = low_price_3min_ago
                            stop_loss_side = 'sell'
                    else: # if position is short
                        high_price_now = float(ohlcv['high'].iloc[-1])
                        high_price_3min_ago = float(ohlcv['high'].iloc[-2])
                        if high_price_now > high_price_3min_ago:
                            stop_loss_price = high_price_now
                            stop_loss_side = 'buy'
                        else:
                            stop_loss_price = high_price_3min_ago
                            stop_loss_side = 'buy'
                    
                    if stop_loss_side == 'buy':
                        self.exchange.create_market_order(self.symbol, stop_loss_side, 1, params={'takeProfitPrice': stop_loss_price})
                    else:
                        self.exchange.create_market_order(self.symbol, stop_loss_side, 1, params={'stopLossPrice': stop_loss_price})


    def closePosition(self):
        '''
        Close position
        Return a dataframe with close position
        '''
        if self.exchange_name == 'binance':
            amount = float(self.exchange.fetch_positions([str(self.symbol)])[0]['info']['positionAmt'])
            if amount > 0:
                side = 'sell'
            else:
                side = 'buy'
        elif self.exchange_name == 'bybit':
            amount = float(self.exchange.fetch_positions(self.symbol)[0]['info']['size'])
            side = self.exchange.fetch_positions(self.symbol)[0]['info']['side']
            if side == 'Buy':
                side = 'sell'
            else:
                side = 'buy'
        self.exchange.create_market_order(self.symbol, side, amount)
    
    def checkIfNoPositionCancelOpenOrder(self):
        '''
        Check if no position cancel open order
        Return a dataframe with check if no position cancel open order
        '''
        if self.exchange_name == 'binance':
            if len(self.exchange.fetch_positions([str(self.symbol)])) == 0:
                if len(self.exchange.fetch_open_orders(self.symbol)) != 0:
                    self.cancelOrder()
        elif self.exchange_name == 'bybit':
            if len(self.exchange.fetch_positions(self.symbol)) == 0:
                if len(self.exchange.fetch_open_orders(self.symbol)) != 0:
                    self.cancelOrder()

    def cancelOrder(self):
        '''
        Cancel order
        Return a dataframe with cancel order
        '''
        orderId = self.exchange.fetch_open_orders(self.symbol)[0]['info']['orderId']
        self.exchange.cancel_order(orderId, self.symbol)
    
    def checkTrend(self):
        '''
        Check trend
        Return a dataframe with trend
        '''
        ohlcv_df = self.getOHLCV('1h')
        slope = ohlcv_df['close'].iloc[-1] - ohlcv_df['close'].iloc[-10]
        if slope < 0:
            trend = pd.DataFrame({'trend': ['down']})
            trend.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTrend.csv'))
            return 'down'
        else:
            trend = pd.DataFrame({'trend': ['up']})
            trend.to_csv(os.path.join(os.path.dirname(__file__), 'YuanTrend.csv'))
            return 'up'

if __name__ == '__main__':
    print(os.path.join(os.path.dirname(__file__), 'YuanBTC.csv'))