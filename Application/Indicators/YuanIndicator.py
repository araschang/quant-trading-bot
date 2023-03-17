import pandas as pd
import ccxt
import os
from Base.ConfigReader import Config
from Application.Api.Service.DiscordService import DiscordService

class Connector(object):
    def __init__(self):
        self.config = Config()

class YuanIndicator(Connector):
    def __init__(self, symbol):
        super().__init__()
        self.symbol = symbol
        self.discord = DiscordService()
        config = self.config['Binance']
        self.exchange = ccxt.binanceusdm({
            'apiKey': config['api_key'],
            'secret': config['api_secret'],
            'enableRateLimit': True,
            'option': {
                'defaultMarket': 'future',
            },
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
        symbol = self.symbol.split('/')
        symbol = symbol[0] + symbol[1]
        if ohlcv_df['volume'].iloc[-1] >= mean_volume * 8:
            slope = ohlcv_df['close'].iloc[-1] - ohlcv_df['close'].iloc[-10]
            trend = pd.read_csv(os.path.join(os.path.dirname(__file__), 'YuanTrend.csv')).iloc[0, 0]
            if slope < 0 and trend == 'up':
                try:
                    check_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                except:
                    ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                    check_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))

                if str(check_df['time'].iloc[-1]) != str(ohlcv_df['time'].iloc[-1]):
                    ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                    self.discord.sendMessage(f'**{symbol}** BUY!')
                    return 'buy'
                else:
                    return ''
            elif slope > 0 and trend == 'down':
                try:
                    check_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                except:
                    ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                    check_df = pd.read_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))

                if str(check_df['time'].iloc[-1]) != str(ohlcv_df['time'].iloc[-1]):
                    ohlcv_df.to_csv(os.path.join(os.path.dirname(__file__), f'Yuan{symbol}.csv'))
                    self.discord.sendMessage(f'**{symbol}** SELL!')
                    return 'sell'
                else:
                    return ''
    
    def openPosition(self, side, amount, leverage, now_price, stoplossMny, apikey, apisecret):
        '''
        Open position
        Return a dataframe with open position
        '''
        if side != 'buy' and side != 'sell':
            return
        exchange = ccxt.binanceusdm({
            'apiKey': apikey,
            'secret': apisecret,
            'enableRateLimit': True,
            'option': {
                'defaultMarket': 'future',
            },
        })
        exchange.set_leverage(leverage, self.symbol)
        exchange.create_market_order(self.symbol, side, amount)
        if side == 'buy':
            side = 'sell'
        else:
            side = 'buy'

        if self.symbol == 'BTC/USDT':
            round_digit = 1
        elif self.symbol == 'ETH/USDT':
            round_digit = 2
        if side == 'buy':
            stop_loss_price = round(now_price - (stoplossMny / amount), round_digit)
        else:
            stop_loss_price = round(now_price + (stoplossMny / amount), round_digit)
        exchange.create_market_order(self.symbol, side, amount, params={'stopLossPrice': stop_loss_price, 'closePosition': True})
    
    def changeStopLoss(self, price, apikey, apisecret):
        '''
        Change stop loss
        Return a dataframe with change stop loss
        '''
        exchange = ccxt.binanceusdm({
            'apiKey': apikey,
            'secret': apisecret,
            'enableRateLimit': True,
            'option': {
                'defaultMarket': 'future',
            },
        })
        orderId = exchange.fetch_open_orders(self.symbol)[0]['info']['orderId']
        exchange.cancel_order(orderId, self.symbol)
        exchange.create_market_order(self.symbol, 'sell', 1, params={'stopLossPrice': price, 'closePosition': True})
    
    def checkIfChangeStopLoss(self, apikey, apisecret):
        pass

    def closePosition(self, apikey, apisecret):
        '''
        Close position
        Return a dataframe with close position
        '''
        exchange = ccxt.binanceusdm({
            'apiKey': apikey,
            'secret': apisecret,
            'enableRateLimit': True,
            'option': {
                'defaultMarket': 'future',
            },
        })
        amount = float(exchange.fetch_positions([str(self.symbol)])[0]['info']['positionAmt'])
        if amount > 0:
            side = 'sell'
        else:
            side = 'buy'
        exchange.create_market_order(self.symbol, side, amount)
    
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