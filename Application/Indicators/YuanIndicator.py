import pandas as pd
import time
import ccxt
from Base.Connector import RedisConnector
from Base.ConfigReader import Config
from Application.Api.Service.StrategyService import StrategyService
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
        if ohlcv_df['volume'].iloc[-1] >= mean_volume * 8:
            slope = ohlcv_df['close'].iloc[-1] - ohlcv_df['close'].iloc[-10]
            if slope < 0:
                self.discord.sendMessage(f'**{self.symbol}** POSSIBLE BUY SIGNAL!')
            else:
                self.discord.sendMessage(f'**{self.symbol}** POSSIBLE SELL SIGNAL!')
