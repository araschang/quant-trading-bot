import ccxt
import pandas as pd
from datetime import datetime, timedelta
from Base.ConfigReader import Config


class Connector(object):
    def __init__(self):
        self.config = Config()


class StrategyService(Connector):
    def __init__(self, symbol):
        super().__init__()
        config = self.config['Binance']
        self.exchange = ccxt.binanceusdm({
            'apiKey': config['api_key'],
            'secret': config['api_secret'],
            'enableRateLimit': True,
            'option': {
                'defaultMarket': 'future',
            },
        })
        self.symbol = symbol
        self.trading_fee = 0.001
    
    def getOHLCV(self, time_interval, timeframe):
        '''
        Get OHLCV data from exchange
        Time interval: 1mon, 3mon, 6mon, 1y
        Return a dataframe with OHLCV data
        '''
        end = datetime.timestamp(datetime.now())
        if time_interval == '1mon':
            start = datetime.timestamp(datetime.now() - timedelta(days=30))
        elif time_interval == '3mon':
            start = datetime.timestamp(datetime.now() - timedelta(days=90))
        elif time_interval == '6mon':
            start = datetime.timestamp(datetime.now() - timedelta(days=180))
        elif time_interval == '1y':
            start = datetime.timestamp(datetime.now() - timedelta(days=365))
        ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe, start, end)
        df = pd.DataFrame(ohlcv, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df['time'] = pd.to_datetime(df['time'], unit='ms')
        return df
    
    def generateUpperBoundAndLowerBound(self, ohlcv_df):
        '''
        Generate upper bound and lower bound
        Return a dataframe with upper bound and lower bound
        '''
        # method 1: use std (close)
        # ohlcv_df['upper_bound'] = ohlcv_df['close'] + 2 * ohlcv_df['close'].rolling(20).std()
        # ohlcv_df['lower_bound'] = ohlcv_df['close'] - 2 * ohlcv_df['close'].rolling(20).std()
        # method 2: use rolling mean and std (high, low)
        ohlcv_df['upper_bound'] = ohlcv_df['high'].rolling(20).mean() + 2 * ohlcv_df['high'].rolling(20).std()
        ohlcv_df['lower_bound'] = ohlcv_df['low'].rolling(20).mean() - 2 * ohlcv_df['low'].rolling(20).std()
        ohlcv_df['std'] = ohlcv_df['close'].rolling(20).std()
        ohlcv_df.dropna(inplace=True)
        ohlcv_df.reset_index(drop=True, inplace=True)
        return ohlcv_df

    def generateGridQty(self, ohlcv_df):
        '''
        Generate grid quantity.
        Need to be improved.
        '''
        grid_qty = 0
        for i in range(len(ohlcv_df)):
            if ohlcv_df['upper_bound'].iloc[i] - ohlcv_df['lower_bound'].iloc[i] > 0.5 * ohlcv_df['std'].iloc[i]:
                grid_qty += 1
        return grid_qty

    def generateGrid(self, ohlcv_df, grid_qty):
        '''
        Generate grid
        Return a list of grid price.
        '''
        grid = []
        grid_interval = (ohlcv_df['upper_bound'].iloc[-1] - ohlcv_df['lower_bound'].iloc[-1]) / grid_qty
        for i in range(grid_qty):
            grid.append(ohlcv_df['lower_bound'] + grid_interval * i)
        return grid

