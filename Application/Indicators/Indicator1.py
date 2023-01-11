from rq import Queue
import pandas as pd
import time
import sys
sys.path.append('./')
from Base.Connector import RedisConnector
from Application.Api.Service.StrategyService import StrategyService
from Application.Api.Service.DiscordService import DiscordService


class Indicator1(object):
    def __init__(self):
        self.redis = RedisConnector().getConn()
        self.queue = Queue(connection=self.redis)
    
    def run(self):
        discord = DiscordService()
        check = 0
        while True:
            strategy = StrategyService('BTC/USDT')
            ohlcv = strategy.getOHLCV('1mon', '1h')
            ohlcv = strategy.generateUpperBoundAndLowerBound(ohlcv)
            price_now = pd.read_csv('./Application/Api/Service/LivePrice/binance_btc.csv')['close'].iloc[0]
            if price_now > ohlcv['upper_bound'].iloc[-1] and check == 0:
                check = 1
                discord.sendMessage('Breakthough the upperbound, BUY SIGNAL!')
            elif price_now < ohlcv['lower_bound'].iloc[-1] and check == 0:
                check = 1
                discord.sendMessage('Breakthough the lowerbound, SELL SIGNAL!')
            else:
                check = 0
            print(ohlcv)
            time.sleep(1)


if __name__ == '__main__':
    indicator = Indicator1()
    indicator.run()
