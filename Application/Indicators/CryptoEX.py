import requests
import json
import pandas as pd
from rq import Queue
import time
from datetime import datetime
from Base.Connector import RedisConnector


class CryptoEX(object):
    def __init__(self):
        self.redis = RedisConnector().getConn()
        self.queue = Queue(connection=self.redis)
    
    def run(self):
        while True:
            df = self.getBinanceData()
            index = self.calculateCryptoEX(df)
            data = [[datetime.now().strftime('%Y-%m-%d %H:%M:%S'), index]]
            data = pd.DataFrame(data, columns=['time', 'index'] , index=[0])
            csv = pd.read_csv('./Application/Api/Service/LivePrice/CryptoEX.csv')
            csv = pd.concat([csv, data], axis=0, ignore_index=True)
            csv.to_csv('./Application/Api/Service/LivePrice/CryptoEX.csv', index=False)
            print(index)
            time.sleep(60)
    
    def getBinanceData():
        data = requests.get('https://www.binance.com/exchange-api/v2/public/asset-service/product/get-products')
        data = json.loads(data.text)
        df = pd.DataFrame(data['data'])[['s', 'b', 'q', 'o', 'h', 'l', 'c', 'v', 'cs']]
        df = df[df['q'].str.contains('USDT')]
        df.columns = ['symbol', 'baseAsset', 'quoteAsset', 'open', 'high', 'low', 'close', 'volume', 'count']
        df['close'] = df['close'].astype(float)
        df['count'] = df['count'].astype(float)
        df['marketcap'] = df['close'] * df['count']
        df = df.sort_values(by=['marketcap'], ascending=False, ignore_index=True)
        df.drop(df[df['baseAsset'] == 'BUSD'].index, axis=0, inplace=True)
        df.dropna(axis=0, inplace=True)
        time.sleep(1)
        return df
    
    def calculateCryptoEX(df):
        coinmarketcap = df['marketcap'].sum()
        df['marketcap_ratio'] = df['marketcap'] / coinmarketcap
        df['index'] = df['close'] * df['marketcap_ratio']
        index = round(df['index'].sum(), 2)
        return index

if __name__ == '__main__':
    cryptoex = CryptoEX()
    print(cryptoex.run())