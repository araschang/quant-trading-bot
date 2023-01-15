import requests
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import json
import pandas as pd
from rq import Queue
import time
from datetime import datetime
from Base.Connector import RedisConnector, MongoConnector
from Base.ConfigReader import Config
from Base.ResponseCode import ResponseCode

class Connector(object):
    def __init__(self):
        self.config = Config()


class CryptoEX(Connector):
    def __init__(self):
        super().__init__()
        self.redis = RedisConnector().getConn()
        self.queue = Queue(connection=self.redis)
    
    @classmethod
    def run(self):
        while True:
            df = self.getBinanceData()
            index = self.calculateCryptoEX(df)
            self.send2Mongo(index)
            data = [[round(datetime.now().timestamp()), index]]
            data = pd.DataFrame(data, columns=['time', 'index'] , index=[0])
            csv = pd.read_csv('./Application/Api/Service/LivePrice/CryptoEX.csv')
            csv = pd.concat([csv, data], axis=0, ignore_index=True)
            csv.to_csv('./Application/Api/Service/LivePrice/CryptoEX.csv', index=False)
            print(index)
            time.sleep(60)
    
    @classmethod
    def getBinanceData(self):
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        data = session.get('https://www.binance.com/exchange-api/v2/public/asset-service/product/get-products', timeout=3)
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
    
    @classmethod
    def calculateCryptoEX(self, df):
        coinmarketcap = df['marketcap'].sum()
        df['marketcap_ratio'] = df['marketcap'] / coinmarketcap
        df['index'] = df['close'] * df['marketcap_ratio']
        index = round(df['index'].sum(), 2)
        return index
    
    @classmethod
    def send2Mongo(self, index):
        mongo = MongoConnector().getExConn()
        data = {
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'index': index
        }
        mongo.insert_one(data)
        return ResponseCode.SUCCESS



if __name__ == '__main__':
    cryptoex = CryptoEX()
    print(cryptoex.run())