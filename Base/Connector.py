import ccxt
from redis import Redis, ConnectionPool
from Base.ConfigReader import Config

class Connector(object):
    def __init__(self):
        self.config = Config()


class RedisConnector(Connector):
    def __init__(self):
        super().__init__()
        config = self.config['Redis']
        host = config["HOST"]
        pool = ConnectionPool(host=host)
        self._redisConnection = Redis(connection_pool=pool)

    def getConn(self):
        return self._redisConnection


class BinanceConnector(Connector):
    def __init__(self):
        super().__init__()
        config = self.config['Binance']
        api_key = config['api_key']
        api_secret = config['api_secret']
        self._binanceConnection = ccxt.binanceusdm({
            'apiKey': config['api_key'],
            'secret': config['api_secret'],
            'enableRateLimit': True,
            'option': {
                'defaultMarket': 'future',
            },
        })
    
    def getConn(self):
        return self._binanceConnection


class OKXConnector(Connector):
    def __init__(self):
        super().__init__()
        config = self.config['OKX']
        self._okxConnection = ccxt.okx({
            'apiKey': config['api_key'],
            'secret': config['api_secret'],
            'password': config['pass_phrase'],
        })
    
    def getConn(self):
        return self._okxConnection


class DiscordConnector(Connector):
    def __init__(self):
        super().__init__()
        config = self.config['Discord']
        self._discordConnection = config['webhook_url']
    
    def getConn(self):
        return self._discordConnection
