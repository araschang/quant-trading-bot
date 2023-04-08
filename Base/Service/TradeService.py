import ccxt
from Base.ConfigReader import Config


class Connector(object):
    def __init__(self):
        self.config = Config()


class TradeService(Connector):
    def __init__(self, exchange, symbol, timeframe):
        super().__init__()
        if exchange == 'Binance':
            config = self.config['Binance']
            self.exchange = ccxt.binanceusdm({
                'apiKey': config['api_key'],
                'secret': config['api_secret'],
                'enableRateLimit': True,
                'option': {
                    'defaultMarket': 'future',
                },
            })
        elif exchange == 'OKX':
            config = self.config['OKX']
            self.exchange = ccxt.okx({
                'apiKey': config['api_key'],
                'secret': config['api_secret'],
                'password': config['pass_phrase'],
            })
        self.symbol = symbol
        self.timeframe = timeframe

    def createOrder(self, side, amount, price):
        '''
        Create an order
        '''
        order = self.exchange.create_order(self.symbol, 'limit', side, amount, price)
        return order
    
    def getOrderID(self, order):
        '''
        Get order id
        '''
        return order['info']['orderId']

    def cancelOrder(self, order_id):
        '''
        Cancel an order
        '''
        self.exchange.cancel_order(order_id)