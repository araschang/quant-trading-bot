import websocket
import requests
import json
import time
import threading
import sys
sys.path.append('/Users/araschang/Desktop/coding/quant-station')
from Base.ConfigReader import Config
from Model.Service.MongoDBService import MongoDBService
from Base.Service.DiscordService import DiscordService
import logging
logging.basicConfig(filename='quantlog.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


class Connector(object):
    def __init__(self):
        self.config = Config()

class WebsocketService(Connector):
    VALID_ACCOUNT_EVENT_TYPE = 'ORDER_TRADE_UPDATE'
    VALID_SYMBOL = ['ETHUSDT']
    CONTRACT_MARKET = ['BTCUSDT', 'ETHUSDT', 'BCHUSDT', 'XRPUSDT', 'EOSUSDT', 'LTCUSDT', 'TRXUSDT', 'ETCUSDT', 'LINKUSDT', 'XLMUSDT', 'ADAUSDT', 'XMRUSDT', 'DASHUSDT', 'ZECUSDT', 'XTZUSDT', 'BNBUSDT', 'ATOMUSDT', 'ONTUSDT', 'IOTAUSDT', 'BATUSDT', 'VETUSDT', 'NEOUSDT', 'QTUMUSDT', 'IOSTUSDT', 'THETAUSDT', 'ALGOUSDT', 'ZILUSDT', 'KNCUSDT', 'ZRXUSDT', 'COMPUSDT', 'OMGUSDT', 'DOGEUSDT', 'SXPUSDT', 'KAVAUSDT', 'BANDUSDT', 'RLCUSDT', 'WAVESUSDT', 'MKRUSDT', 'SNXUSDT', 'DOTUSDT', 'DEFIUSDT', 'YFIUSDT', 'BALUSDT', 'CRVUSDT', 'TRBUSDT', 'RUNEUSDT', 'SUSHIUSDT', 'SRMUSDT', 'EGLDUSDT', 'SOLUSDT', 'ICXUSDT', 'STORJUSDT', 'BLZUSDT', 'UNIUSDT', 'AVAXUSDT', 'FTMUSDT', 'HNTUSDT', 'ENJUSDT', 'FLMUSDT', 'TOMOUSDT', 'RENUSDT', 'KSMUSDT', 'NEARUSDT', 'AAVEUSDT', 'FILUSDT', 'RSRUSDT', 'LRCUSDT', 'MATICUSDT', 'OCEANUSDT', 'CVCUSDT', 'BELUSDT', 'CTKUSDT', 'AXSUSDT', 'ALPHAUSDT', 'ZENUSDT', 'SKLUSDT', 'GRTUSDT', '1INCHUSDT', 'BTCBUSD', 'CHZUSDT', 'SANDUSDT', 'ANKRUSDT', 'BTSUSDT', 'LITUSDT', 'UNFIUSDT', 'REEFUSDT', 'RVNUSDT', 'SFPUSDT', 'XEMUSDT', 'BTCSTUSDT', 'COTIUSDT', 'CHRUSDT', 'MANAUSDT', 'ALICEUSDT', 'HBARUSDT', 'ONEUSDT', 'LINAUSDT', 'STMXUSDT', 'DENTUSDT', 'CELRUSDT', 'HOTUSDT', 'MTLUSDT', 'OGNUSDT', 'NKNUSDT', 'SCUSDT', 'DGBUSDT', '1000SHIBUSDT', 'BAKEUSDT', 'GTCUSDT', 'ETHBUSD', 'BTCDOMUSDT', 'BNBBUSD', 'ADABUSD', 'XRPBUSD', 'IOTXUSDT', 'DOGEBUSD', 'AUDIOUSDT', 'RAYUSDT', 'C98USDT', 'MASKUSDT', 'ATAUSDT', 'SOLBUSD', 'FTTBUSD', 'DYDXUSDT', '1000XECUSDT', 'GALAUSDT', 'CELOUSDT', 'ARUSDT', 'KLAYUSDT', 'ARPAUSDT', 'CTSIUSDT', 'LPTUSDT', 'ENSUSDT', 'PEOPLEUSDT', 'ANTUSDT', 'ROSEUSDT', 'DUSKUSDT', 'FLOWUSDT', 'IMXUSDT', 'API3USDT', 'GMTUSDT', 'APEUSDT', 'WOOUSDT', 'FTTUSDT', 'JASMYUSDT', 'DARUSDT', 'GALUSDT', 'AVAXBUSD', 'NEARBUSD', 'GMTBUSD', 'APEBUSD', 'GALBUSD', 'FTMBUSD', 'DODOBUSD', 'ANCBUSD', 'GALABUSD', 'TRXBUSD', '1000LUNCBUSD', 'OPUSDT', 'DOTBUSD', 'TLMBUSD', 'WAVESBUSD', 'LINKBUSD', 'SANDBUSD', 'LTCBUSD', 'MATICBUSD', 'CVXBUSD', 'FILBUSD', '1000SHIBBUSD', 'LEVERBUSD', 'ETCBUSD', 'LDOBUSD', 'UNIBUSD', 'AUCTIONBUSD', 'INJUSDT', 'STGUSDT', 'FOOTBALLUSDT', 'SPELLUSDT', '1000LUNCUSDT', 'LUNA2USDT', 'AMBBUSD', 'PHBBUSD', 'LDOUSDT', 'CVXUSDT', 'ICPUSDT', 'APTUSDT', 'QNTUSDT', 'APTBUSD', 'BLUEBIRDUSDT', 'FETUSDT', 'AGIXBUSD', 'FXSUSDT', 'HOOKUSDT', 'MAGICUSDT', 'TUSDT', 'RNDRUSDT', 'HIGHUSDT', 'MINAUSDT', 'ASTRUSDT', 'AGIXUSDT', 'PHBUSDT', 'GMXUSDT', 'CFXUSDT', 'STXUSDT', 'COCOSUSDT', 'BNXUSDT', 'ACHUSDT', 'SSVUSDT', 'CKBUSDT', 'PERPUSDT', 'TRUUSDT', 'LQTYUSDT', 'USDCUSDT', 'IDUSDT', 'ARBUSDT', 'JOEUSDT', 'TLMUSDT', 'AMBUSDT', 'LEVERUSDT', 'RDNTUSDT', 'HFTUSDT', 'XVSUSDT']

    def __init__(self):
        super().__init__()
        self.aras_api_key = self.config['Binance_Aras']['api_key']
        self.yuan_api_key = self.config['Binance_Yuan']['api_key']
        self.mongo = MongoDBService()
        self._livePriceConn = self.mongo._livePriceConn()
        self._transactionConn = self.mongo._transactionConn()
        self._allMarketConn = self.mongo._allMarketConn()
        self.discord = DiscordService()

    def binancePriceWebsocket(self, currency, timeframe):
        websocket.enableTrace(False)
        socket = f'wss://fstream.binance.com/ws/{currency}@kline_{timeframe}'
        ws = websocket.WebSocketApp(socket,
                                    on_message=self.binanceOnMessage,
                                    on_error=self.on_error,
                                    on_close=self.on_close,
                                    on_pong=self.on_pong)
        ws.run_forever(ping_interval=25, ping_timeout=10)

    def binanceOnMessage(self, ws, message):
        try:
            json_result = json.loads(message)
            s = json_result['s']
            t = int(json_result['k']['t'])
            o = float(json_result['k']['o'])
            l = float(json_result['k']['l'])
            h = float(json_result['k']['h'])
            c = float(json_result['k']['c'])
            v = float(json_result['k']['v'])

            data_mongo = {
                'SYMBOL': s,
                'TIME': t,
                'OPEN': o,
                'HIGH': h,
                'LOW': l,
                'CLOSE': c,
                'VOLUME': v,
            }

            self._livePriceConn.update_one({'SYMBOL': s}, {'$set': data_mongo}, upsert=True)
            print(data_mongo)
        except Exception as e:
            print(e)

    def binanceAccountWebsocket(self, api_key):
        websocket.enableTrace(False)
        listen_key = self.getBinanceListenKey(api_key)
        socket = f'wss://fstream.binance.com/ws/{listen_key}'
        ws = websocket.WebSocketApp(socket,
                                    on_message=self.createAccountOnMessage(api_key),
                                    on_error=self.on_error,
                                    on_close=self.on_close,
                                    on_pong=self.on_pong)
        ping_thread = threading.Thread(target=self.binanceAccountPing, args=(ws,))
        ping_thread.daemon = True
        ping_thread.start()
        ws.run_forever()

    def getBinanceListenKey(self, api_key):
        base_url = 'https://fapi.binance.com'
        endpoint = '/fapi/v1/listenKey'
        url = base_url + endpoint
        headers = {
            'X-MBX-APIKEY': api_key
        }
        response = requests.post(url, headers=headers)
        return response.json()['listenKey']

    def binanceAccountPing(self, ws):
        while ws.keep_running:
            time.sleep(3000)
            base_url = 'https://fapi.binance.com'
            endpoint = f'/fapi/v1/listenKey'
            url = base_url + endpoint
            response = requests.put(url)

    def binanceAccountOnMessage(self, ws, message, api_key):
        data = json.loads(message)
        if data['e'] == self.VALID_ACCOUNT_EVENT_TYPE and data['o']['s'] in self.VALID_SYMBOL:
            symbol = data['o']['s']
            side = data['o']['S']
            price = float(data['o']['ap'])
            order_type = data['o']['o']
            order_status = data['o']['X']

            if side == 'BUY':
                post_order_side_should_be = 'sell'
            elif side == 'SELL':
                post_order_side_should_be = 'buy'

            if order_type == 'MARKET' and order_status == 'FILLED':
                transaction = list(self._transactionConn.find({'API_KEY': api_key, 'SYMBOL': symbol, 'IS_CLOSE': 0}).sort('TIME', -1).limit(1))
                if (len(transaction) != 0) and (transaction[0]['SIDE'] == post_order_side_should_be): # has open position
                    logging.debug(f'Account info {api_key} {symbol} {side} {price} {order_type} {order_status}')
                    self._transactionConn.update_one({'API_KEY': api_key, 'SYMBOL': symbol, 'IS_CLOSE': 0}, {'$set': {'CLOSE_PRICE': price, 'IS_CLOSE': 1}})
                    if api_key == self.aras_api_key:
                        name = 'Aras'
                    elif api_key == self.yuan_api_key:
                        name = 'Yuan'
                    self.discord.sendMessage(f'**{symbol}** {name} position closed at {price}')

    def createAccountOnMessage(self, api_key):
        def wrapped_on_message(ws, message):
            self.binanceAccountOnMessage(ws, message, api_key)
        return wrapped_on_message

    def binanceAllMarketWebsocket(self):
        websocket.enableTrace(False)
        socket = 'wss://stream.binance.com:9443/ws/!ticker_1h@arr'
        ws = websocket.WebSocketApp(socket,
                                    on_message=self.binanceAllMarketOnMessage,
                                    on_error=self.on_error,
                                    on_close=self.on_close,
                                    on_pong=self.on_pong)
        ws.run_forever(ping_interval=25, ping_timeout=10)

    def binanceAllMarketOnMessage(self, ws, message):
        datas = json.loads(message)
        for data in datas:
            if data['s'] in self.CONTRACT_MARKET:
                data_mongo = {
                    'SYMBOL': data['s'],
                    'TIME': int(data['E']),
                    'OPEN': float(data['o']),
                    'HIGH': float(data['h']),
                    'LOW': float(data['l']),
                    'CLOSE': float(data['c']),
                    'VOLUME': float(data['v']),
                    'PCT_CHANGE': float(data['P']),
                }

                self._allMarketConn.update_one({'SYMBOL': data['s']}, {'$set': data_mongo}, upsert=True)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, close_msg, close_status_code, close_status):
        print("### closed ###" + close_msg)

    def on_pong(self, wsapp, message):
        print("Got a pong! No need to respond")

if __name__ == '__main__':
    web = WebsocketService()
    web.binanceAccountWebsocket()