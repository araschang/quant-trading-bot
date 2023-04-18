import websocket
import requests
import pandas as pd
import json
import time
import threading
from Base.ConfigReader import Config
from Model.Service.MongoDBService import MongoDBService
from Base.Service.DiscordService import DiscordService


class Connector(object):
    def __init__(self):
        self.config = Config()

class WebsocketService(Connector):
    VALID_ACCOUNT_EVENT_TYPE = 'ORDER_TRADE_UPDATE'
    VALID_SYMBOL = ['BTCUSDT', 'ETHUSDT']

    def __init__(self):
        super().__init__()
        self.aras_api_key = self.config['Binance_Aras']['api_key']
        self.yuan_api_key = self.config['Binance_Yuan']['api_key']
        self.mongo = MongoDBService()
        self._livePriceConn = self.mongo._livePriceConn()
        self._accountConn = self.mongo._accountConn()
        self.discord = DiscordService()

    def binancePriceWebsocket(self, currency, timeframe):
        websocket.enableTrace(False)
        socket = f'wss://fstream.binance.com/ws/{currency}@kline_{timeframe}'
        ws = websocket.WebSocketApp(socket,
                                    on_message=self.binanceOnMessage,
                                    on_error=self.on_error,
                                    on_close=self.on_close,
                                    on_pong=self.on_pong)
        ws.run_forever(ping_interval=25, ping_timeout=10) # 25秒發一次ping，10秒沒收到pong就斷線，官方api文件說ping_interval設25秒可以避免斷線

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
            data = {
                'SYMBOL': [s],
                'TIME': [t],
                'OPEN': [o],
                'HIGH': [h],
                'LOW': [l],
                'CLOSE': [c],
                'VOLUME': [v],
            }
            df = pd.DataFrame(data)
            print(df)

            data_mongo = {
                'SYMBOL': s,
                'TIME': t,
                'OPEN': o,
                'HIGH': h,
                'LOW': l,
                'CLOSE': c,
                'VOLUME': v,
            }

            self._livePriceConn.insert_one(data_mongo)
            cursor = list(self._livePriceConn.find({'SYMBOL': s}))
            if len(cursor) > 1:
                self._livePriceConn.delete_one({'_id': cursor[0]['_id']})

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
            time = int(data['E'])
            symbol = data['o']['s']
            side = data['o']['S']
            quantity = float(data['o']['q'])
            price = float(data['o']['ap'])
            order_type = data['o']['o']
            order_status = data['o']['X']
            orderId = int(data['o']['i'])

            if order_type == 'MARKET':
                db_has_no_same_doc = len(self._accountConn.find({'ORDER_ID': orderId})) == 0
                is_new_position = len(self._accountConn.find({'API_KEY': api_key, 'SYMBOL': symbol, 'IS_CLOSE': 0})) == 0
                if db_has_no_same_doc and is_new_position:
                    data_mongo = {
                        'API_KEY': api_key,
                        'TIME': time,
                        'SYMBOL': symbol,
                        'SIDE': side,
                        'QUANTITY': quantity,
                        'PRICE': price,
                        'ORDER_TYPE': order_type,
                        'ORDER_STATUS': order_status,
                        'ORDER_ID': orderId,
                        'STOPLOSS_STAGE': 0,
                        'CLOSE_PRICE': 0,
                        'IS_CLOSE': 0,
                    }
                    self._accountConn.insert_one(data_mongo)
                    if api_key == self.aras_api_key:
                        name = 'Aras'
                    elif api_key == self.yuan_api_key:
                        name = 'Yuan'
                    self.discord.sendMessage(f'**{symbol}** {name} {side.upper()} {quantity} at {price}')
                elif db_has_no_same_doc and not is_new_position:
                    self._accountConn.update_one({'API_KEY': api_key, 'SYMBOL': symbol, 'IS_CLOSE': 0}, {'$set': {'CLOSE_PRICE': price, 'IS_CLOSE': 1}})
                    if api_key == self.aras_api_key:
                        name = 'Aras'
                    elif api_key == self.yuan_api_key:
                        name = 'Yuan'
                    self.discord.sendMessage(f'**{symbol}** {name} position closed at {price}')

    def createAccountOnMessage(self, api_key):
        def wrapped_on_message(ws, message):
            self.binanceAccountOnMessage(ws, message, api_key)
        return wrapped_on_message

    def on_error(self, ws, error):
        print(error)

    def on_close(self, close_msg, close_status_code, close_status):
        print("### closed ###" + close_msg)

    def on_pong(self, wsapp, message):
        print("Got a pong! No need to respond")
