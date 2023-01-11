from flask_restful import Resource
from flask import request
from rq import Queue
from Application.Api.Service.WebsocketService import WebsocketService
from Base.ResponseCode import ResponseCode
from Base.Connector import RedisConnector


class WebsocketController(Resource):
    def __init__(self):
        self.redis_conn = RedisConnector().getConn()

    def post(self):
        '''
        Request for opening websocket connection
        Json format:
        {
            "exchange": "xxx",
            "symbol": "xxx",
        }
        '''
        data = request.get_json()
        if not data or data['exchange'] is None or data['symbol'] is None:
            return ResponseCode.BAD_REQUEST
        exchange = data['exchange']
        symbol = data['symbol']
        channel = 'websocket'
        queue = Queue(channel, connection=self.redis_conn)
        if exchange == 'Binance':
            queue.enqueue(WebsocketService.binanceWebsocket, symbol, job_timeout=-1)
        elif exchange == 'OKX':
            queue.enqueue(WebsocketService.okxWebsocket, job_timeout=-1)
        return ResponseCode.SUCCESS
