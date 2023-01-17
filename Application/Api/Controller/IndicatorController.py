from flask_restful import Resource
from flask import request
from rq import Queue
from Application.Indicators.Indicator1 import Indicator1
from Application.Indicators.CryptoEX import CryptoEX
from Base.ResponseCode import ResponseCode
from Base.Connector import RedisConnector


class IndicatorController(Resource):
    def __init__(self):
        self.redis_conn = RedisConnector().getConn()
    
    def post(self):
        '''
        Request for running indicator
        Json format:
        {
            "indicator": "xxx",
            "action": "xxx",
        }
        Valid action: start, stop
        '''
        data = request.get_json()
        channel = 'indicator'
        queue = Queue(channel, connection=self.redis_conn)
        if not data or data['indicator'] is None or data['action'] is None:
            return ResponseCode.BAD_REQUEST
        
        if data['indicator'] == 'ex' and data['action'] == 'start':
            queue.enqueue(CryptoEX.run, job_timeout=-1)
            return ResponseCode.SUCCESS
        elif data['indicator'] == 'ex' and data['action'] == 'draw':
            queue.enqueue(CryptoEX.drawCryptoEX, data['timeframe'])
            return ResponseCode.SUCCESS
        elif data['indicator'] == '1' and data['action'] == 'start':
            queue.enqueue(Indicator1.run, job_timeout=-1)
            return ResponseCode.SUCCESS
