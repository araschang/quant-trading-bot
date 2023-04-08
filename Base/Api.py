from flask_restful import Api
from flask import Flask
from Model.Service.WebsocketService import WebsocketService
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

import logging

logging.basicConfig(filename='modellog.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
scheduler = BackgroundScheduler(job_defaults={'max_instances': 2})

app = Flask(__name__)
api = Api(app)

def binance_btc_websocket():
    try:
        websocket = WebsocketService()
        websocket.binanceWebsocket('btcusdt', '3m')
    except Exception as e:
        logging.error('An error occurred: %s', e, exc_info=True)
        print(e)

scheduler.add_job(binance_btc_websocket, 'interval', hours=24, next_run_time=datetime.now(), max_instances=2)
scheduler.start()