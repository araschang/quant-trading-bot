from apscheduler.schedulers.background import BackgroundScheduler
from Model.Service.WebsocketService import WebsocketService
from Base.Service.DiscordService import DiscordService
from datetime import datetime
import logging

logging.basicConfig(filename='quantlog.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
scheduler = BackgroundScheduler(job_defaults={'max_instances': 2})

def binance_btc_websocket():
    try:
        websocket = WebsocketService()
        websocket.binanceWebsocket('btcusdt', '3m')
    except Exception as e:
        logging.error('An error occurred: %s', e, exc_info=True)
        print(e)

def binance_eth_websocket():
    try:
        websocket = WebsocketService()
        websocket.binanceWebsocket('ethusdt', '3m')
    except Exception as e:
        logging.error('An error occurred: %s', e, exc_info=True)
        print(e)

def stable_check():
    webhook = DiscordService()
    webhook.sendMessage('STABLE CHECK')

### to-do: add ohlcv to mongodb

scheduler.add_job(binance_btc_websocket, 'interval', hours=24, next_run_time=datetime.now(), max_instances=2)
scheduler.add_job(binance_eth_websocket, 'interval', hours=24, next_run_time=datetime.now(), max_instances=2)
scheduler.add_job(stable_check, 'interval', hours=8)
scheduler.start()