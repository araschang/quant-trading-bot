from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
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

def error_handler(job_id, exception):
    print(f'Error in job {job_id}: {exception}')
    scheduler.add_job(safe_run, 'date', id=job_id, run_date=datetime.now(), args=[job_id])

def safe_run(job_id):
    try:
        globals()[job_id]()
    except Exception as e:
        error_handler(job_id, e)

def stable_check():
    webhook = DiscordService()
    webhook.stableCheck()

### to-do: add ohlcv to mongodb
job1_id = 'binance_btc_websocket'
job2_id = 'binance_eth_websocket'
scheduler.add_job(safe_run, 'date', id=job1_id, run_date=datetime.now(), args=[job1_id])
scheduler.add_job(safe_run, 'date', id=job2_id, run_date=datetime.now(), args=[job2_id])
scheduler.add_job(stable_check, 'interval', hours=8)
scheduler.start()