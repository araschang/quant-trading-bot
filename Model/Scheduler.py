from apscheduler.schedulers.background import BackgroundScheduler
from Model.Service.WebsocketService import WebsocketService
from Base.Service.DiscordService import DiscordService
from datetime import datetime, timedelta
from Model.Service.MongoDBService import MongoDBService
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
    webhook.stableCheck('All jobs are running normally.')

def db_check():
    webhook = DiscordService()
    mongo = MongoDBService()
    db1 = mongo._livePriceConn().count_documents({})
    db2 = mongo._memberInfoConn().count_documents({})
    db3 = mongo._lastTradeConn().count_documents({})
    db4 = mongo._transactionConn().count_documents({})
    db5 = mongo._lastSignalConn().count_documents({})
    db_lst = [db1, db2, db3, db4, db5]
    for i in range(len(db_lst)):
        if db_lst[i] >= 10:
            webhook.stableCheck(f'db{i+1} is unstable, please check it.')

### to-do: add ohlcv to mongodb
job1_id = 'binance_btc_websocket'
job2_id = 'binance_eth_websocket'
scheduler.add_job(safe_run, 'date', id=job1_id, run_date=datetime.now(), args=[job1_id])
scheduler.add_job(safe_run, 'date', id=job2_id, run_date=datetime.now(), args=[job2_id])
scheduler.add_job(stable_check, 'interval', hours=8, next_run_time=datetime.now()+timedelta(seconds=10))
scheduler.start()