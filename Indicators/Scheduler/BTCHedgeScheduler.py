from apscheduler.schedulers.background import BackgroundScheduler
from Base.ConfigReader import Config
from Indicators.BTCHedge import BTCHedge
from Model.Service.MongoDBService import MongoDBService
from datetime import datetime, timedelta
import logging

logging.basicConfig(filename='quantlog.log', level=logging.ERROR, format='%(asctime)s %(levelname)s %(message)s')
scheduler = BackgroundScheduler(job_defaults={'max_instances': 1})
config = Config()
api_key = config['Binance']['api_key']
api_secret = config['Binance']['api_secret']

def detect_signal():
    try:
        indicator = BTCHedge('binance', api_key, api_secret)
        indicator.signalGenerator()
    except Exception as e:
        logging.error('An error occurred in BTCHedge signalGenerator: %s', e, exc_info=True)
        print(e)
        print('DETECT BTC HEDGE SIGNAL DONE')

scheduler.add_job(detect_signal, 'interval', seconds=0.5, next_run_time=datetime.now() + timedelta(seconds=3))
scheduler.start()
