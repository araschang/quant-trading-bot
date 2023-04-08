from flask_restful import Api
from flask import Flask
from Indicators.Scheduler import YuanScheduler
from Model import Scheduler

app = Flask(__name__)
api = Api(app)
