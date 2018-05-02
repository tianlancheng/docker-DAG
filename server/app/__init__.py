# -*- coding: utf-8 -*-
from flask import Flask
from flask import request
from flask import jsonify
import json,os
import docker
from config import config
from flask_pymongo import PyMongo
import redis



app = Flask(__name__)
app.config.from_object(config['default'])

#connect to redis
pool = redis.ConnectionPool(host=app.config['REDIS_HOST'],port=app.config['REDIS_PORT'],db=0)
rcon = redis.StrictRedis(connection_pool=pool)

mongo = PyMongo(app)

from .dag import dag
app.register_blueprint(dag,url_prefix='/api')

