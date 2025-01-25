# import redis
# import json
# from threading import Timer
# import logging
# from.main importBase
import json
import asyncio
import redis
import queue
import threading
from collections import deque
from pymongo import MongoClient, errors as pymongo_errors
from datetime import datetime, time
import traceback
import sys
import os
import time
from itertools import product
from datetime import datetime
# to fix inehritance errors
import aiohttp
from pymongo.errors import *
from socketio.exceptions import *
from redis.exceptions import *


import socket
import asyncio
import ssl
import os
import sys

from json import JSONDecodeError
from requests.exceptions import *
from urllib3.exceptions import *
from concurrent.futures import ThreadPoolExecutor


class LowLatencyDataBase:
    def __init__(self, 
                 redis_host="localhost", 
                 redis_port=6379, 
                 redis_db=0, 
                 mongo_uri='mongodb://localhost:27017/', 
                 mongo_db_name='XTS_MARKET_DATA_WEBSOC'
                 ):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.mongo_uri = mongo_uri
        self.mongo_db_name = mongo_db_name
        self.executor = ThreadPoolExecutor(max_workers = 10)  
        self.shutdown_flag = False

        self.pool = redis.ConnectionPool(host='localhost', port=6379, db=0)

        self.mongo_client = MongoClient(mongo_uri)
        self.mongo_db = self.mongo_client[str(mongo_db_name)]

        self.redis_client = redis.Redis(host=redis_host, 
                                        port=redis_port, 
                                        db=redis_db, 
                                        connection_pool = self.pool)
        
        self.mongo_db = self.mongo_client[mongo_db_name]
        


    def create_deque_and_handles(self):
        try:
            self.mongo_coll = {
                '1501-json-full': self.mongo_db["1501-json-full"],
                '1502-json-full': self.mongo_db["1502-json-full"],
                '1505-json-full': self.mongo_db["1505-json-full"],
                '1512-json-full': self.mongo_db["1512-json-full"],
                '1510-json-full': self.mongo_db["1510-json-full"],
                '1105-json-full': self.mongo_db["1105-json-full"],
            }
            self.info("Collections for the Database for Channels Initialised")
            self.data_deques = {
                '1501-json-full': deque(),
                '1502-json-full': deque(),
                '1505-json-full': deque(),
                '1512-json-full': deque(),
                '1510-json-full': deque(),
                '1105-json-full': deque(),
            }
            self.info("Deque for Channels Initialised")
            self.event_handles = [
                '1501-json-full',
                '1502-json-full',
                '1505-json-full',
                '1510-json-full',
                '1512-json-full',
                '1105-json-full',
            ]
            self.info("Event handlers for Channels Initialised")
        except Exception as e:
            print(fr"Initiating Deque and Handles error: {e}")

        self.threads_deques = {}
        for key, value in self.data_deques.items():
            try:
                thread = threading.Thread(target = self.process_data, 
                                        args =(key,))
                self.threads_deques[key] = thread
                thread.start()
                self.info(fr"Starting thread the Deque for {key}")
                # thread.join()
            except Exception as e:
                print(fr"threaded processes for deques went wrong | error: {e}")


        self.threads_redis_channels = {}
        for channel, db in self.mongo_coll.items():
            try:
                thread = threading.Thread(target = self.listen_to_channel, args = (channel,db))
                self.threads_redis_channels[channel] = thread
                thread.start()
                self.info(fr"Starting thread the Channel: {channel} | Database: {db}")
                # thread.join()
            except Exception as e:
                print(fr"threaded processes for event handles went wrong | error: {e}")

    def process_data(self, key):
        redis_client = redis.Redis(host = self.redis_host, 
                                    port = self.redis_port, 
                                    db = self.redis_db, 
                                    connection_pool = self.pool)
        
        while True:            
            if self.data_deques[key]:
                data = self.data_deques[key].popleft()
                time_string = datetime.now().strftime("%H:%M:%S.%f")[:-1]
                data = json.loads(data)
                message = json.dumps({"time": time_string, "data": data})
                try:
                    redis_client.publish(channel = key, 
                                         message = message)
                except Exception as e:
                    print(f"Failed to publish to Redis: {e}")
                    time.sleep(0.1)


    def listen_to_channel(self, 
                        channel,
                        db):
        
        redis_client = redis.Redis(host=self.redis_host, 
                                        port=self.redis_port, 
                                        db=self.redis_db, 
                                        connection_pool = self.pool)
        pubsub = redis_client.pubsub()
        pubsub.subscribe(channel)

        while True:
            for message in pubsub.listen():
                if message['type'] == 'message':
                    try:
                        channel_byte_string = message['channel']
                        data_byte_string = message['data']

                        channel_string = channel_byte_string.decode('utf-8')
                        data_string = data_byte_string.decode('utf-8')
                        
                        data_dict = json.loads(data_string)
                        insert_data = {
                            'channel_name': channel_string,
                            'data': data_dict['data'],
                            'time': data_dict['time']
                        }

                        print(f"Listening To Channel Function {data_dict}")
                        db.insert_one(insert_data)

                    except Exception as e:
                        raise ValueError(fr"Error inserting data into {channel_string}")
