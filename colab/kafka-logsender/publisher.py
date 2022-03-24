#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 00:51:20 2019

@author: smd
"""

import datetime
import time
import sys
from json import dumps
from kafka import KafkaProducer

def publish(lines) :
    try: 
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


        firstTime = -1
        firstWallTime = -1
        for line in lines:
            if line == '' :
                break
            lineTime = datetime.datetime.strptime(line[:23], '%Y-%m-%dT%H:%M:%S.%f')
            if firstTime == -1 :
                firstTime = lineTime
                firstWallTime = datetime.datetime.now()
            diffTime = lineTime - firstTime
            diffMs = diffTime.total_seconds() * 1000 + diffTime.microseconds / 1000
            wallTime = datetime.datetime.now()
            diffWallTime = wallTime - firstWallTime
            diffWallMs = diffWallTime.total_seconds() * 1000 + diffWallTime.microseconds / 1000
            if diffWallMs < diffMs :
                time.sleep( (diffMs - diffWallMs) / 1000)

            wallTime = datetime.datetime.now()
            timestamp = wallTime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'+0000';
            newline = timestamp + line[28:]            	               
            producer.send('weblog', value=newline)
    except:
        try:
            conn.close()
        except:
            True
        True

filename = "web.log"
if len(sys.argv) > 1:
    filename = sys.argv[1]

f = open(filename, "r")
lines = f.readlines()
f.close()
publish(lines)
