#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 1 00:51:20 2022

@author: smd
"""

import sys
import datetime
import argparse
import dataclasses, json
from dataclasses import dataclass

@dataclass
class ParticulateLine:
  sensor_id: str
  sensor_type: str
  location: str
  latitude: float
  longitude: float
  timestamp: str
  p1: float
  p2: float

def parse( line ):
  tokens = line.split(';')
  id = tokens[0]
  type = tokens[1]
  loc = tokens[2]
  lat = float(tokens[3])
  lon = float(tokens[4])
  ts = tokens[5]
  p1 = float(tokens[6])
  p2 = float(tokens[9])

  return ParticulateLine(sensor_id = id, sensor_type = type, location = loc, latitude = lat, longitude = lon, timestamp = ts, p1 = p1, p2 = p2)


import time
import sys
from json import dumps
from kafka import KafkaProducer

def publish(lines, topic, speedup) :
    try: 
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        
        firstLineTime = -1
        firstWallTime = -1
        for line in lines:
            try:
                parts = line.split(';')
                lineTime = datetime.datetime.strptime(parts[5], '%Y-%m-%dT%H:%M:%S')
                if firstLineTime == -1 :
                    firstLineTime = lineTime
                    firstWallTime = datetime.datetime.now()
                    
                deltaLineTime = lineTime - firstLineTime
                
                deltaLineTimeS = (lineTime - firstLineTime) / datetime.timedelta(microseconds=1) / 1000000.0
                
#                print('line relative time: : {} secs'.format(deltaLineTimeS))

                wallTime = datetime.datetime.now()
                deltaWallTimeS = (wallTime - firstWallTime) / datetime.timedelta(microseconds=1) / 1000000.0
                

#                print('wall relative time: : {} secs'.format(deltaWallTimeS))
                
		    
                delay = (deltaLineTimeS/speedup - deltaWallTimeS)

#                print( 'wall time: {}, delay: {}, {}'.format(deltaWallTimeS, delay, 1.0/delay))
		
                if delay > 0 :
                    time.sleep( delay )
                        
                dt = parse( line )
#                print(dt)
                producer.send(topic, value=dataclasses.asdict(dt))
            except Exception as err:
                print(err)
                
    except Exception as err:
            print(err)

parser = argparse.ArgumentParser(description='dataset kafka publisher...')
parser.add_argument('--filename', type=str, default='2020-01-06_sds011-pt.csv', help='dataset filename ') 
parser.add_argument('--topic', dest='topic', type=str, default='particles_json', help='kafka topic (default: particles_json)')
parser.add_argument('--speedup', type=int, dest='speedup', default=3600, help='time speedup factor (default: 3600)')

args = parser.parse_args()
print(args)

f = open(args.filename, "r")
lines = f.readlines()
f.close()
publish(lines, args.topic, args.speedup)
