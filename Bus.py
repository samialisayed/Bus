#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 18:22:21 2020

@author: sami
"""
import bisect
import json
import scipy.interpolate
from dateutil import parser
import datetime
import numpy as np
#import sys

from pyspark import SparkContext


def FilterBus(partId, records):
    import csv
    reader = csv.reader(records)
    for row in reader:
        route_id = row[2]
        trip_id = row[3]
        busID = row[4]
        time = row[5]
        distance = row[6]
        timeToLastStop = row[8]
        if time[11:13] not in ['10','11','00','01','02','03']:      #data stream stops from 10pm to 4am
            yield(route_id,trip_id,busID,time, float(distance), timeToLastStop)

def FindingBadTrips(partId, records):
    x = ('75', '2', 'sami', '2020-10-25T00:00:03+07:00', 4774.75, '926')
    for row in records:
        if row[0] == x[0] and row[1]==x[1] and row[2]==x[2] and row[3]==x[3]:
            yield(row[0],row[1],row[2])
            x = row
        else:
            x = row
            
def Findingbad(partId, records):
    x = ('75', '2', 'sami', '2020-10-25T00:00:03+07:00', 4774.75, '926')
    for row in records:
        if row[2] == x[2] and row[3]==x[3]:
            yield(x[0],x[1],x[2])
            x = row
        else:
            x = row
            
def interval(partId, records):
    x = first[0]
    for row in records:
        if row[0]==x[0] and row[1]==x[1] and row[2]==x[2] and row[3]!=x[3] and row[4]!=x[4]:
            yield(x[0], x[1], x[2], (x[3],row[3]), (x[4],row[4]))
            x = row
        else:
            x = row
            
def searching(idd,records):
    for row in records:
        for i in data:
            if row[0]==i['route_id'] and row[1]==i['trip_id']:
                left = bisect.bisect_left(list(reversed(i['stops_distance'])), row[4][0])
                right = bisect.bisect_left(list(reversed(i['stops_distance'])), row[4][1])
                if (row[4][0] > row[4][1] and left != right):
                    yield(row[0], row[1], row[2], row[3], row[4], list(reversed(i['stops']))[right])
                    
def merge_interval(partId, records):
    repeat = ('36','2','51B04731',('2020-10-01T05:11:52+07:00', '2020-10-01T05:12:02+07:00'),(7818.87, 7770.54),[1935, 7803.019308816372])
    x = ('36','2','51B04731',('2020-10-01T05:11:52+07:00', '2020-10-01T05:12:02+07:00'),(7818.87, 7770.54),[1935, 7803.019308816372])
    for row in records:
        if row[5][0] != x[5][0] and x[5][1] != repeat[5][1]:
            yield(x)
            x = row
        elif row[5][0] != x[5][0] and x[5][1] == repeat[5][1]:
            x = row
        else:
            yield(x[0],x[1],x[2],(x[3][0],row[3][1]),(x[4][0],row[4][1]),x[5])
            repeat = x
            x = row
    yield(x)
    

def Finding_time(idd,records):
    for row in records:
        y_interp = scipy.interpolate.interp1d([row[4][1], row[4][0]], [0,(parser.parse(row[3][1]) - parser.parse(row[3][0])).total_seconds()])
        time = parser.parse(row[3][0]) + datetime.timedelta(0,np.array([[y_interp(row[5][1])]])[(0,0)])
        yield(row[0],row[1],row[2],row[5][0],row[5][1],time.replace(tzinfo=None))
        
def to_csv(x):
    import csv, io
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip()

if __name__ == '__main__':
    sc = SparkContext()
    bus = sc.textFile('/data/share/ebms/2020/*.csv')
    
    with open('route_stops.json') as f:
        data = json.load(f)
    for i in data:
        i['stops_distance']=[]
        for j in i['stops']:
            i['stops_distance'].append(j[1])
            
    bus_bytime = bus.mapPartitionsWithIndex(FilterBus).distinct().sortBy(lambda x: (x[2],x[3]))
    bad_trips = bus_bytime.mapPartitionsWithIndex(FindingBadTrips).distinct().collect()
    before_final = bus_bytime.filter(lambda x: ((x[0],x[1],x[2]) not in bad_trips))
    badd_trips = before_final.mapPartitionsWithIndex(Findingbad).distinct().collect()
    final = before_final.filter(lambda x: ((x[0],x[1],x[2]) not in badd_trips))
    first = final.take(1)
    bus_time = final.mapPartitionsWithIndex(interval).mapPartitionsWithIndex(searching).mapPartitionsWithIndex(merge_interval).mapPartitionsWithIndex(Finding_time)
    
    bus_time.map(to_csv).saveAsTextFile('All.csv')
