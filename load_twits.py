#!/usr/bin/env python3
from kafka import KafkaProducer
from time import sleep
import glob
import json
import re
import os

def producer_f(topic,broker_addr):

    producer = KafkaProducer(bootstrap_servers=broker_addr,api_version=(2,0,2))
    count = 0

    for json_file in glob.glob("processed_data/StockTwits/*.json"):
        
        filename = json_file
        file_src = open(filename,"r")
        twits = json.load(file_src)
        stock_code = twits['symbol']['symbol'][0]
        stock_title = twits['symbol']['title'][0]

        for stocktwit in twits['messages']:
            count += 1
            body = stocktwit['body']
            body = re.sub('[^a-zA-Z0-9.,?$:/() ]','',body)
            created_at = stocktwit['created_at']
            try:
                sentiment = stocktwit['entities']['sentiment']['basic']
            except:
                sentiment = ''
            line = {'stock_code': stock_code, 'stock_title': stock_title, 'body': body, 'created_at': created_at, 'sentiment': sentiment }
            line = json.dumps(line)
            producer.send(topic,line.encode())
            
            #sleep(1)

            #print("\nProduced input tuple {}: {}".format(count-1, line))
            print("Sent {}".format(line))
            
        
        file_src.close()
        #os.rename(filename, filename+".txt")
        if os.path.exists(filename):
            os.remove(filename)


    producer.flush()
    print("\nDone with producing data to topic {}.".format(topic))

data_pipe = 'Twits'
broker_addr = '192.168.1.227:29092'

producer_f(data_pipe, broker_addr)

