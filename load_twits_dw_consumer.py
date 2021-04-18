#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
from mysql.connector import Error
from kafka import KafkaConsumer
import re

def dw_consumer():
    # Connect to MySQL database
    dw_conn = None
    dw_load_query = "INSERT INTO `fact_market_sentiment`(`sentiment_id`, `stock_symbol`, `bearish`, `bullish`, `unemotional`) " \
                     "VALUES(NULL,%s,%s,%s,%s)"

    
    print('\nWaiting for AGGREGATED TUPLES, Ctr/Z to stop ...')
    
    
    consumer = KafkaConsumer('AggrData',bootstrap_servers='192.168.1.227:29092',api_version=(2,0,2))
    aggr_tuples = [] 
    z = 0

    for message in consumer:
        in_string = message.value.decode()
        print ('\nMesssage Received: {}'.format(in_string))

        if "END_OF_RES" in in_string:
            break
        
        in_split = in_string.split(',')
        
        stock_symbol = in_split[0].strip(' \'')
        bearish = re.sub('[^0-9]','',in_split[1].strip(' \''))
        bullish = re.sub('[^0-9]','',in_split[2].strip(' \''))
        unemotional = re.sub('[^0-9]','',in_split[3].strip(' \''))
        
        in_tuple = (stock_symbol,bearish,bullish,unemotional)
        print ('\nTuple Received: {}'.format(in_tuple))
        aggr_tuples.append(in_tuple)
        
        z = z+1
        
        # if z == 1:
        #   break
    
    try:  
        dw_conn = mysql.connector.connect(host='192.168.1.227', # !!! make sure you use your VM IP here !!!
                                  port=23306, 
                                  database = 'dw',
                                  user='root',
                                  password='secret')
        
        if dw_conn.is_connected():
                print('\nConnected to destination DW MySQL database')
        
        dw_cursor = dw_conn.cursor()
        
        #odb_cursor.execute(odb_aggregate_query)
        #aggr_tuples = odb_cursor.fetchall()
    
        dw_cursor.executemany(dw_load_query,aggr_tuples)
        
        dw_conn.commit()
        
        dw_cursor.execute("SELECT count(*) FROM fact_market_sentiment")
        res = dw_cursor.fetchone()
    
        print('DW is loaded: {} new tuples are inserted'.format(len(aggr_tuples)))
        print('               {} total tuples are inserted'.format(res[0]))    
        
            
    except Error as e:
        print(e)
        
    finally:
        if dw_conn is not None and dw_conn.is_connected():
            dw_cursor.close()
            dw_conn.close()    
            
if __name__ == '__main__':
    dw_consumer()
    