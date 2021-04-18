#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
from mysql.connector import Error
from kafka import KafkaProducer, KafkaConsumer
from time import sleep

def odb_producer():
    # Connect to MySQL database
    odb_conn = None
    odb_aggregate_query = "SELECT `stock_symbol`, SUM(CASE WHEN t.`basic_sentiment` = 'Bearish' THEN 1 ELSE 0 END) \
        AS bearish, SUM(CASE WHEN t.`basic_sentiment` = 'Bullish' THEN 1 ELSE 0 END) AS bullish, \
        SUM(CASE WHEN t.`basic_sentiment` = '' THEN 1 ELSE 0 END) AS unemotional \
        FROM `stocktwits` t GROUP BY `stock_symbol`"    
                          
    consumer = KafkaConsumer('odb-update-stream',bootstrap_servers='192.168.1.227:29092',api_version=(2,0,2))                      
    producer = KafkaProducer(bootstrap_servers='192.168.1.227:29092',api_version=(2,0,2))

          
    print('\nWaiting for ODB UPDATE EVENT, Ctr/Z to stop ...')
    for message in consumer:
        print ('\nODB UPDATE EVENT RECEIVED FROM odb-update-stream')
        print ('Producing aggregated tuple for AggrData stream ...')
        
        sleep(1)
        
        break
                           
    try:  
        odb_conn = mysql.connector.connect(host='192.168.1.227', # !!! make sure you use your VM IP here !!!
                                  port=13306, 
                                  database = 'odb',
                                  user='root',
                                  password='secret')
        
        if odb_conn.is_connected():
                print('\nConnected to source ODB MySQL database')
                
        odb_cursor = odb_conn.cursor()
        odb_cursor.execute(odb_aggregate_query)
        aggr_tuples = odb_cursor.fetchall()

        for tuple in aggr_tuples :
            in_string = ''.join(str(tuple)).strip('()')
            producer.send('AggrData',in_string.encode() )
            print("\nProduced aggregated tuple: {}".format(tuple))

        END_OF_RES = "END_OF_RES"    
        producer.send('AggrData',END_OF_RES.encode() )
        producer.flush()
        
            
    except Error as e:
        print(e)
        
    finally:
        if odb_conn is not None and odb_conn.is_connected():
            odb_cursor.close()
            odb_conn.close()
            
if __name__ == '__main__':
    odb_producer()
    