#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
from mysql.connector import Error

from kafka import KafkaConsumer, KafkaProducer
import json
from time import sleep

def odb_consumer():
    # Connect to MySQL database
    conn = None
    query = "INSERT INTO `stocktwits` (	`twits_id`, `tweet_text`, `created_at`, `basic_sentiment`, `stock_symbol`, `stock_title`) " \
            "VALUES(NULL, %s,%s,%s,%s,%s)"
    
    consumer = KafkaConsumer('Twits',bootstrap_servers='192.168.1.227:29092',api_version=(2,0,2))
    producer = KafkaProducer(bootstrap_servers='192.168.1.227:29092')
                             
    
    print('\nWaiting for INPUT TUPLES, Ctr/Z to stop ...')
    
    tuples = [] 
    # tuples = [('$SPY tech bros are playing a very risky game with the exception of $AAPL $QCOM', '2021-04-04T23:49:40Z', 'Bearish', 'AAPL', 'Apple Inc.'),
    # ('$SPY The is actual footage of Bears waking up tomorrow &amp; going down stairs to answer Margin Calls $AAPL $TSLA $DIS $CAT', '2021-04-04T23:45:45Z', '', 'AAPL', 'Apple Inc.')]  
    z = 1
    for message in consumer:
        in_string = message.value.decode()
        in_tuple = json.loads(in_string)
        # print ('\nInput Tuple Received: {}'.format(in_string))
        
        #sleep(1)
        
        stock_code = in_tuple['stock_code']
        stock_title = in_tuple['stock_title']
        body = in_tuple['body']
        created_at = in_tuple['created_at']
        sentiment = in_tuple['sentiment']

        tuples.append((body,created_at,sentiment,stock_code,stock_title))

        if z >= 6000:
            break
        z += 1

        
  
    try:  
        conn = mysql.connector.connect(host='192.168.1.227', # !!! make sure you use your VM IP here !!!
                                  port=13306, 
                                  database = 'odb',
                                  user='root',
                                  password='secret')
        if conn.is_connected():
                print('\nConnected to destination ODB MySQL database')
        
        cursor = conn.cursor()

        cursor.executemany(query,tuples)   

            
        conn.commit()
        
        
        m = 'odb update event'   
        producer.send('odb-update-stream', m.encode())
        print('\nODB UPDATE EVENT SENT TO ODB UPDATE STREAM')
        producer.flush()
        
            
    except Error as e:
        print(e)
        
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close() 
    
            
if __name__ == '__main__':
    odb_consumer()
    