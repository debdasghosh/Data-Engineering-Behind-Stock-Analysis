#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
from mysql.connector import Error

HOST_IP = "192.168.1.227"
ODB_PORT = 13306
DW_PORT = 23306
DB_USER = "root"
DB_PASS = "secret"

LOCATION_QUERY = "INSERT INTO dim_location(location_id, city, state) VALUES (%s, %s, %s)"
COMPANY_QUERY = "INSERT INTO dim_company(stock_symbol, company_name, sector, industry) VALUES (%s, %s, %s, %s)"
DATE_QUERY = "INSERT INTO dim_date(day, week, month, quarter, year) VALUES (%s, %s, %s, %s, %s)"
QUARTER_QUERY = "INSERT INTO dim_quarter(quarter, year) VALUES (%s, %s)"
DAILY_QUERY = "INSERT INTO fact_daily_stock(date_id, stock_symbol, location_id, open, close, high, low, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
QUARTERLY_QUERY = "INSERT INTO fact_quarterly_stock(quarter_id, stock_symbol, location_id, revenue, open, close, high, low, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

def find(ele, col, func):
    """Performs a linear search of the specified collection with a custom comparison function."""
    for e in col:
        if func(ele, e):
            return e
    raise ValueError("The specified value is not in the provided collection")

def load_dw():
    dw_conn = None
    odb_conn = None

    try:
        # Connect to the data warehouse
        dw_conn = mysql.connector.connect(host=HOST_IP, port=DW_PORT, user=DB_USER, password=DB_PASS)

        if dw_conn.is_connected():
            print("Connected to the DW...")

        dw_cursor = dw_conn.cursor()

        # Create the data warehouse schema from file
        with open("create_dw.sql") as f:
            for query in f:
                dw_cursor.execute(query)
    
        print("Successfully created the DW...")

        # Connect to the operational database
        odb_conn = mysql.connector.connect(host=HOST_IP, port=ODB_PORT, user=DB_USER, password=DB_PASS)

        if odb_conn.is_connected():
            print("Connected to the ODB...")
        
        odb_cursor = odb_conn.cursor()
        odb_cursor.execute("USE odb")

        # Load location data from the ODB
        odb_cursor.execute("SELECT location_id, city, state FROM location")
        dw_cursor.executemany(LOCATION_QUERY, odb_cursor.fetchall())

        print("Successfully loaded location information...")

        # Load company data from the ODB
        odb_cursor.execute("SELECT stock_symbol, company_name, sector, industry FROM company")
        dw_cursor.executemany(COMPANY_QUERY, odb_cursor.fetchall())

        print("Successfully loaded company information...")

        # Load date data from the ODB
        odb_cursor.execute("SELECT DISTINCT DAY(date), WEEK(date), MONTH(date), CASE WHEN MONTH(date) >= 10 THEN 4 WHEN MONTH(date) >= 7 THEN 3 WHEN MONTH(date) >= 4 THEN 2 ELSE 1 END,YEAR(date) FROM stock_sale")
        dw_cursor.executemany(DATE_QUERY, odb_cursor.fetchall())

        print("Successfully loaded date information...")

        # Load quarter information from the ODB
        # NOTE: quarters are calculated generically, with Jan-March as 1, April-Jun as 2, etc.
        odb_cursor.execute("SELECT DISTINCT quarter, year FROM company_revenue")
        dw_cursor.executemany(QUARTER_QUERY, odb_cursor.fetchall())

        print("Successfully loaded quarter information...")

        # Load dates and IDs from the DW
        dw_cursor.execute("SELECT date_id, CONCAT(year, '-', month, '-', day) FROM dim_date")
        dates = dw_cursor.fetchall()

        # Load daily sales data from the ODB
        odb_cursor.execute("SELECT DATE_FORMAT(date, '%Y-%c-%e'), c.stock_symbol, c.location_id, open, close, high, low, volume FROM stock_sale s, company c WHERE s.stock_symbol = c.stock_symbol")
        sales = [[find(r[0], dates, lambda x, y: x == y[1])[0]] + list(r)[1:] for r in odb_cursor.fetchall()]
        dw_cursor.executemany(DAILY_QUERY, sales)

        print("Successfully loaded daily sales information...")

        # Load quarters and IDs from the DW
        dw_cursor.execute("SELECT quarter_id, quarter, year FROM dim_quarter")
        quarters = dw_cursor.fetchall()

        # Aggregate sales data by quarter and load from the ODB
        odb_cursor.execute("SELECT CASE WHEN MONTH(date) >= 10 THEN 4 WHEN MONTH(date) >= 7 THEN 3 WHEN MONTH(date) >= 4 THEN 2 ELSE 1 END, YEAR(date), c.stock_symbol, c.location_id, r.revenue, SUM(open), SUM(close), SUM(high), SUM(low), sum(volume) FROM stock_sale s, company c, company_revenue r WHERE s.stock_symbol = c.stock_symbol AND r.stock_symbol = c.stock_symbol AND r.year = YEAR(date) AND r.quarter = (CASE WHEN MONTH(date) >= 10 THEN 4 WHEN MONTH(date) >= 7 THEN 3 WHEN MONTH(date) >= 4 THEN 2 ELSE 1 END) GROUP BY CASE WHEN MONTH(date) >= 10 THEN 4 WHEN MONTH(date) >= 7 THEN 3 WHEN MONTH(date) >= 4 THEN 2 ELSE 1 END, YEAR(date), c.stock_symbol, c.location_id, r.revenue")
        q_sales = [[find(r[:2], quarters, lambda x, y: x[0] == y[1] and x[1] == y[2])[0]] + list(r)[2:] for r in odb_cursor.fetchall()]
        dw_cursor.executemany(QUARTERLY_QUERY, q_sales)
        print("Successfully loaded quarterly sales information...")

        # Summary Tables
        dw_cursor.execute("INSERT INTO `agg_stock_volume` SELECT NULL, `date_id`, `location_id`, SUM(`volume`) FROM `fact_daily_stock` GROUP BY `location_id`,`date_id`;")
        dw_cursor.execute("INSERT INTO `agg_quaterly_revenue` SELECT NULL, `quarter_id`, `stock_symbol`, SUM(`revenue`) FROM `fact_quarterly_stock` GROUP BY `stock_symbol`,`quarter_id`;")
        print("Successfully loaded summary tables...")
        

        # Commit changes to the data warehouse
        dw_conn.commit()

        print("Finished creating and loading the DW")

    except IOError as e:
        print(f"I/O Error: {e!r}")

    except Error as e:
        print(f"MySQL Error: {e!r}")
    
    finally:
        # Close the DW connection
        if dw_conn is not None and dw_conn.is_connected():
            dw_cursor.close()
            dw_conn.close()
        
        # Close the ODB connection
        if odb_conn is not None and odb_conn.is_connected():
            odb_cursor.close()
            odb_conn.close()

if __name__ == "__main__":
    load_dw()
