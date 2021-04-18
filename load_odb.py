#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from datetime import datetime
import mysql.connector
from mysql.connector import Error

# Database credentials
HOST_IP = "192.168.1.227"
PORT = 13306
DB_USER = "root"
DB_PASS = "secret"

# OLTP queries
LOCATION_QUERY = "INSERT INTO location(city, state) VALUES (%s, %s)"
COMPANY_QUERY = "INSERT INTO company(stock_symbol, company_name, sector, industry, location_id) VALUES (%s, %s, %s, %s, %s)"
REVENUE_QUERY = "INSERT INTO company_revenue(quarter, revenue, year, stock_symbol) VALUES (%s, %s, %s, %s)"
SALES_QUERY = "INSERT INTO stock_sale(date, open, high, low, close, volume, stock_symbol) VALUES (%s, %s, %s, %s, %s, %s, %s)"

def load_odb():
    conn = None

    try:
        # Connect to the database
        conn = mysql.connector.connect(host=HOST_IP, port=PORT, user=DB_USER, password=DB_PASS)

        if conn.is_connected():
            print("Connected to the ODB...")
        
        cursor = conn.cursor()

        # Create the schema from file
        with open("create_odb.sql") as f:
            for query in f:
                cursor.execute(query)
        
        print("Successfully created the ODB...")

        # Load company information from file
        with open("processed_data/stocks_by_company.csv") as f:
            reader = csv.reader(f)
            # Skip the header in the CSV file
            locs = {}
            c_rows = []
            companies = set()

            for i, r in enumerate(reader):
                if i == 0: continue
                l = ",,,".join(r[4:])
                if not l in locs:
                    locs[l] = len(locs) + 1
                c_rows.append(r[:4] + [locs[l]])
                companies.add(r[0])

            cursor.executemany(LOCATION_QUERY, [l.split(",,,") for l in locs])
            cursor.executemany(COMPANY_QUERY, c_rows)

        print("Successfully loaded company information...")

        # Load quarterly revenue information from file
        with open("processed_data/quarterly_revenue_by_company_by_Statista.csv") as f:
            reader = csv.reader(f)
            # Skip header and strip Q's from quarters
            rows = [
                [r[0][1], r[1], r[2], r[3]]
                for i, r in enumerate(reader)
                if i > 0 and r[3] in companies
            ]
            cursor.executemany(REVENUE_QUERY, rows)

        print("Successfully loaded quarterly revenue information...")

        # Load sales information from file
        with open("processed_data/daily_stocks_by_company.csv") as f:
            reader = csv.reader(f)
            # Skip header and convert dates to Python datetime objects
            rows = [
                [datetime.strptime(r[0], "%Y-%m-%d"), r[1], r[2], r[3], r[4], r[5], r[6]]
                for i, r in enumerate(reader)
                if i > 0
            ]
            cursor.executemany(SALES_QUERY, rows)

        print("Successfully loaded sales information...")

        # Commit changes to the database
        conn.commit()

        print("Finished creating and loading the ODB")

    except IOError as e:
        print(f"I/O Error: {e!r}")

    except Error as e:
        print(f"MySQL Error: {e!r}")
    
    finally:
        # Close the database connection
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    load_odb()
