# Data-Engineering-Behind-Stock-Analysis
INFSCI 1540 Data Engineering Project

To generate datasets run the following R codes as per the order:
1. S&P500CompaniesExtraction.R --> For extracting S&P500 data from Wikipedia
2. StockMarketDataCleaning.R --> For getting data from Yahoo Finance
3. StatistaRevenueCleaning.R --> For extracting data from excel files downloaded from Statista and merge into a single CSV file
4. StockTwitsExtraction.R --> For extracting data using StockTwits API

Then in VM run the docker compose file:
sudo docker-compose up -d

To create and load ODB and DW databases run:
python load_odb.py
python load_dw.py
