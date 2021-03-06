
-- Which state/city has the most number of volumes of stocks for the S&P 500 American companies on date/week/month/quarter/year basis?
-- Which state has the most number of volumes of stocks for the S&P 500 American companies in a particular day?

SELECT dl.state, sum(rc.volume) AS tot_vol 
from agg_stock_volume rc 
INNER JOIN dim_location dl on dl.location_id=rc.location_id 
INNER JOIN dim_date d on d.date_id = rc.date_id 
WHERE d.year = 2017 and d.month = 1 and d.day = 3 
GROUP BY dl.state ORDER BY tot_vol DESC;

-- Revenues distribution of companies Sector/Industry wise and Quarter/Year wise
-- Revenues per Sector in a particular Quarter

SELECT sc.sector, sum(rc.revenue) AS revenue 
from dim_company sc 
INNER JOIN agg_quaterly_revenue rc on rc.stock_symbol=sc.stock_symbol 
INNER JOIN dim_quarter q on q.quarter_id = rc.quarter_id 
WHERE q.year = 2017 and q.quarter = 3 
GROUP BY sc.sector ORDER BY revenue DESC;

-- Latest Sentiments about a Company

SELECT * FROM `fact_market_sentiment`;