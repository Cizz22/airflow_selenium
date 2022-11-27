import datetime

# The size of each step in days
day_delta = datetime.timedelta(days=1)

import os, json, pandas as pd

from datetime import datetime
from datetime import timedelta, date
from one_time_historical.utils import get_scrap_data, pg_engine as pg
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, ForeignKey, Text, select, insert,Numeric

from one_time_historical.creds import creds

pd.options.mode.chained_assignment = None

def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + timedelta(n)
            
            
def getBrokerlistData():
    start_date = date(2022, 11, 20)
    end_date = date(2022, 11, 25)
    
    BrokerSummary = pd.DataFrame()
    
    for single_date in daterange(start_date, end_date):
        dateB = single_date.strftime("%Y%m%d")
    
        URL = 'https://www.idx.co.id/primary/TradingSummary/GetBrokerSummary?length=9999&date={}'.format(dateB)
        
        data = get_scrap_data(URL)
        
         # Mengubah json ke dalam bentuk DataFrame
        df = pd.DataFrame(data['data'], columns=['Date','IDFirm','Volume','Value','Frequency'])
        
        df['Date'] = pd.to_datetime(df['Date'])

        BrokerSummary = pd.concat([BrokerSummary, df])

    return BrokerSummary
    
    

def createBrokerSummary():
    engine = pg(creds)
    metadata = MetaData(bind=engine)
    sekuritas = Table('sekuritas', metadata,autoload=True, autoload_with=engine)
    
    stocks = Table('broker_summary', metadata,
                       Column('id', Integer, primary_key=True, autoincrement=True),
                       Column('IDFirm', String,ForeignKey(sekuritas.c.IDFirm)),
                       Column('Date', DateTime,nullable=False),
                       Column('Volume', Numeric,nullable=False),
                       Column('Value', Numeric,nullable=False),
                       Column('Frequency', Integer,nullable=False))
    stocks.drop(checkfirst=True)
    stocks.create()
    
def insertBrokerSummary():
    data = getBrokerlistData()
    
    with pg(creds).connect() as conn:
        data.to_sql('broker_summary', conn, if_exists='append', index=False)
        
