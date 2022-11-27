import os, json, pandas as pd

from datetime import datetime
from one_time_historical.utils import get_scrap_data, pg_engine as pg
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, ForeignKey, Text, select, insert

from one_time_historical.creds import creds

pd.options.mode.chained_assignment = None

def getBrokerlistData():
    DaftarBroker = pd.DataFrame()
    
    URL = 'https://www.idx.co.id/primary/TradingSummary/GetBrokerSummary?length=9999'
        
    data = get_scrap_data(URL)

    # Mengubah json ke dalam bentuk DataFrame
    df = pd.DataFrame(data['data'], columns=['IDFirm', 'FirmName'])

    DaftarBroker = pd.concat([DaftarBroker, df])

    # urut berdasarkan kode saham
    DaftarBroker = DaftarBroker.sort_values(by='IDFirm').reset_index(drop=True)

    return DaftarBroker

def createBrokerListTable():
    engine = pg(creds)
    metadata = MetaData(bind=engine)
    stocks = Table('sekuritas', metadata,
                       Column('IDFirm', String, primary_key=True),
                       Column('FirmName', Text,nullable=False, unique=True))
    stocks.drop(checkfirst=True)
    stocks.create()
    
def insertBrokerListData():
    data = getBrokerlistData()
    
    with pg(creds).connect() as conn:
        data.to_sql('sekuritas', conn, if_exists='append', index=False)
        


