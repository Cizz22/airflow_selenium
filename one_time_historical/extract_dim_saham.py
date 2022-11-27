import sys
sys.path.insert(0, '/opt/airflow')

import pandas as pd

from datetime import datetime
from one_time_historical.utils import get_scrap_data, pg_engine as pg
from sqlalchemy import MetaData, Table, Column, String, DateTime, Text, Float

from one_time_historical.creds import creds

pd.options.mode.chained_assignment = None

def getstocklistData():
    DaftarSaham = pd.DataFrame()
    
    URL = 'https://www.idx.co.id/primary/StockData/GetSecuritiesStock?start=0&length=9999&code=&sector=Energy&board=&language=id-id'
        
    data = get_scrap_data(URL)

    # Mengubah json ke dalam bentuk DataFrame
    df = pd.DataFrame(data['data'])

    # Mengubah format tanggal menjadi datetime
    df['ListingDate'] = [datetime.strptime(
        x[:10], '%Y-%m-%d') for x in df['ListingDate']]
    df['Sector'] = "Energy"

    DaftarSaham = pd.concat([DaftarSaham, df], ignore_index=True)

    # urut berdasarkan kode saham
    DaftarSaham = DaftarSaham.sort_values(by='Code').reset_index(drop=True)

    
    return DaftarSaham

def createStockListTable():
    engine = pg(creds)
    metadata = MetaData(bind=engine)
    stocks = Table('saham', metadata,
                       Column('Code', String, primary_key=True),
                       Column('Name', Text,
                              nullable=False, unique=True),
                       Column('ListingDate', DateTime),
                       Column('Shares',Float ),
                       Column('ListingBoard', String),
                       Column('Sector', String),
                       )
    stocks.drop(checkfirst=True)
    stocks.create()
    
def insertStockListData():
    data = getstocklistData()
    
    with pg(creds).connect() as conn:
        data.to_sql('saham', conn, if_exists='append', index=False)
        




