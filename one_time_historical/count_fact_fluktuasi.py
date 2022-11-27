from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, ForeignKey, Text, select, insert, Numeric

import pandas as pd
from one_time_historical.creds import creds
from one_time_historical.utils import pg_engine as pg

def getFluktiasiData():
    harga_saham = pd.read_sql_table('harga_saham_harian', pg(creds))
    table_fluktuasi = pd.DataFrame()
    
    for row in harga_saham.itertuples():
        fluktuasi = (row.close - row.open) / row.open
        
        df = pd.DataFrame([[row.code, fluktuasi, row.timestamp]], columns=['code', 'fluktuasi', 'timestamp'])
        
        table_fluktuasi = pd.concat([table_fluktuasi, df])
    
    return table_fluktuasi

def createFluktuasiTable():
    engine = pg(creds)
    metadata = MetaData(bind=engine)
    saham = Table('saham', metadata, autoload=True, autoload_with=engine)
    fluktuasi = Table('fluktuasi', metadata,
                       Column('id', Integer, primary_key=True, autoincrement=True),
                       Column('code', Integer, ForeignKey(saham.c.Code), nullable=False),
                       Column('fluktuasi', Numeric, nullable=False),
                       Column('timestamp', DateTime, nullable=False))
    fluktuasi.drop(checkfirst=True)
    fluktuasi.create()
    
def insertFluktuasiData():
    data = getFluktiasiData()
    
    with pg(creds).connect() as conn:
        data.to_sql('fluktuasi', conn, if_exists='append', index=False)
        
        