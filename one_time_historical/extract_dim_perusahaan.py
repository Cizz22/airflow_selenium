import os, json, pandas as pd

from datetime import datetime
from one_time_historical.utils import get_scrap_data, pg_engine as pg
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, ForeignKey, Text, select, insert
from one_time_historical.creds import creds

pd.options.mode.chained_assignment = None

def getCompanylistData():
    DaftarSaham = pd.read_sql_query('select * from saham', pg(creds))
    
    # DaftarSaham = [code for code in DaftarSaham['Code']]
    
    DaftarPerusahaan = pd.DataFrame()
    
    
    URL = 'https://www.idx.co.id/primary/ListedCompany/GetCompanyProfiles?KodeEmiten&language=id-id&length=1000'
    data = get_scrap_data(URL)
           
        # Mengubah json ke dalam bentuk DataFrame
    df = pd.DataFrame(data['data'], columns=['BAE','KodeEmiten','EfekEmiten_EBA','EfekEmiten_ETF','EfekEmiten_Obligasi','EfekEmiten_Saham','EfekEmiten_SPEI','Industri','SubIndustri','Email','Fax','JenisEmiten','KegiatanUsahaUtama','KodeDivisi','NPKP','NPWP','PapanPencatatan','TanggalPencatatan','Telepon','Website'])
    
    
    for i, data in enumerate(df['KodeEmiten']):
        if data in DaftarSaham['Code'].values:
            df.iloc[i:i+1].fillna('', inplace=True)
            DaftarPerusahaan = pd.concat([DaftarPerusahaan, df.iloc[i:i+1]])
    
    DaftarPerusahaan = DaftarPerusahaan.sort_values(by='KodeEmiten').reset_index(drop=True)

    return DaftarPerusahaan

def createCompanyList():
    engine = pg(creds)
    metadata = MetaData(bind=engine)
    saham = Table('saham', metadata,autoload=True, autoload_with=engine)
    
    stocks = Table('perusahaan', metadata,
                       Column('id', Integer, primary_key=True, autoincrement=True),
                       Column('BAE', String),
                       Column('KodeEmiten', String,ForeignKey(saham.c.Code), nullable=False, unique=True),
                       Column('EfekEmiten_EBA', String),
                       Column('EfekEmiten_ETF',String),
                       Column('EfekEmiten_Obligasi', String),
                       Column('EfekEmiten_Saham', String),
                       Column('EfekEmiten_SPEI', String),
                       Column('Industri', String),
                       Column('SubIndustri', String),
                       Column('Email', String),
                       Column('Fax', String),
                       Column('JenisEmiten', String),
                       Column('KegiatanUsahaUtama', String),
                       Column('KodeDivisi', String),
                       Column("NPKP", String),
                       Column("NPWP", String),
                       Column("PapanPencatatan", String),
                       Column("TanggalPencatatan", DateTime),
                       Column("Telepon", String),
                       Column("Website", String),
                       )
    stocks.drop(checkfirst=True)
    stocks.create()
    
def insertCompanyList():
    data = getCompanylistData()
    
    with pg(creds).connect() as conn:
        data.to_sql('perusahaan', conn, if_exists='append', index=False)
        
