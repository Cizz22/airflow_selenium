
import requests, json, pandas as pd

from datetime import datetime, date, timedelta

from one_time_historical.utils import pg_engine as pg,  getdate, getcookie, cleandata

from one_time_historical.creds import creds

from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, ForeignKey, Text, select, insert


def getStockPriceDaily():
    DaftarSaham = pd.read_sql_table('saham', pg(creds).connect())
    awal, akhir = getdate(0)
    header, cookies, crumb, tradingperiod = getcookie()
    result = getstockdata(DaftarSaham, header, cookies, crumb, awal, akhir, tradingperiod)
    return result
    

def getstockdata(DaftarSaham, header: dict, cookies, crumb: str, awal, akhir,tradingperiod):
    error = []
    DataStock = pd.DataFrame(columns=['timestamp', 'open', 'low', 'high', 'close', 'volume'])
    
    #Melakukan looping untuk mengambil semua data saham
    for kode_emiten in DaftarSaham['Code']:
        try:
            # Membuka website dengan crumb dan cookies yang sudah didapatkan sebelumnya
            website = requests.get('https://query1.finance.yahoo.com/v8/finance/chart/'+kode_emiten+'.JK?&range=1d&useYfid=true',                                   '&interval=1m&includePrePost=true&events=div|split|earn&lang=en-US&region=US&crumb='+crumb +
                                   '&corsDomain=finance.yahoo.com', headers=header, cookies=cookies).text
            
            # Mengubah text yang didapatkan dari request ke dalam bentuk json
            site_json = json.loads(website)
            
            # Menentukan lokasi data saham (open, low, high, close, volume)
            stockdata = site_json['chart']['result'][0]['indicators']['quote'][0]
            
            # Melakukan transformasi data dari json ke DataFrame
            stockdf = pd.DataFrame(list(zip([datetime.fromtimestamp(x).strftime("%x %X") for x in site_json['chart']['result'][0]['timestamp']],
                                            stockdata['open'], stockdata['low'], stockdata['high'], stockdata['close'],stockdata['volume'])), columns=['timestamp', 'open', 'low', 'high', 'close', 'volume'])
            
            # Mengisi data yang kosong/NaN dengan 0
            stockdf.fillna(0, inplace=True)
            
            stockdf['code'] = kode_emiten
            
            # Mengubah tipe data kolom timestamp menjadi datetime
            stockdf['timestamp'] = pd.to_datetime(stockdf['timestamp'])
            
            # Mengubah tipe data kolom open, low, high, close, dan volume menjadi integer
            stockdf[['open', 'low', 'high', 'close', 'volume']] = stockdf[[
                'open', 'low', 'high', 'close', 'volume']].astype('int64')
            
            stockdf = cleandata(stockdf, awal, akhir, tradingperiod)
            
            # Menyimpan data saham ke dalam DataFrame
            DataStock = pd.concat([DataStock, stockdf], ignore_index=True)
            
        except:
            error.append(kode_emiten)
            continue
        
    print('error: ', error)
    return DataStock


def cleandata(stockdf, awal: str, akhir: str, tradingperiod):
        tanggal = pd.bdate_range(start=datetime.strptime(awal, "%Y_%m_%d"), end=datetime.strptime(
            akhir, "%Y_%m_%d")+timedelta(days=0), freq='1T')
        
        startperiod = datetime.utcfromtimestamp(tradingperiod[0]['start']+25200).strftime('%H:%M')
        endperiod = datetime.utcfromtimestamp(tradingperiod[0]['end']+25200-60).strftime('%H:%M')
        
        tanggal = tanggal[tanggal.indexer_between_time(startperiod, endperiod)]
        
        tanggal = tanggal.strftime("%Y-%m-%d %H:%M:%S")
        tanggal = tanggal.to_list()
        
        emptydf = pd.DataFrame(columns={
                               'code':'','timestamp': '', 'open': '', 'low': '', 'high': '', 'close': '', 'volume': ''})
        emptydf['timestamp'] = tanggal
        
        result = pd.concat([stockdf, emptydf], ignore_index=True, sort=False)
       
        result.drop_duplicates(
            subset=['timestamp'], keep='first', inplace=True)
       
        x = 0
        while result['close'][x] == 0:
            x += 1
        for i in range(x):
            result['open'][i] = result['open'][x]
            result['low'][i] = result['low'][x]
            result['high'][i] = result['high'][x]
            result['close'][i] = result['close'][x]
        for i in range(x, len(result['timestamp'])):
            if result['close'][i] == 0:
                result['open'][i] = result['open'][i-1]
                result['low'][i] = result['low'][i-1]
                result['high'][i] = result['high'][i-1]
                result['close'][i] = result['close'][i-1]
        
        return result


  
def createStockPriceDailyTable():    
    engine = pg(creds)
    metadata = MetaData(bind=engine)
    saham = Table('saham', metadata,autoload=True, autoload_with=engine)
    
    stocks = Table('harga_saham_harian', metadata,
                      Column('id', Integer, primary_key=True, autoincrement=True),
                      Column('code', String(10), ForeignKey(saham.c.Code)),
                      Column('open', Integer),
                      Column('low', Integer),
                      Column('high', Integer),
                      Column('close', Integer),
                      Column('volume', Integer),
                      Column('timestamp', DateTime),
    )
    stocks.drop(checkfirst=True)
    stocks.create()

def insertStockPriceDaily():
    data = getStockPriceDaily()
    
    with pg(creds).begin() as conn:
        data.to_sql('harga_saham_harian', conn, if_exists='append', index=False)

