
# Inisiasi library
import requests
import pandas as pd
import time
from datetime import datetime, date, timedelta
import re
import json
import os
from sqlalchemy import create_engine

from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options



def pg_engine(creds):
    return create_engine(
        "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            creds['user'],
            creds['pass'],
            creds['host'],
            creds['port'],
            creds['db']
        ),
    )



def get_scrap_data(url):
    # client = docker.
    # container = client.containers.get('selenium')
    # ip_add = container.attrs['NetworkSettings']['IPAddress']
    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=1920x1080")
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    options.add_argument(f'user-agent={user_agent}')
    driver =  webdriver.Remote(
                    command_executor="http://selenium:4444/wd/hub",
                    desired_capabilities=DesiredCapabilities.CHROME,
                    options=options)
    
    with driver as browser:
        # Menggunakan Selenium untuk membuka website
        browser.get(url)

        # Mengambil semua text yang ada di webpage
        content = browser.find_element(By.TAG_NAME, "pre").text

        # Mengubah text ke dalam bentuk json
        parsed_json = json.loads(content)
        
        return parsed_json

def getdate(n):
    # Mengambil tanggal hari ini
    today = date.today()
    # Mengambil tanggal hari kerja terakhir (termasuk hari ini)
    if date.weekday(today) == 5:
        akhir = today - timedelta(days=1)
    elif date.weekday(today) == 6:
        akhir = today - timedelta(days=2)
    else:
        akhir = today
    # Mengambil tanggal 5 hari kerja sebelumnya (termasuk hari ini)
    awal = (akhir - pd.tseries.offsets.BDay(n)).date()
    # Mengubah format datetime ke dalam bentuk string
    awal = awal.strftime("%Y_%m_%d")
    akhir = akhir.strftime("%Y_%m_%d")
    return (awal, akhir)

def getcookie():
    with requests.session():
        # deklarasi header untuk request
        header = {'Connection': 'keep-alive',
                  'Expires': '-1',
                  'Upgrade-Insecure-Requests': '1',
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) \
                    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
                  }

        # Mengunjungi website
        website = requests.get('https://finance.yahoo.com/quote/AALI.JK?p=AALI.JK&.tsrc=fin-srch', headers=header)

        # Mengambil crumb
        crumb = re.findall('"CrumbStore":{"crumb":"(.+?)"}', str(website.text))[0]

        # Mengambil cookies
        cookies = website.cookies

        # #Mengambil jam buka dan jam tutup bursa saham
        website = requests.get('https://query1.finance.yahoo.com/v8/finance/chart/AALI.JK?&range=5d&useYfid=true',                                   '&interval=1m&includePrePost=true&events=div|split|earn&lang=en-US&region=US&crumb='+crumb+
                                  '&corsDomain=finance.yahoo.com', headers = header, cookies = cookies).text
        site_json=json.loads(website)
        
        tradingperiod = site_json['chart']['result'][0]['meta']['tradingPeriods']['regular'][0]
        
        return (header, cookies, crumb, tradingperiod)

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