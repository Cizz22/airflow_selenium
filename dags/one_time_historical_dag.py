import os
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import logging
import sys
sys.path.insert(0, '/opt/airflow')

from sqlalchemy import create_engine
from one_time_historical.extract_dim_saham import createStockListTable, insertStockListData
from one_time_historical.extract_dim_sekuritas import createBrokerListTable, insertBrokerListData
from one_time_historical.extract_dim_perusahaan import createCompanyList, insertCompanyList
from one_time_historical.extract_fact_daily_price import createStockPriceDailyTable,insertStockPriceDaily
from one_time_historical.extract_fact_broker_summary import createBrokerSummary, insertBrokerSummary
from one_time_historical.count_fact_fluktuasi import createFluktuasiTable, insertFluktuasiData

from one_time_historical.creds import creds
from one_time_historical.utils import pg_engine as pg


def drop_table():
    pg(creds).connect().execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')

default_args = {
    'owner': 'kcb kelompok',
    # 'wait_for_downstream': True,
    'start_date': datetime(2022, 11, 27),
    'end_date': datetime(2022, 11, 30),
    'retries': 3,
    'retries_delay': timedelta(minutes=5),
    'trigger_rule': 'all_success'
    }

with DAG(
    dag_id='saham_one_time_historical',
    start_date=pendulum.datetime(2022, 11, 27, tz="UTC"),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    max_active_runs=1
) as dag:
    #start
    start = DummyOperator(task_id='start')
    
    #drop table
    drop_all_table = PythonOperator(
        python_callable = drop_table,
        task_id = 'drop_all_table'
    )
    
    #create table 
    create_table_saham = PythonOperator(
        python_callable=createStockListTable,
        task_id='create_table_saham'
    )
    
    create_table_perusahaan = PythonOperator(
        python_callable=createCompanyList,
        task_id='create_table_perusahaan'
    )
    
    create_table_sekuritas = PythonOperator(
        python_callable=createBrokerListTable,
        task_id='create_table_sekuritas'
    )
    
    create_table_harga_saham = PythonOperator(
        python_callable=createStockPriceDailyTable,
        task_id='create_table_harga_saham'
    )
    
    create_table_broker_summary = PythonOperator(
        python_callable=createBrokerSummary,
        task_id='create_table_broker_summary'
    )
    
    create_fluktuasi_table = PythonOperator(
        python_callable=createFluktuasiTable,
        task_id='create_fluktuasi_table'
    )
    
    #insert data
    insert_table_saham = PythonOperator(
        python_callable=insertStockListData,
        task_id='insert_table_saham'
    )
    
    insert_table_perusahaan = PythonOperator(
        python_callable=insertCompanyList,
        task_id='insert_table_perusahaan'
    )
    
    insert_table_sekuritas = PythonOperator(
        python_callable=insertBrokerListData,
        task_id='insert_table_sekuritas'
    )
    
    insert_table_harga_saham = PythonOperator(
        python_callable=insertStockPriceDaily,
        task_id='insert_table_harga_saham'
    )
    
    insert_table_broker_summary = PythonOperator(
        python_callable=insertBrokerSummary,
        task_id='insert_table_broker_summary'
    )
    
    insert_table_fluktuasi = PythonOperator(
        python_callable=insertFluktuasiData,
        task_id='insert_table_fluktuasi'
    )

    end = DummyOperator(task_id='end')


start >> drop_all_table
drop_all_table >> [create_table_saham, create_table_sekuritas]  
create_table_saham >> [insert_table_saham, create_table_perusahaan, create_table_harga_saham, create_fluktuasi_table]
create_table_sekuritas >> [create_table_broker_summary, insert_table_sekuritas]

create_table_perusahaan >> insert_table_perusahaan >> end
[create_table_broker_summary, insert_table_sekuritas] >> insert_table_broker_summary >> end

[create_table_harga_saham, insert_table_saham] >> insert_table_harga_saham 

[insert_table_harga_saham, create_fluktuasi_table] >> insert_table_fluktuasi >> end





