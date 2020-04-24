import numpy as np
import pandas as pd

import time
import datetime
import threading
import redis

import tushare as ts
from concurrent import futures
from utils.time_util import time_cut

red = redis.Redis(host='localhost')

stock_basics = ts.get_stock_basics()
tickers = stock_basics[(stock_basics['name'].str.find('ST')==-1) & (stock_basics['timeToMarket']>19900101) & (stock_basics['timeToMarket']<20190101)
            & (stock_basics['npr']>0)].index.values.tolist()

MAX_WORKERS = 30
executor = futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
def get_realtime_quotes(tickers):
    N = (len(tickers)+MAX_WORKERS-1)//MAX_WORKERS
    tickers_list = [tickers[i*N:min(i*N+N, len(tickers))] for i in range((len(tickers)+N-1)//N)]
    results = executor.map(ts.get_realtime_quotes, tickers_list)
    df = pd.concat(results)
    df['time'] = df['time'].apply(time_cut)
    return df.reset_index()

def run_get_realtime_quotes(tickers):
    cur_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur_date = cur_datetime[0:10]
    am_start_time = cur_date + ' 09:25:00'
    am_end_time = cur_date + ' 11:30:00'
    pm_start_time = cur_date + ' 13:00:00'
    pm_end_time = cur_date + ' 15:00:00'
    ingest_time = am_start_time
    if cur_datetime < am_start_time:
        print('no open market!')
        return
    if len(red.keys('t_tick_'+pm_end_time)) > 0 and cur_datetime > pm_end_time:
        print('ingest over all tick!')
        return
    df_buffer = None
    while ingest_time < pm_end_time:
        cur_datetime = datetime.datetime.now()
        try:
            df = get_realtime_quotes(tickers)
        except Exception as e:
            if cur_datetime>pd.to_datetime(pm_end_time):
                break
            print('time out!')
            continue
        date_tick = df.iloc[0]['date']
        time_tick = df.iloc[0]['time']
        if date_tick is None:
            date_tick = cur_date
        if time_tick is None:
            time_tick = cur_datetime.strftime('%H:%M:%S')
        time_tick = time_cut(time_tick)
        date_time = date_tick + ' ' + time_tick
        df['datetime'] = pd.to_datetime(date_time)
        df = df[['code', 'datetime', 'price', 'volume', 'amount']].dropna()
        if df_buffer is None:
            df_buffer = df
        elif date_time[:-3] == ingest_time[:-3] or date_time[-2:] == '00':
            df_buffer = pd.concat([df_buffer, df])
        if date_time[:-3] > ingest_time[:-3]:
            key = 't_tick_' + date_time[:-3] +':00'
            df_tick_bytes = df_buffer.to_msgpack()
            red.set(key, df_tick_bytes)
            df_buffer = df
            if date_time[-2:] == '00':
                df_buffer = None
            print(ingest_time + '  has ingest into table ' + key + '!')
            ingest_time = date_time
        if ingest_time >= am_end_time and ingest_time < pm_start_time:
            print('Noon break time!')
            time.sleep((pd.to_datetime(pm_start_time)-datetime.datetime.now()).total_seconds())
            continue
            
run_get_realtime_quotes(tickers)