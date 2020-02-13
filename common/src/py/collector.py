import pandas as pd
import requests
import time
import multiprocessing as mp
from multiprocessing import Queue
import os
import json

import os
from os import listdir
from os.path import isfile, join
import logging
import traceback
logger = logging.getLogger(__name__)

def download_one(url, symbol, dest_dir, period1=0, period2=9999999999, interval='1m', includePrePost=True):
    try:
        url = url + symbol + '?symbol=' + symbol + '&period1=' + str(period1) + \
              '&period2=' + str(period2) + \
              '&interval=' + str(interval)
        if includePrePost:
            url += '&includePrePost=true'
        resp = requests.get(url)
        f = open(dest_dir + '/' + symbol + ".json", "w")
        f.write(resp.text)
        f.close()
    except Exception as e:
        print ('ERROR while downloading symbol: ' + symbol)

class collector:

    def __init__(self, symbols, base_url='https://query1.finance.yahoo.com/v8/finance/chart/'):
        self.base_url = base_url
        self.symbols = symbols
        self.pool = mp.Pool(mp.cpu_count())

    def collect(self,interval, window_in_days, dest_dir):
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        now = int(time.time())
        period2 = now
        period1 = now - window_in_days * 24 * 60 * 60
        self.collect_in_parallel(period1, period2, interval, dest_dir)

    def collect_in_parallel(self, period1, period2, interval, dest_dir):
        futures = []
        for symbol in self.symbols:
            url = self.base_url
            future = self.pool.apply_async(download_one, args=[url, symbol, dest_dir, period1, period2, interval])
            futures.append(future)
        count = 0
        for future in futures:
            future.get()
            count = count + 1
            if count % 100 == 0:
                print ('completed  ' + str(count) + ' of ' + str(len(self.symbols)))
        print ('completed all collection')

        self.pool.close()

Q = Queue()

def process_data(raw_file_path, dest_file):
    data = json.load(open(raw_file_path, 'r'))
    #print ('processing ' + raw_file_path )
    try:
        result_dict = {
            "symbol": data['chart']['result'][0]['meta']['symbol'],
            "prev_close": data['chart']['result'][0]['meta']['chartPreviousClose'],
            "timestamp": data['chart']['result'][0]['timestamp'],
            "open": data['chart']['result'][0]['indicators']['quote'][0]['open'],
            "low": data['chart']['result'][0]['indicators']['quote'][0]['low'],
            "close": data['chart']['result'][0]['indicators']['quote'][0]['close'],
            "high": data['chart']['result'][0]['indicators']['quote'][0]['high'],
            "volume": data['chart']['result'][0]['indicators']['quote'][0]['volume']
        }
        df = pd.DataFrame(result_dict)
        df = df.dropna()
        df.to_csv(dest_file)
        print('processed data for ' + str(df['symbol'].unique()))
    except Exception as ex:
        print ('ERROR while processing data')
        traceback.print_exc()
    return


def merge():
    merged_df = None
    while True:
        a_df = Q.get()
        if merged_df is None:
            merged_df = a_df
        else:
            merged_df.append(a_df)
        if Q.empty() == True:
            break
    return merged_df

def convert(raw_file_dir, dest_dir):
    df = None
    print ('started converting raw files to dataframe ' + raw_file_dir)
    pool = mp.Pool(mp.cpu_count())
    files = [f for f in listdir(raw_file_dir) if isfile(join(raw_file_dir, f))]
    make_dir_if_not_exists(dest_dir)

    print ('there are ' + str(len(files)) + ' to process')
    for file in files:
        try:
            pool.apply_async(process_data, args=[join(raw_file_dir, file), join(dest_dir, file + ".csv")])
        except Exception as ex:
            logger.error('ERROR while converting file ' + file)
    pool.close()

    pool.join()
    print ('closing pool and merging data')


#Not to be used on huge files - will crash
def convert_to_df(raw_file_dir):
    files = [f for f in listdir(raw_file_dir) if isfile(join(raw_file_dir, f))]
    result_df = None
    for file in files:
        data = json.load(open(join(raw_file_dir, file), 'r'))

        result_dict = {
            "symbol": data['chart']['result'][0]['meta']['symbol'],
            "prev_close": data['chart']['result'][0]['meta']['chartPreviousClose'],
            "timestamp": data['chart']['result'][0]['timestamp'],
            "open": data['chart']['result'][0]['indicators']['quote'][0]['open'],
            "low": data['chart']['result'][0]['indicators']['quote'][0]['low'],
            "close": data['chart']['result'][0]['indicators']['quote'][0]['close'],
            "high": data['chart']['result'][0]['indicators']['quote'][0]['high'],
            "volume": data['chart']['result'][0]['indicators']['quote'][0]['volume']
        }
        df = pd.DataFrame(result_dict)
        if result_df is None:
            result_df = df
        else:
            result_df = result_df.append(df)

    return result_df


def enrich(df):
    df['diff'] = df['close'] - df['prev_close']
    df['diff'] = df['diff'].divide(df['prev_close'])
    df['diff'] = df['diff'] * 100
    # categorize
    df.loc[(df['diff'] > 0) & (df['diff'] < 2), 'category'] = 'winner_2pc'
    df.loc[(df['diff'] >= 2) & (df['diff'] < 5), 'category'] = 'winner_5pc'
    df.loc[(df['diff'] >= 5) & (df['diff'] < 8), 'category'] = 'winner_8pc'
    df.loc[(df['diff'] >= 8) & (df['diff'] < 10), 'category'] = 'winner_10pc'
    df.loc[df['diff'] >= 10, 'category'] = 'outperformers'
    df.loc[df['diff'] < 0, 'category'] = 'loser'
    return df

def make_dir_if_not_exists(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

def concat(src_csv_dir):
    filepaths = [join(src_csv_dir,f) for f in listdir(src_csv_dir) if f.endswith('.csv')]
    return pd.concat(map(pd.read_csv, filepaths))