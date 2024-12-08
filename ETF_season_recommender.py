import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup
import itertools 
from linebot import LineBotApi
from linebot.models import TextSendMessage, ImageSendMessage
from linebot.exceptions import LineBotApiError
import logging as log
import datetime
import telegram
import configparser
import core.MessageSender as MessageSender
import logging

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

config = configparser.ConfigParser()
config.read('config.ini')
logging.basicConfig(filename='binance_trade.log', level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logging.getLogger().addHandler(logging.StreamHandler())

def main():
    df_0050 = get_0050()
    df_0056 = get_0056()
    df_006203 = get_006203()
    df_00850 = get_00850()
    df_00701 = get_00701()
    df_00692 = get_00692()
    frames = [df_0050, df_0056, df_006203, df_00850, df_00701, df_00692]
    
    all_combinations_result = combination_cal(frames, 5)
    all_best_result = combination_cal(frames, 6)
    
    datetime_dt = datetime.datetime.today()
    datetime_str = datetime_dt.strftime("%Y/%m/")
    message = "---------------------------\n"
    message += "[" + datetime_str + "] 本季熱門存股：\n"
    message += all_combinations_result.reset_index().to_string(header=False, index=False) + '\n'
    message += "--------------------------\n"
    message += "[" + datetime_str + "] 本季最佳存股推薦：\n"
    message += all_best_result.reset_index().to_string(header=False, index=False) + '\n'
    message += "--------------------------\n"
    sendMessage(message)


def get_00701():
    url = 'https://www.cathaysite.com.tw/funds/etf/pcf.aspx?fc=AD'
    dfo = pd.read_html(url)[4]
    df=dfo['股 票']
#     df['股票代號'] = pd.to_numeric(df['股票代號'])
#     df.dropna()
    df = df.rename(columns={"股票名稱": "name", "股票代號": "stock_id"})
    df = df.drop(df.columns[3], axis=1)
    df = df.drop(columns=['股數'], axis=1)
#     df.set_index(df.columns[0] , inplace=True)
    df.set_index('stock_id' , inplace=True)
    return df

def get_0050():
    url = "https://www.yuantaetfs.com/api/Composition?fundid=1066"
    df = pd.read_json(url)
    df = df.drop(columns=['cashinlieu', 'minimum', 'ename', 'qty'], axis=1)
    df = df.rename(columns={"stkcd": "stock_id"})
#     df.set_index('stkcd' , inplace=True)
    df.set_index('stock_id' , inplace=True)
    return df

def get_0056():
    url = "https://www.yuantaetfs.com/api/Composition?fundid=1084"
    df = pd.read_json(url)
    df = df.drop(columns=['cashinlieu', 'minimum', 'ename', 'qty'], axis=1)
    df = df.rename(columns={"stkcd": "stock_id"})
#     df.set_index('stkcd' , inplace=True)
    df.set_index('stock_id' , inplace=True)
    return df

def get_006203():
    url = "https://www.yuantaetfs.com/api/Composition?fundid=1103"
    df = pd.read_json(url)
    df = df.drop(columns=['cashinlieu', 'minimum', 'ename', 'qty'], axis=1)
    df = df.rename(columns={"stkcd": "stock_id"})
#     df.set_index('stkcd' , inplace=True)
    df.set_index('stock_id' , inplace=True)
    return df

def get_00850():
    url = "https://www.yuantaetfs.com/api/StkWeights?date=&fundid=1198"
    df = pd.read_json(url)
    df = df.drop(columns=['ename', 'ym', 'qty', 'weights'], axis=1)
    df = df.rename(columns={"code": "stock_id"})
#     df.set_index('code' , inplace=True)
    df.set_index('stock_id' , inplace=True)

    return df

def get_00692():
    url = 'https://websys.fsit.com.tw/FubonETF/Trade/FundInvest.aspx?stock=00692&lan=TW'
    dfo = pd.read_html(url)[2]
    dfo.drop(dfo.tail(1).index,inplace=True)
    df = dfo['股票']
    df['股票代碼'] = pd.to_numeric(df['股票代碼'])
    df = df.rename(columns={"股票名稱": "name", '股票代碼': 'stock_id'})
    df = df.drop(columns=['股數', '金額', '權重(%)'], axis=1)
    df.set_index('stock_id' , inplace=True)
    return df

def combination_cal(frames, count):
    all_combinations_result = pd.DataFrame()
    combinations_object = itertools.combinations(frames, count)
    combinations_list = list(combinations_object)
    for r in range(len(combinations_list)):
        combinations_result = pd.concat(combinations_list[r], axis=1, join='inner')
        combinations_result = combinations_result.loc[:,~combinations_result.columns.duplicated()]
        if len(all_combinations_result) == 0:
            all_combinations_result = combinations_result
        else :
            all_combinations_result = pd.merge(all_combinations_result, combinations_result, left_on=['stock_id', 'name'], right_on=['stock_id', 'name'], how="outer")
    all_combinations_result = all_combinations_result.loc[:,~all_combinations_result.columns.duplicated()]
    return all_combinations_result

def sendMessage(msg):
    messageSender = None
    try:
        messageSender = MessageSender.MessageSender(configPath='./core/MessageSender.cfg')
        messageSender.sendMessageToMq(msg)
    except Exception as e:
        logging.error("sendMessage err:" + str(e))
    finally:
        if messageSender is not None:
           messageSender.Stop()
    
        
if __name__ == "__main__":
    main()