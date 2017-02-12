# encoding: utf-8
'''
* 抓氣象局的台灣北部各地即時溫度 2/3
* 收到S3更新的event, 解析爬回來的檔案,存入DynamoDB
* DynamoDB 先建好以下Table:
    Table       : tw_temperature
    Primary Key : area_id(string) +
                  time_stamp (Number)
'''
from __future__ import print_function
import re
import time
import datetime
import logging
import boto3


# logging module
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)  # 只輸出INFO & 以上等級的log

#  GLOBAL CONSTANT
OBJ_S3 = boto3.resource('s3')
OBJ_DDB = boto3.resource('dynamodb', region_name='ap-southeast-1')
OBJ_TBL = OBJ_DDB.Table('tw_temperature')


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''

    # 從 s3產生出來的event 取出我們要的原始網頁
    s_content = get_object_content(event)

    # 解析網頁內容
    lst_data = parse_content(s_content)

    # 逐筆存到DynamoDB
    for item in lst_data:
        OBJ_TBL.put_item(
            Item={
                'area_id': item[0],
                'time_stamp': item[1],
                'area_name': item[2],
                'datetime': item[3],
                'temperature': item[4]
            }
        )

    return None


def parse_content(s_content):
    '''
        解析網頁內容,預期最後處理成這樣的格式:
        ['46692', 1486553400, '臺北', '2017/02/08 19:30:00', '16.8'],[.....
    '''
    # 重新切行
    s_content = s_content.replace('\n', '')
    lst_content = s_content.split('</tr>')

    lst_final = []
    for s_line in lst_content:

        # 第一次過濾: 每行有 <tr>...font 這樣的字串
        s_pattern = r'<tr>(.*)font'
        s_match = re.search(s_pattern, s_line)
        if s_match is None:
            continue
        else:
            s_line = s_match.group(0)


        # 二次過濾:檢查td欄位數
        lst_line = s_line.split('<td')
        cols = 16
        if len(lst_line) == cols:
            # ok
            # print(s_line)
            pass
        else:
            # fail, html table format changed?
            LOGGER.warning('欄位數有誤,請檢查是否Table格式改變')
            LOGGER.warning(s_line)
            continue

        # 至此,每一行應該都是合格的資料,等待切欄位parse

        # 1. areaid
        s_pattern = r'>(.*)<'
        s_match = re.search(s_pattern, lst_line[1])
        if s_match is None:
            continue
        else:
            s_areaid = s_match.group(0)[1:-1]

        # 2.city
        s_pattern = r'\'>(.*)</a'
        s_match = re.search(s_pattern, lst_line[2])
        if s_match is None:
            continue
        else:
            s_city = s_match.group(0)[2:-3]

        # 3.data time (一般格式 & unix timesteamp格式)
        current = datetime.datetime.now() + datetime.timedelta(hours=8)  # UTCnow() + 8 hrs
        yyyy = str(current)[0:4]

        s_datatime = yyyy + '/' + lst_line[3][1:-5] + ':00'

        d_datetime = datetime.datetime.strptime(
            s_datatime, "%Y/%m/%d %H:%M:%S")
        i_unixtimestamp = int(time.mktime(d_datetime.timetuple()))

        # 4.temperature
        s_pattern = r'>[-]?[0-9]*\.[0-9]*<'  # 正負浮點數
        s_match = re.search(s_pattern, lst_line[4])
        if s_match is None:
            # 有可能因設備故障,溫度無資料,跳過不處理
            continue
        else:
            s_temperature = s_match.group(0)[1:-1]

        lst_row = [s_areaid, i_unixtimestamp,
                   s_city, s_datatime, s_temperature]

        lst_final.append(lst_row)

    LOGGER.info('total records:' + str(len(lst_final)))
    return lst_final


def get_object_content(event):
    '''
    從 s3產生出來的event 取出我們需要處理的那個檔案
    '''
    # 從event 取得檔名/bucket等資訊
    dict_event = event['Records'][0]
    s_object_name = dict_event['s3']['object']['key']
    s_bucket = dict_event['s3']['bucket']['name']

    # download file & convert to string
    s_content = s3_download_to_string(s_bucket, s_object_name)

    return s_content


def s3_download_to_string(s_bucket, s_object_name):
    '''
    下載 S3 object ,然後不寫檔, 直接轉成字串
    '''

    obj = OBJ_S3.Object(s_bucket, s_object_name)
    s_out = obj.get()['Body'].read().decode('utf-8')

    return s_out


if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
