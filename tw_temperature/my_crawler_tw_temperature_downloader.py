# encoding: utf-8
'''
抓氣象局的台灣北部各地即時溫度 1/3
下載氣溫資料,並存檔原始資料到S3
'''
from __future__ import print_function
import logging
import time
import boto3
from botocore.vendored import requests


# logging module
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)  # 只輸出INFO & 以上等級的log

#  GLOBAL CONSTANT
OBJ_S3 = boto3.resource('s3')
S_BUCKET = 'test.temperature'  # 放資料的s3 bucket,需先建好


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''

    lst_area = [
        'ObsN',  # 北
        'ObsC',  # 中
        'ObsS',  # 南
        'ObsE',  # 東
        'ObsI'  # 離島
    ]
    for s_area in lst_area:
        # URL
        s_url = r'http://www.cwb.gov.tw/V7/observe/real/%s.htm' % s_area

        # Fetch
        s_content = fetcher(s_url)

        # Upload to S3
        if s_content is not None:
            # 檔名為unix_timestamp命名
            s_object_name = '%s.htm' % int(time.time() * 1000)

            # upload
            s3_put_object_from_string(
                s_content, s_object_name)  # 上傳檔案_來源: 字串

    return None


def fetcher(s_url):
    '''
    回傳指定url的 body內容

    '''
    # Make a Request
    req = requests.get(s_url, verify=False)
    req.encoding = 'utf-8'
    s_content = req.text

    # return content
    if req.status_code == 200:
        return s_content
    else:
        # 抓網頁失敗的處理
        LOGGER.warning('request fail ! status_code:' + str(req.status_code))
        return None

    return None


def s3_put_object_from_string(s_str, s_object_name):
    '''
    上傳到S3,  來源: 字串
    '''
    # string
    s_str = s_str.encode('utf-8')

    # upload
    OBJ_S3.Object(S_BUCKET, s_object_name).put(Body=s_str)

    # log
    LOGGER.info('object_name: ' + s_object_name)
    return None

if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
