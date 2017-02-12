# encoding: utf-8
'''
* 抓氣象局的台灣北部各地即時溫度 3/3
* Query DynamoDB ,並且產生 json 檔案

'''
from __future__ import print_function
import logging
import codecs
import boto3
from boto3.dynamodb.conditions import Key, Attr

# logging module
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)  # 只輸出INFO & 以上等級的log

#  GLOBAL CONSTANT
OBJ_S3 = boto3.resource('s3')
OBJ_DDB = boto3.resource('dynamodb', region_name='ap-southeast-1')
OBJ_TBL = OBJ_DDB.Table('tw_temperature')  # DynamoDB Table
S_BUCKET = 'test.temperature'  # 放資料的s3 bucket,需先建好


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''
    lst_area = [
        'A0Z08',  # 合歡山
        '46692',  # 台北
        '46741',  # 台南
        '46699',  # 花蓮
        'C0R36',  # 墾丁
        '46810'  # 東沙島
    ]

    for area_id in lst_area:
        # Query Table
        lst_data = dynamodb_query(area_id)

        # 產生 json 檔案,存到 /tmp 之下
        process_data(lst_data, area_id)

        # 上傳到S3, 並設定為 public 屬性以供下載
        s3_put_object_from_local(area_id)
        s3_object_acl_public(area_id)

    return None


def dynamodb_query(area_id):
    '''
    依條件查詢
    http://boto3.readthedocs.io/en/latest/guide/dynamodb.html#querying-and-scanning
    '''

    response = OBJ_TBL.query(
        KeyConditionExpression=Key('area_id').eq(area_id),
        ScanIndexForward=False,  # sort
        Limit=150
    )

    return response['Items']


def process_data(lst_data, area_id):
    '''
    產生 json 格式溫度資料
    '''
    lst_final = []
    for item in lst_data:
        time_stamp = int(item['time_stamp']) * 1000
        temperature = float(item['temperature'])
        area_name = item['area_name']
        lst_item = [time_stamp, temperature]

        lst_final.append(lst_item)

    s_json = '{"label":"<area_name>", "data": <s_json>}'
    s_json = s_json.replace('<s_json>', str(lst_final))
    s_json = s_json.replace('<area_name>', area_name)

    # write file
    write_file(s_json, '/tmp/temperature_%s.json' % area_id)

    return None


def s3_put_object_from_local(area_id):
    '''
    上傳到S3
    來源: 本地檔案
    '''

    # local filename
    s_local_file = r'/tmp/temperature_%s.json' % area_id

    # upload
    s_object_name = 'temperature_%s.json' % area_id
    OBJ_S3.Object(S_BUCKET, s_object_name).put(
        Body=open(s_local_file, 'rb'))

    return None


def s3_object_acl_public(area_id):
    '''
    設定 s3物件成為public 屬性
    '''
    s_object_name = 'temperature_%s.json' % area_id

    object_acl = OBJ_S3.ObjectAcl(S_BUCKET, s_object_name)
    object_acl.put(ACL='public-read')

    return None


def write_file(s_out, s_filename, mode='w'):
    '''
    寫檔 , 預設是 overwrite mode
    '''

    file = codecs.open(s_filename, mode, "utf-8")
    file.write(s_out)
    file.close()

    return None

if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
