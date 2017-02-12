# encoding: utf-8
'''
使用 boto3 lib, 對 S3 做一些基本操作

'''
from __future__ import print_function
import codecs
import boto3

#  GLOBAL CONSTANT
OBJ_S3 = boto3.resource('s3')
S_BUCKET = 'test.bucket.9487'  # 測試的s3 bucket,需先建好


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''

    # 產生一個測試檔案 , 待會上傳會用到
    s_local_file = r'/tmp/test.txt'
    write_test_file(s_local_file)


    # -- 各種對s3的操作 --

    # bucket
    s3_list_buckets()  # 列出所有bucket

    # 上傳檔案
    # 預期結果: s3新增兩個檔案,分別為 F1_from_local.txt , F2_from_string.txt
    s3_put_object_from_local(s_local_file)  # 上傳來源: 本地檔案
    s3_put_object_from_string()  # 上傳來源: 字串

    # 下載檔案
    # 預期結果: 下載剛才上傳的兩個檔案, 前者 F1_from_local.txt 會存檔,
    #           後者F2_from_string.txt會直接print()出來
    s3_download_file()  # 下載到檔案
    s3_download_to_string()  # 下載到字串

    return None


def write_test_file(s_filename):
    '''
    先產生一個測試檔案 , 待會上傳會用到
    '''
    s_out = '測試,謝謝大家'
    file = codecs.open(s_filename, 'w', "utf-8")
    file.write(s_out)
    file.close()

    return None


def s3_list_buckets():
    '''
    列出所有bucket
    '''
    for bucket in OBJ_S3.buckets.all():
        print(bucket.name)

    return None


def s3_download_file():
    '''
    下載 S3 object , 並且寫檔
    '''
    s_object_name = 'F1_from_local.txt'
    s_output_file = r'/tmp/F1_from_local.txt'

    OBJ_S3.Bucket(S_BUCKET).download_file(s_object_name, s_output_file)

    return None


def s3_download_to_string():
    '''
    下載 S3 object ,然後不寫檔, 直接轉成字串
    '''
    s_object_name = 'F2_from_string.txt'

    obj = OBJ_S3.Object(S_BUCKET, s_object_name)
    s_out = obj.get()['Body'].read().decode('utf-8')

    print(s_out)
    # return s_out

    return None


def s3_put_object_from_local(s_local_file):
    '''
    上傳到S3
    來源: 本地檔案 (s_local_file)
    '''
    # target filename
    s_object_name = 'F1_from_local.txt'

    # upload
    OBJ_S3.Object(S_BUCKET, s_object_name).put(
        Body=open(s_local_file, 'rb'))

    return None


def s3_put_object_from_string():
    '''
    上傳到S3,  來源: 字串
    '''
    # target filename
    s_object_name = 'F2_from_string.txt'

    # string
    s_str = "謝謝大家".encode('utf-8')

    # upload
    OBJ_S3.Object(S_BUCKET, s_object_name).put(Body=s_str)

    return None


if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
