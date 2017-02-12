# encoding: utf-8
'''
* 使用 boto3 lib, 對 dynamoDB 做基本的CRUD操作
* 首先準備:
    dynamoDB 新建一個新Table
    Table 名稱: Music
    欄位名稱(Primary)使用兩個:
        1. Partition key : Artist (字串)
        2. Sort key      : SongTitle (字串)

* 場景說明: 一個歌手-歌曲的資料庫, 另外也記錄了收聽次數
* 預期結果:
    1. 新增一筆 '五月天-傷心的人別聽慢歌-0'的資料
    2. 將收聽次數從 0 改成 100
    3. 將這筆資料讀出來
    4. 刪掉這筆資料
'''
from __future__ import print_function
import boto3
from boto3.dynamodb.conditions import Key, Attr


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''

    # init
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1')
    table = dynamodb.Table('Music')

    # --- CRUD ---
    # Create
    dynamodb_put_item(table)  # 寫入1筆
    dynamodb_batch_writer(table)  # 批次寫入50筆

    # Update
    dynamodb_update_item(table)  # update

    # Read
    dynamodb_get_item(table)  # 知道pkey的情況下直接取出1筆
    dynamodb_query(table)  # 取得近10筆資料

    # Delete
    dynamodb_delete_item(table)  # 刪除一筆資料

    return None


def dynamodb_query(table):
    '''
    依條件查詢
    http://boto3.readthedocs.io/en/latest/guide/dynamodb.html#querying-and-scanning

    注意要import這行:
    from boto3.dynamodb.conditions import Key, Attr
    '''

    response = table.query(
        KeyConditionExpression=Key('Artist').eq('五月天'),
        ScanIndexForward=True,  # sort
        Limit=10
    )

    print(response['Items'])  # dump
    print(response['Items'][0])  # first record

    return None


def dynamodb_batch_writer(table):
    '''
    批次寫入
    https://boto3.readthedocs.io/en/latest/guide/dynamodb.html#batch-writing
    可以減少連線開銷
    '''
    with table.batch_writer() as batch:
        for i in range(50):
            batch.put_item(
                Item={
                    'Artist': '五月天',
                    'SongTitle': '歌曲' + str(i),
                    'view_cnt': 0

                }
            )

    return None


def dynamodb_put_item(table):
    '''
    Creating a New Item
    '''

    s_artist = '五月天'
    s_title = '傷心的人別聽慢歌'

    table.put_item(
        Item={
            'Artist':  s_artist,
            'SongTitle': s_title,
            'view_cnt': 0
        }
    )

    return None


def dynamodb_update_item(table):
    '''
    Updating Item
    '''
    s_artist = '五月天'
    s_title = '傷心的人別聽慢歌'

    table.update_item(
        Key={
            'Artist':  s_artist,
            'SongTitle': s_title,
        },
        UpdateExpression='SET view_cnt = :val_1',
        ExpressionAttributeValues={
            ':val_1': 100
        }
    )

    return None


def dynamodb_get_item(table):
    '''
    Getting an Item
    '''

    s_artist = '五月天'
    s_title = '傷心的人別聽慢歌'

    response = table.get_item(
        Key={
            'Artist':  s_artist,
            'SongTitle': s_title,
        }
    )
    item = response['Item']
    print(item)

    return None


def dynamodb_delete_item(table):
    '''
    Deleting Item
    '''

    s_artist = '五月天'
    s_title = '傷心的人別聽慢歌'

    table.delete_item(
        Key={
            'Artist':  s_artist,
            'SongTitle': s_title,
        }
    )

    return None


if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
