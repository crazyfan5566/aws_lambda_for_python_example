# encoding: utf-8
'''
使用 boto3 裡面的 requests lib 抓網頁
http://docs.python-requests.org/
'''
from __future__ import print_function
from botocore.vendored import requests


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''

    s_url = 'https://www.google.com'

    # disable the security certificate check
    requests.packages.urllib3.disable_warnings()

    # Make a Request
    req = requests.get(s_url, verify=False)

    # dump
    print('status_code : %s ' % req.status_code)
    print('content-type: %s ' % req.headers['content-type'])
    print('encoding    : %s ' % req.encoding)
    print('text length : %s ' % len(req.text))
    print('text        : %s ' % req.text[0:2000])

    return None


if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
