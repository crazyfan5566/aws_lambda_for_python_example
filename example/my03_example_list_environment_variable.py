# encoding: utf-8
'''
探索 lambda 執行環境, 列出所有環境變數
預期可看到一些 lib的路徑, security credentials之類
'''
from __future__ import print_function
import os


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''

    lst_env_var = os.environ.keys()

    for key in lst_env_var:
        print(key, os.environ[key])

    return None


if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
