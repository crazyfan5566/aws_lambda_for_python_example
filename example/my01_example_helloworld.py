# encoding: utf-8
'''
Hello World
'''
from __future__ import print_function
from time import sleep
import random


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''

    # sleep() 0~5秒之間的亂數
    # 由於lambda 函數預設 3秒後就 timeout ,所以執行下去會有機會失敗
    # 請觀察 dashboard 執行失敗的統計圖表

    f_rnd = random.random() * 5
    print('random sleep time : %s ' % f_rnd)
    sleep(f_rnd)

    return "Hello World!"


if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
