# encoding: utf-8
'''
如何基本輸出  1. print  2. logging module
'''
from __future__ import print_function
import logging


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''

    # print()
    print('watermelon')
    print('傷心的人別聽慢歌')  # utf-8 string

    # logging module
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # 只輸出INFO & 以上等級的log

    logger.debug('apple')  # debug 等級的log 將不會被輸出
    logger.info('banana')
    logger.warning('grape')
    logger.error('lemon')
    logger.critical('後來的我們')  # utf-8 string

    return None


if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
