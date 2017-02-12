# encoding: utf-8
'''
* 測試寫檔案到 /tmp,這是lambda唯一允許寫入的目錄
* 此程式是遞迴取得檔案清單,然後寫入成檔案
'''
from __future__ import print_function
import os
import codecs


def lambda_handler(event, context):
    '''
    entry point (lambda)
    '''

    # get file list
    s_folder = r'/var'
    s_output = str(get_file_list_recursively(s_folder))

    # write file
    s_output_file = r'/tmp/filelist.txt'
    write_file(s_output, s_output_file)

    # get file list /tmp to check
    s_folder = r'/tmp'
    s_output = str(get_file_list_recursively(s_folder))
    print(s_output)

    return None


def write_file(s_out, s_filename, mode='w'):
    '''
    寫檔 , 預設是 overwrite mode
    '''

    file = codecs.open(s_filename, mode, "utf-8")
    file.write(s_out)
    file.close()

    return None


def get_file_list_recursively(s_folder):
    '''
    遞迴取得檔案列表
    '''

    lst_files = []

    for (s_dir, _, files) in os.walk(s_folder):
        for s_file in files:
            path = os.path.join(s_dir, s_file)
            if os.path.exists(path):
                lst_files.append(path)

    return lst_files

if __name__ == '__main__':
    '''
    entry point
    '''
    lambda_handler(None, None)
