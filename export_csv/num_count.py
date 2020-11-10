#! usr/bin/python
# -*- coding: utf-8 -*-

import os


def count_file(dir_path):
    for file in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, file)) and file[-4:] == '.csv':
            # print(file)
            table_name = os.path.basename(file)[:-4]
            with open(os.path.join(dir_path, file), encoding='utf8') as f:
                string = f.read()
                count = string.count('#@DatayesRow@#')
                print(table_name + ' : {}'.format(count - 1))
        elif os.path.isfile(os.path.join(dir_path, file)) and file[-4:] != '.csv':
            print('invalid file!')
        else:
            count_file(os.path.join(dir_path, file))


if __name__ == '__main__':
    dir_path = r'\\10.20.202.51\Datacopy\to_jiaojiao\ZYLC'
    count_file(dir_path)
