# -*- coding: utf-8 -*-
import csv
import os


class PathSentences(object):
    def __init__(self, base_path, prefix=None, postfix=None, 
            delimiter=',', quotechar='"', encoding='utf-8', out_dict=False):
        self.base_path = base_path
        self.prefix = prefix
        self.postfix = postfix
        # format
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        # output
        self.csv_reader = csv.DictReader if out_dict else csv.reader

    def __iter__(self):
        for special_file in os.listdir(self.base_path):
            fname = os.path.join(self.base_path, special_file)
            if not os.path.isfile(fname):
                continue

            if (self.postfix and special_file.endswith(self.postfix)) or \
               (self.prefix and special_file.startswith(self.prefix)) or \
               (not self.prefix and not self.postfix):
                with open(fname, encoding=self.encoding) as fp: 
                    reader = self.csv_reader(fp, delimiter=self.delimiter, quotechar=self.quotechar)
                    for row in reader:
                        yield row 


class FileSentences(object):
    def __init__(self, filename, delimiter=',', quotechar='"', encoding='utf-8', out_dict=False):
        self.filename = filename
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        # output
        self.csv_reader = csv.DictReader if out_dict else csv.reader

    def __iter__(self):
        with open(self.filename, encoding=self.encoding) as fp: 
            reader = self.csv_reader(fp, delimiter=self.delimiter, quotechar=self.quotechar)
            for row in reader:
                yield row 


if __name__ == '__main__':
    fn = '001.csv'
    datas = FileSentences(fn, out_dict=True) 
    for data in datas:
        print(data, data['A'], data['B'])
    print('='*10)

    fn = '002.csv'
    pdatas = FileSentences(fn)
    for data in pdatas:
        print(data)


