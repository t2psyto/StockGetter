#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import ConfigParser
from stock_db import StockDB
from stock_fs import StockFS

def main():
    conf = ConfigParser.SafeConfigParser()
    conf.read("stock.ini")
    is_test = int(conf.get("CONFIG","is_test"))

    db = StockDB(host=conf.get("DB",
                                  "host"),
                    user=conf.get("DB",
                                  "user"),
                    pas=conf.get("DB",
                                 "pass"),
                    database=conf.get("DB",
                                      "database"))

    """DBテーブルの生成"""
    db.create_fs_db()
    db.Commit()

    """CCODEの取得"""
    ccodes = db.GetCCode()

    """StockFS"""
    f = StockFS()
    for ccode in ccodes:
        print ccode
        f1 = f.get(ccode)
        list_of_list = [data.getList() for data in f1]
        db.InsertStockFSData(list_of_list)
    db.Commit()
    db.close()

    return 0

if __name__ == "__main__":

    sys.exit(main())
