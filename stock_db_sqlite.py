#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import sys
import datetime

class StockDB:
    """
        Stockデータを管理するDB
    """
    def __init__(self, host=None,user=None,pas=None,database):
        self._host = host
        self._user = user
        self._pas = pas
        self._database = database

        self._conn = sqlite3.connect('%s.db' % database, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        
        ## performance tuneing
        isolation_level = self._conn.isolation_level
        self._conn.isolation_level = 'EXCLUSIVE'
        sqlite_pragma = {"journal_mode":"WAL", "SYNCHRONOUS":"OFF"}
        for k,v in sqlite_pragma.items():
            sql_pragma = "PRAGMA %s = %s;" % (k,v)
            cur.execute(sql_pragma)
        self._conn.commit()
        self._conn.isolation_level = 'DEFERRED'

        self._conn.text_factory = str
        self._cur = self._conn.cursor()

    def __del__(self):
        if self._conn:
            self.close()

    def close(self):
        self._cur.close()
        self._cur = None
        self._conn.close()
        self._conn = None

    def create_db(self):
        """
        テーブルを作成する
        """
        """sqlite create"""

        sql = '''CREATE TABLE IF NOT EXISTS stock_price (
                                 ccode int(11),
                                 date date,
                                 open double,
                                 high double,
                                 low double,
                                 close double,
                                 volume double,
                                 updatetime timestamp DEFAULT (DATETIME('now','localtime')),
                                 PRIMARY KEY(ccode,date)
                                 ) ;'''
        self._cur.execute(sql)


        sql = '''CREATE TABLE IF NOT EXISTS finance_data(
                                 ccode int(11),
                                 market_cap double,
                                 shares_issued double,
                                 dividend_yield double,
                                 dividend_one double,
                                 per double,
                                 pbr double,
                                 eps double,
                                 bps double,
                                 price_min double,
                                 round_lot double,
                                 updatetime timestamp DEFAULT (DATETIME('now','localtime')),
                                 PRIMARY KEY(ccode)
                                 )  ;'''
        self._cur.execute(sql)

        sql = '''CREATE TABLE IF NOT EXISTS brand_data(
                                 ccode int(11),
                                 industry_code int(11),
                                 industry_name varchar(32),
                                 market varchar(32),
                                 name varchar(128),
                                 info varchar(256),
                                 updatetime timestamp DEFAULT (DATETIME('now','localtime')),
                                 PRIMARY KEY(ccode)
                                 )  ;'''
        self._cur.execute(sql)

        sql = '''CREATE TABLE IF NOT EXISTS brand_refresh(
                                 ccode_count int(11),
                                 updatetime timestamp
                                 )  ;'''
        self._cur.execute(sql)

        sql = '''CREATE TABLE IF NOT EXISTS stock_condition(
                                 ccode int(11),
                                 completed int(11),
                                 start_date date,
                                 end_date date,
                                 updatetime timestamp DEFAULT (DATETIME('now','localtime')),
                                 PRIMARY KEY(ccode)
                                 )  ;'''
        self._cur.execute(sql)


    def create_fs_db(self):
        sql = '''CREATE TABLE IF NOT EXISTS stock_fs_data(
                                 ccode int(11),
                                 settling_day date,
                                 procedure_m varchar(32) ,
                                 published_day date,
                                 settling_months varchar(32) ,
                                 total_sales bigint  ,
                                 operating_profit bigint  ,
                                 ordinary_profit bigint ,
                                 current_net_income bigint ,
                                 eps decimal(12,2) ,
                                 income_per_stock decimal(12,2) ,
                                 allotment_per_stock decimal(12,2) ,
                                 allotment_kind varchar(32) ,
                                 bps decimal(12,2) ,
                                 all_issued_stock bigint ,
                                 total_asset bigint ,
                                 owned_capital bigint ,
                                 fund bigint ,
                                 dept_with_interest bigint ,
                                 capital_to_asset_ratio decimal(12,2) ,
                                 roa decimal(12,2) ,
                                 updatetime timestamp DEFAULT (DATETIME('now','localtime')),
                                 PRIMARY KEY(ccode,settling_day)
                                 )  ;'''
        self._cur.execute(sql)

    def GetDateBrandRefreshed(self):
        """
        Brandの更新日付を取得する。
        :return: 更新日付(date型)
        """
        try:
            sql = "SELECT ccode_count,updatetime from brand_refresh order by updatetime desc;"
            self._cur.execute(sql)

        except sqlite3.Error, e:
            print "Error Occured in Select Brand Refreshed "
            print "Error %s:" % e.args[0]


        rows = self._cur.fetchall()
        if rows is None:
            return None
        for r in rows:
            #print rows
            ret = r[1]
            #print ret
            return ret

    def GetCCode(self):
        """
        CCODEの一覧を取得する。
        :return:list
        """
        try:
            sql = "SELECT ccode from brand_data order by ccode;"
            self._cur.execute(sql)
        except sqlite3.Error, e:
            print "Error Occured in Select CCode"
            print "Error %s:" % e.args[0]
        rows = self._cur.fetchall()
        if rows is None:
            return None
        ret = []
        for r in rows:
            ret.append(r[0])
        return ret

    def GetStartDate(self,ccode):
        """
        CCODE別に取得を開始すべき日付を取得する。
        :param ccode: ccode
        :return: 日付(date型)
        """
        try:
            sql = "select ccode,completed,start_date,end_date from stock_condition where ccode = " + str(ccode)
            self._cur.execute(sql)
        except sqlite3.Error, e:
            print "Erro Occured in Select Start Date"
            print "Error %s:" % e.args[0]
        rows = self._cur.fetchall()
        if rows is None:
            return None
        for r in rows:
            if r[1] == 0:
                return r[2] #start_date
            return r[3] + datetime.timedelta(days=1) #end_date

    def UpdateStockCondition(self,ccode,completed,start_date,end_date):
        """
        株価取得のconditionをupdateする。
        :param ccode:
        :param completed:
        :param start_date:
        :param end_date:
        :return:
        """
        try:
            sql = "REPLACE into stock_condition(ccode,completed,start_date,end_date) values(?,?,?,?);"
            self._cur.execute(sql,[ccode,completed,start_date,end_date])
        except sqlite3.Error, e:
            print "Error update stock condition in ",ccode
            print "Error %s:" % e.args[0]


    def UpdateBrandRefreshed(self,cnt=0):
        """
        BrandRefreshテーブルを更新する。
        :param cnt:
        :return:
        """
        try:
            sql = "REPLACE INTO brand_refresh(ccode_count) VALUES(?);"
            self._cur.execute(sql,[cnt])
        except sqlite3.Error, e:
            print "Error Occured in update brand refreshed"
            print "Error %s:" % e.args[0]

    def InsertStockFSData(self,list_of_list=[]):
        """

        :param list:
        :return:
        """
        if len(list_of_list) == 0:
            return None
        try:
            sql = """REPLACE INTO stock_fs_data(ccode ,
                                     settling_day ,
                                     procedure_m ,
                                     published_day  ,
                                     settling_months  ,
                                     total_sales   ,
                                     operating_profit   ,
                                     ordinary_profit  ,
                                     current_net_income ,
                                     eps ,
                                     income_per_stock ,
                                     allotment_per_stock ,
                                     allotment_kind  ,
                                     bps  ,
                                     all_issued_stock  ,
                                     total_asset  ,
                                     owned_capital  ,
                                     fund  ,
                                     dept_with_interest ,
                                     capital_to_asset_ratio  ,
                                     roa ) VALUES"""
            ss = "(? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? )"
            params_list = []
            for x in list_of_list:
                params_list.append(x)
            sql += ss
            self._cur.executemany(sql,params_list)

        except sqlite3.Error, e:
            print "Error Occurred in insert "
            print "Error %s:" % e.args[0]


    def InsertStockData(self,list_of_dict=[]):
        """
        StockPriceテーブルに各株価をinsertする。
        :param list_of_dict: 株価情報のdictが入ったlist
        :return:
        """
        if len(list_of_dict) == 0:
            return None
        try:
            sql = "REPLACE INTO stock_price" + "(ccode, date, open, high,low,close,volume) VALUES"
            ss = "( ?, ?, ?, ?, ?, ?, ?)"
            params_list = []
            for row in list_of_dict:
                tmp = [row["ccode"],row["date"],row["open"],row["high"],row["low"],row["close"],row["volume"]]
                params_list.append(tmp)
            sql += ss
            self._cur.executemany(sql,params_list)

        except sqlite3.Error, e:
            print "Error Occurred in insert "
            print "Error %s:" % e.args[0]

    def InsertBrandData(self,list_of_dict=[]):
        """
        Brandデータをinsertする。
        :param list_of_dict:
        :return:
        """
        if len(list_of_dict) == 0:
            return None
        try:
            sql = "REPLACE INTO brand_data" + "(ccode, industry_code, industry_name, market,name,info) VALUES"
            ss  = "( ?, ?, ?, ?, ?, ?)"
            params_list = []
            for row in list_of_dict:
#                ss += "( %s, %s, %s, %s, %s, %s),"
                tmp = [row["ccode"],row["industry_code"],row["industry_name"],row["market"],row["name"],row["info"]]
                params_list.append(tmp)
            sql += ss
            print len(params_list)
            sys.stdout.flush()
            self._cur.executemany(sql,params_list)

        except sqlite3.Error, e:
            print "Error Occurred in insert "
            print "Error %s:" % e.args[0]
            #debug
            print e
            print "sql:", sql
            print "len(params_list):", len(params_list)

    def InsertFinancialData(self,list_of_dict=[]):
        """
        financial dataをinsertする。
        :param list_of_dict:
        :return:
        """
        try:
            sql = "REPLACE INTO finance_data (ccode, market_cap, shares_issued, dividend_yield,dividend_one,per,pbr,eps,bps,price_min,round_lot) VALUES"
            ss = "( ?, ?, ?, ?,?,?,?,?,?,?,?)"
            params_list = []
            for row in list_of_dict:
                tmp = [row["ccode"],row["market_cap"],row["shares_issued"],row["dividend_yield"],row["dividend_one"],row["per"],row["pbr"],row["eps"],row["bps"],row["price_min"],row["round_lot"]]
                params_list.append(tmp)
            sql += ss
            self._cur.executemany(sql,params_list)

        except sqlite3.Error, e:
            print "Error Occurred in insert "
            print "Error %s:" % e.args[0]


    def Commit(self):
        self._conn.commit()

    def Rollback(self):
        self._conn.rollback()
