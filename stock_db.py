#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector as mysql

class StockDB:
    """
        Stockデータを管理するDB
    """
    def __init__(self, host,user,pas,database):
        self._host = host
        self._user = user
        self._pas = pas
        self._database = database

        self._conn = mysql.connect(host=host,user=user,password=pas,database=database)
        self._conn.text_factory = str
        self._cur = self._conn.cursor(buffered=True)

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
        """mysql create"""

        sql = '''CREATE TABLE IF NOT EXISTS stock_price (
                                 ccode int(11),
                                 date datetime,
                                 open double,
                                 high double,
                                 low double,
                                 close double,
                                 volume double,
                                 updatetime timestamp,
                                 PRIMARY KEY(ccode,date)
                                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8  ;'''
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
                                 updatetime timestamp,
                                 PRIMARY KEY(ccode)
                                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8  ;'''
        self._cur.execute(sql)

        sql = '''CREATE TABLE IF NOT EXISTS brand_data(
                                 ccode int(11),
                                 industry_code int(11),
                                 industry_name varchar(32),
                                 market varchar(32),
                                 name varchar(128),
                                 info varchar(256),
                                 updatetime timestamp,
                                 PRIMARY KEY(ccode)
                                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8  ;'''
        self._cur.execute(sql)

        sql = '''CREATE TABLE IF NOT EXISTS brand_refresh(
                                 ccode_count int(11),
                                 updatetime timestamp
                                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8  ;'''
        self._cur.execute(sql)

        sql = '''CREATE TABLE IF NOT EXISTS stock_condition(
                                 ccode int(11),
                                 completed int(11),
                                 start_date datetime,
                                 end_date datetime,
                                 updatetime timestamp,
                                 PRIMARY KEY(ccode)
                                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8  ;'''
        self._cur.execute(sql)

    def GetDateBrandRefreshed(self):
        """
        Brandの更新日付を取得する。
        :return: 更新日付(date型)
        """
        try:
            sql = "SELECT ccode_count,DATE(updatetime) from brand_refresh order by updatetime desc;"
            self._cur.execute(sql)

        except mysql.Error:
            print "Error Occured in Select Brand Refreshed "

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
        except mysql.Error:
            print "Error Occured in Select CCode"
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
            sql = "select ccode,completed,DATE(start_date),DATE_ADD(DATE(end_date),INTERVAL 1 DAY) from stock_condition where ccode = " + str(ccode)
            self._cur.execute(sql)
        except mysql.Error:
            print "Erro Occured in Select Start Date"
        rows = self._cur.fetchall()
        if rows is None:
            return None
        for r in rows:
            if r[1] == 0:
                return r[2] #start date
            return r[3]

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
            sql = "REPLACE into stock_condition(ccode,completed,start_date,end_date) values(%s,%s,%s,%s);"
            self._cur.execute(sql,[ccode,completed,start_date,end_date])
        except mysql.Error:
            print "Error update stock condition in ",ccode


    def UpdateBrandRefreshed(self,cnt=0):
        """
        BrandRefreshテーブルを更新する。
        :param cnt:
        :return:
        """
        try:
            sql = "REPLACE INTO brand_refresh(ccode_count) VALUES(%s);"
            self._cur.execute(sql,[cnt])
        except mysql.Error:
            print "Error Occured in update brand refreshed"

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
            ss = ""
            params_list = []
            for row in list_of_dict:
                ss += "( %s, %s, %s, %s, %s, %s, %s),"
                tmp = [row["ccode"],row["date"],row["open"],row["high"],row["low"],row["close"],row["volume"]]
                [params_list.append(x) for x in tmp]
            sql += ss[0:-1]
            self._cur.execute(sql,params_list)

        except mysql.Error:
            print "Error Occurred in insert "

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
            ss = ""
            params_list = []
            for row in list_of_dict:
                ss += "( %s, %s, %s, %s, %s, %s),"
                tmp = [row["ccode"],row["industry_code"],row["industry_name"],row["market"],row["name"],row["info"]]
                [params_list.append(x) for x in tmp]
            sql += ss[0:-1]
            self._cur.execute(sql,params_list)

        except mysql.Error:
            print "Error Occurred in insert "

    def InsertFinancialData(self,list_of_dict=[]):
        """
        financial dataをinsertする。
        :param list_of_dict:
        :return:
        """
        try:
            sql = "REPLACE INTO finance_data (ccode, market_cap, shares_issued, dividend_yield,dividend_one,per,pbr,eps,bps,price_min,round_lot) VALUES"
            ss = ""
            params_list = []
            for row in list_of_dict:
                ss += "( %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s),"
                tmp = [row["ccode"],row["market_cap"],row["shares_issued"],row["dividend_yield"],row["dividend_one"],row["per"],row["pbr"],row["eps"],row["bps"],row["price_min"],row["round_lot"]]
                [params_list.append(x) for x in tmp]
            sql += ss[0:-1]
            self._cur.execute(sql,params_list)

        except mysql.Error:
            print "Error Occurred in insert "


    def Commit(self):
        self._conn.commit()

    def Rollback(self):
        self._conn.rollback()
