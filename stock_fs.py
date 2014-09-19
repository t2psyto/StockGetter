#!/usr/bin/python
# -*- coding: utf-8 -*-

try:
    # For Python3
    from urllib.request import urlopen
except ImportError:
    # For Python2
    from urllib2 import urlopen
from jsm.util import html_parser
from lxml import etree
import pandas as pd
import re

class StockFSParser():
    """
        取得したHTMLをlistオブジェクトに格納する。
    """

    SITE_URL = "http://profile.yahoo.co.jp/independent/%(ccode)s"

    def __init__(self):
        self._elm = None
        self._ccode = None

    def fetch(self,ccode):

        url = self.SITE_URL % {"ccode":ccode}

        fp = urlopen(url)
        html = fp.read()
        fp.close()
        html = html.decode("utf-8", "ignore")
        soup = html_parser(html)
        elm = soup.findAll("table",{
            "class":"yjMt"
        })#文字列のリストになるので注意が必要。0番目だけが必要なもの
        self._elm = str(elm[0])
        self._ccode = ccode

    def get(self):
        if self._elm:
            table = etree.XML(self._elm)
            rows = iter(table)
            headers = [col.text for col in next(rows)]
            retlist = []
            append = retlist.append
            for row in rows:
                values = [col.text for col in row]
                append(dict(zip(headers, values)))

            df = pd.DataFrame(retlist).set_index([u"　"])
            df = df.apply(self.renewList,axis=1)
            df = df.T

            retlist = []
            append = retlist.append
            for index , row in df.iterrows():
                tlist = list(row)
                append(StockFSData(ccode=str(self._ccode),settling_day=tlist[0],procedure=tlist[1],published_day=tlist[2],
                               settling_months=tlist[3],total_sales=tlist[4],operating_profit=tlist[5],ordinary_profit=tlist[6],current_net_income=tlist[7],
                               eps=tlist[8],income_per_stock=tlist[9],allotment_per_stock=tlist[10],allotment_kind=tlist[11],bps=tlist[12],
                               all_issued_stock=tlist[13],total_asset=tlist[14],owned_capital=tlist[15],fund=tlist[16],dept_with_interest=tlist[17],
                               capital_to_asset_ratio=tlist[18],roa=tlist[19]))
            return retlist

        else:
            return None

    def renewList(self,list=[]):
        list = [self.parseToDate(strg) for strg in list]
        list = [self.deleteComma(strg) for strg in list]
        list = [self.parseToNumber(strg) for strg in list]
        list = [self.deleteYen(strg) for strg in list]

        return list

    def parseToDate(self,strg=""):
        strg = strg.replace(u"年","/")
        strg = strg.replace(u"月期","/1")
        strg = strg.replace(u"か月","")
        strg = strg.replace(u"月","/")
        strg = strg.replace(u"日本方式","Japanese")
        strg = strg.replace(u"日","")

        return strg

    def parseToNumber(self,strg=""):
        strg = strg.replace(u"百万円","000000")
        strg = strg.replace(u"千株","000")
        strg = strg.replace(u"株","")
        strg = strg.replace(u"%","")
        return strg

    def deleteComma(self,strg=""):
        return strg.replace(",","")

    def deleteYen(self,strg=""):
        strg = strg.replace(u"-","")
        strg = strg.replace(u"‐","")
        strg = strg.replace(u"円","")
        return strg


class StockFSData():
    """
        財務情報を格納するためのデータクラス
    """

    def __init__(self,ccode,settling_day,procedure,published_day,settling_months,total_sales,operating_profit,ordinary_profit,
                 current_net_income,eps,income_per_stock,allotment_per_stock,
                 allotment_kind,bps,all_issued_stock,total_asset,owned_capital,
                 fund,dept_with_interest,capital_to_asset_ratio,roa):
        self._ccode = self._int(ccode)
        self._settling_day = settling_day
        self._procedure = procedure
        self._published_day = published_day
        self._settling_months = self._int(settling_months)
        self._total_sales = self._int(total_sales)
        self._operating_profit = self._int(operating_profit)
        self._ordinary_profit = self._int(ordinary_profit)
        self._current_net_income = self._int(current_net_income)
        self._eps = self._float(eps)
        self._income_per_stock = self._float(income_per_stock)
        self._allotment_per_stock = self._float(allotment_per_stock)
        self._allotment_kind = allotment_kind
        self._bps = self._float(bps)
        self._all_issued_stock = self._int(all_issued_stock)
        self._total_asset = self._int(total_asset)
        self._owned_capital = self._int(owned_capital)
        self._fund = self._int(fund)
        self._dept_with_interest = self._int(dept_with_interest)
        self._capital_to_asset_ratio = self._float(capital_to_asset_ratio)
        self._roa = self._float(roa)

    def _parse(self, val, default=0):
        m = re.search('(-|)[0-9,\.]+', val)
        if m:
            return m.group(0).replace(',', '')
        return default

    def _int(self, val, default=0):
        return int(self._parse(val, default))

    def _float(self, val, default=0.0):
        return float(self._parse(val, default))

    def getDict(self):
        return {"ccode":self._ccode,
                "settling_day":self._settling_day,
                "procedure":self._procedure,
                "published_day":self._published_day,
                "settling_months":self._settling_months,
                "total_sales":self._total_sales,
                "operating_profit":self._operating_profit,
                "ordinary_profit":self._ordinary_profit,
                "current_net_income":self._current_net_income,
                "eps":self._eps,
                "income_per_stock":self._income_per_stock,
                "allotment_per_stock":self._allotment_per_stock,
                "allotment_kind":self._allotment_kind,
                "bps":self._bps,
                "all_issued_stock":self._all_issued_stock,
                "total_asset":self._total_asset,
                "owned_capital":self._owned_capital,
                "fund":self._fund,
                "dept_with_interest":self._dept_with_interest,
                "capital_to_asset_ratio":self._capital_to_asset_ratio,
                "roa":self._roa}

    def getList(self):
        return [self._ccode,self._settling_day,self._procedure,self._published_day,self._settling_months,self._total_sales,
                self._operating_profit,self._ordinary_profit,self._current_net_income,self._eps,
                self._income_per_stock,self._allotment_per_stock,self._allotment_kind,self._bps,self._all_issued_stock,
                self._total_asset,self._owned_capital,self._fund,self._dept_with_interest,self._capital_to_asset_ratio,self._roa]

class StockFS():
    """
    財務データを取得する
    """
    def get(self,ccode):
        sp = StockFSParser()
        sp.fetch(ccode)
        return sp.get()

if __name__ == "__main__":
    f = StockFS()
    f1 = f.get(1301)
    for data in f1:
        print data.getList()

