# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 23:15:55 2017

@author: Sixing
"""
from tool import readList,getCursor

def mainFunction():
    path = 'E:/Code/Data/insertSQL.csv'
    sqlList = readList(path)
    conn,cur = getCursor()#获取数据库连接和游标
    print('total is '+str(len(sqlList)))
    for i in range(len(sqlList)):
        try:
            cur.execute(sqlList[i])
            conn.commit()
            print('now is '+str(i))
        except Exception:
            print('error'+sqlList[i])
        #break
    cur.close();conn.close()

if __name__ == '__main__':
    mainFunction()