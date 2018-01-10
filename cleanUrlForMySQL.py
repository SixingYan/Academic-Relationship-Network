# -*- coding: utf-8 -*-
from tool import getCursor,getResult

sltCollNotNull = 'select * from dlurl1 where status=0 and tem=0'

def cleanUrlMySQL():
    conn,cur = getCursor()#获取数据库连接和游标
    dlList = getResult(sltCollNotNull,cur)
    for dl in dlList:
        url,userid = analysisRecord(dl['url'])
        result = addInfo(url,userid,dl['id'],cur,conn)
        if result==1:
            cur.execute('update dlurl1 set tem=1 where id='+str(dl['id']))#标记
            conn.commit()
            print('Now is '+str(dl['id']))  
        
def analysisRecord(url):
    url = url.split('&')[0]
    userid = url[:]
    userid = userid.replace('http://dl.acm.org/author_page.cfm?id=','')
    userid = userid[4:]#only numbers begin at 4 are considered     
    return url,int(userid)
    
def addInfo(url,userid,id,cur,conn):
    updateSQL = 'update dlurl1 set url="'+url+'", userid='+str(userid)+' where id='+str(id)
    try:
        cur.execute(updateSQL)
        conn.commit()
        #print(updateSQL)
        return 1
    except Exception:
        print('update error '+'id: '+str(id))
        print(updateSQL)
        return 0
        
if __name__ == '__main__':	
    cleanUrlMySQL()