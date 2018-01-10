'''
用于获得新的合作专家
'''
from tool import signal,getHttpUa,httpProxies,headers,readTXT,getCursor,getResult,ChangeOrNot,editeProxies,editeHeader,getPage

import time
import random
from bs4 import BeautifulSoup

sltCollNotNull = 'select id,colleage from dlurl1 where status=1'
#sltNameUrl = 'select name,url from dlurl'

#主要的循环方法
def findPage():
    http,ua = getHttpUa()
    conn,cur = getCursor()
    dlList = getResult(sltCollNotNull,cur)
    for dl in dlList:
        # this is test!!!! read from a txt
        #html = readTXT('E:/Code/Test Data/Paul Robert Barford - ACM author profile page - colleagues.txt')
        #html = readTXT('E:/Code/Test Data/Yu Zheng - ACM author profile page.txt')
        #html = readTXT('E:/Code/Test Data/A. Smolic - ACM author profile page.txt')
        if ChangeOrNot() == True:
            editeProxies(http)
            editeHeader(ua)
        time.sleep(random.randint(1, 12))
        
        html = str(getPage(dl['colleage']))#取出url
        if html != ' ':
            nameLink = analysisPage(html)
            for nl in nameLink:
            	addInfo(conn,cur,nl)
            	#print(nl)
            print('Now is '+str(dl['id']))
            
        #break#only run one time
    
    cur.close()
    conn.close()

def analysisPage(doc):
    nameLink = []
    #total = 0
    soup = BeautifulSoup(''.join(doc),"lxml")
    a = soup.find('a',{'name':"collab"})
    divAb = a.parent.parent
    tr = divAb.table.tr
    for td in tr.findAll('td'):
        for div in td.findAll('div'):
            if div.a.string != None:
                name = cleanName(div.a.string)
                url = 'http://dl.acm.org/' + div.a['href']
                url,userID = extractUserID(url)
                
                if checkSame(url,userID)==False:#不存在相同
                    nameLink.append([name,url,userID])
    return nameLink

def checkSame(url,userID):
    conn,cur = getCursor()
    checkSQL = 'select id,url from dlurl1 where userid='+userID
    number = cur.execute(checkSQL)
    if number>0:#如果结果集记录大于0
        for c in cur.fetchall():#提取每个记录
            if c['url']==url:#对url是否相同
                return True
    return False

def cleanName(name):
    newName = ''
    for n in name.split(' '):
        if len(n)==0:
            continue
        else:
            newName = newName +n.strip()+' '
    return newName.strip()
    
def addInfo(conn,cur,nl):
    checkSQL = 'select * from dlurl where userid='+nl[2]
    number = cur.execute(checkSQL,nl[1])
    if number==0:
        insertSQL = 'insert into dlurl1 (name,url,status,tem,userID) values (%s,%s,0,1,%d)'
        insertValues = (nl[0],nl[1],nl[2])
        try:
            cur.execute(insertSQL,insertValues)
            conn.commit()
        except Exception:
            print('insert error '+str(insertValues))

def extractUserID(url):
    url = url.split('&')[0]
    userid = url[:]
    userid = userid.replace('http://dl.acm.org/author_page.cfm?id=','')
    userid = userid[4:]#only numbers begin at 4 are considered     
    return url,int(userid)

if __name__ == '__main__':	
    findPage()