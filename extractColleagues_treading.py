# -*- coding: utf-8 -*-
'''
用于获得新的合作专家
'''
from queue import Queue
from threading import Thread
from tool import getHttpUa,getCursor,getResult,ChangeOrNot,editeHeader,getPage,cleanURL
from tool import commHttpProxies,commHeaders,commCookies,editeCookies,writeTXT
import time
import random
from bs4 import BeautifulSoup
#import datetime

ipQueue = Queue()	
uaQueue = Queue()
dlQueue = Queue()
htmlQueue = Queue()
infoQueue = Queue()

global httpProxies
global headers
global cookies
global num

sltDLNotCom = 'select id,colleage,name from dlurl1 where tem=1 limit 2'
Dir_path = 'E:/Code/Data/Error/'
httpError_path = 'httpError_'

class pageWorker(Thread):
    def __init__(self,ipQueue,uaQueue,dlQueue,htmlQueue):
        Thread.__init__(self)
        self.ipQueue = ipQueue
        self.uaQueue = uaQueue
        self.dlQueue = dlQueue
        self.htmlQueue = htmlQueue
        
    def run(self):
        while True:
            httpProxies = commHttpProxies.copy()
            headers = commHeaders.copy()
            cookies = commCookies.copy()

            dl  = self.dlQueue.get()
            http = self.ipQueue.get()
            ua = self.uaQueue.get()
            httpProxies['https'] = http
            #ts1 = datetime.datetime.now()
            #修饰参数
            if ChangeOrNot() == True:#随机触发
                headers=editeHeader(ua,headers,dl['name'])#改变user agent
                cookies=editeCookies(cookies)
            time.sleep(random.randint(3, 14))#随机休眠

            #Optional            
            dl['colleage'] = cleanURL(dl['colleage'])#delete cookies
            #if '&CFID=716005087&CFTOKEN=29677367' in dl['colleage']:
            #    dl['colleage'] = dl['colleage'].replace('&CFID=716005087&CFTOKEN=29677367','')

            #取出html
            html = str(getPage(dl['colleage'],httpProxies,headers,cookies))#取出url
            #放回
            self.ipQueue.put(http)
            self.uaQueue.put(ua)
            if html == ' ':#未获取成功，重新放入
                self.dlQueue.put(dl)
            #放入
            print('get HTML:'+str(dl['id']))
            self.htmlQueue.put((html,dl))
            #print('get: '+str(dl['id']))
            #ts2 = datetime.datetime.now()
            #print('page id:'+str(dl['id'])+' time:'+str(ts2-ts1))
            self.dlQueue.task_done()

class analysisWorker(Thread):
    def __init__(self,htmlQueue,infoQueue):
        Thread.__init__(self)
        self.htmlQueue = htmlQueue
        self.infoQueue = infoQueue

    def run(self):
        while True:
           html,dl = self.htmlQueue.get()
           #ts1 = datetime.datetime.now()
           nameLink = analysisPage(html,dl['id'])
           #放入
           if len(nameLink)>0:
               print('analysis:'+str(dl['id']))
               for nl in nameLink:
                   self.infoQueue.put((nl,dl['id']))           
           else:
               self.htmlQueue.put((html,dl))
           #print('analyze: '+str(dl['id']))
           #ts2 = datetime.datetime.now()
           #print('analysis id:'+str(dl['id'])+' time:'+str(ts2-ts1))
           self.htmlQueue.task_done()

class mysqlWorker(Thread):
    def __init__(self,infoQueue):
        Thread.__init__(self)
        self.infoQueue = infoQueue

    def run(self):
        while True:
            nl,id = infoQueue.get()
            #ts1 = datetime.datetime.now()
            #处理
            
            flag = addInfo(nl,id)#成功为True
            
            #print('infoset:' +str(infoSet['name']))
            #print('mysql: '+str(dl['id']))
            #ts2 = datetime.datetime.now()
            #print('mysql id:'+str(dl['id'])+' time:'+str(ts2-ts1))
            if not flag:
                self.infoQueue.put((nl,id))#放回
            else:
                print('mysql:'+str(id))
            conn,cur = getCursor()
            cur.execute('update dlurl1 set tem=0 where id='+str(id))
            conn.commit()
            cur.close();conn.close();
            self.infoQueue.task_done()

def analysisPage(doc,id):
    nameLink = []
    #total = 0
    soup = BeautifulSoup(''.join(doc),"lxml")
    a = soup.find('a',{'name':"collab"})
    try:
        divAb = a.parent.parent
        tr = divAb.table.tr
        for td in tr.findAll('td'):
            for div in td.findAll('div'):
                if div.a.string != None:
                    name = cleanName(div.a.string)
                    #print('OK here')
                    url = 'http://dl.acm.org/' + div.a['href']
                    url,userID = extractUserID(url)
                    #print(url)
                    #print(str(userID))
                    if checkSame(url,userID)==False:#不存在相同
                        nameLink.append([name,url,userID])                        
    except Exception:
        print('error:'+str(a))
        path = Dir_path+httpError_path+str(id)+'.txt'
        writeTXT(path,doc)
    return nameLink

def checkSame(url,userID):
    #
    conn,cur = getCursor()
    try:
        checkSQL = 'select id,url from dlurl1 where userid='+str(userID)
        number = cur.execute(checkSQL)
        if number>0:#如果结果集记录大于0
            for c in cur.fetchall():#提取每个记录
                if c['url']==url:#对url是否相同
                    cur.close()
                    conn.close()
                    return True
        cur.close()
        conn.close()
        return False
    except Exception:
        print('check error!:'+url)
        cur.close()
        conn.close()
        return True
    
def cleanName(name):
    newName = ''
    for n in name.split(' '):
        if len(n)==0:
            continue
        else:
            newName = newName +n.strip()+' '
    return newName.strip()
'''
def addInfo(nl,id):
    conn,cur = getCursor()
    insertSQL='insert into dlurl1 (name,url,status,tem,userID) values("'+nl[0]+'","'+nl[1]+'",0,1,'+str(nl[2])+');'
    #insertValues = (nl[0],nl[1],nl[2])
    try:
        cur.execute(insertSQL)
        conn.commit()
        print('Complete:'+nl[0]+'  id:'+str(id))
        cur.close()
        conn.close()
        return True
    except Exception:
        print('insert error:')
        print(nl)
        cur.close()
        conn.close()
        return False
'''
def addInfo(nl,id):
    conn,cur = getCursor()
    insertSQL='insert into dlurl1 (name,url,status,tem,userID) values("'+nl[0]+'","'+nl[1]+'",0,1,'+str(nl[2])+');'
    #insertValues = (nl[0],nl[1],nl[2])
    try:
        with open(''):
            
        cur.execute(insertSQL)
        conn.commit()
        print('Complete:'+nl[0]+'  id:'+str(id))
        cur.close()
        conn.close()
        return True
    except Exception:
        print('insert error:')
        print(nl)
        cur.close()
        conn.close()
        return False

def extractUserID(url):
    url = url.split('&')[0]
    userid = url[:]
    userid = userid.replace('http://dl.acm.org/author_page.cfm?id=','')
    userid = userid[4:]#only numbers begin at 4 are considered     
    return url,int(userid)

if __name__ == '__main__':
    #
    http,uag = getHttpUa()
    num = 0
    for ip in http:
        ipQueue.put(ip)
        
    for ua in uag:
        uaQueue.put(ip)
    print('ok1')
    for k in range(1):
        aWorker = analysisWorker(htmlQueue,infoQueue)
        aWorker.daemon = True
        aWorker.start()
    print('ok2')
    for i in range(3):
        pWorker = pageWorker(ipQueue,uaQueue,dlQueue,htmlQueue)
        pWorker.daemon = True
        pWorker.start()
    print('ok3')
    conn,cur = getCursor()
    dlList = getResult(sltDLNotCom,cur)#返回url实体的二维数组
    for dl in dlList:
        dlQueue.put(dl)
    cur.close()
    conn.close()
    print('ok4')
    for j in range(1):
        mWorker = mysqlWorker(infoQueue)
        mWorker.daemon = True
        mWorker.start()