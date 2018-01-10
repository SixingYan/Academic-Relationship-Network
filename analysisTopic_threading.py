# -*- coding: utf-8 -*-
'''
	直接抽取主题词
'''
from queue import Queue
from threading import Thread
import time
import random
import csv
from bs4 import BeautifulSoup
from tool import getHttpUa,getCursor,getPage,ChangeOrNot
from tool import editeHeader,editeCookies,writeMetrx,commHttpProxies,commHeaders,commCookies
#import os;os.chdir('e:/Code/Python');import analysisTopic_threading;analysisTopic_threading.mainFunction()

saveFile_path = 'e:/Code/Data/subject new 2/topics_'
expertList_path = 'e:/Code/Data/subject 8000.csv'
conn,cur = getCursor()

ipQueue = Queue()	
uaQueue = Queue()
dlQueue = Queue()
htmlQueue = Queue()

global httpProxies
global headers
global cookies

class pageWorker(Thread):
    def __init__(self,ipQueue,uaQueue,dlQueue,htmlQueue):
        Thread.__init__(self)
        self.ipQueue = ipQueue
        self.uaQueue = uaQueue
        self.dlQueue = dlQueue
        self.htmlQueue = htmlQueue
        
    def run(self):
        while True:         
           	# 从队列中获取任务并扩展tuple
            httpProxies = commHttpProxies.copy()
            headers = commHeaders.copy()
            cookies = commCookies.copy()

            dl  = self.dlQueue.get()
            http = self.ipQueue.get()
            ua = self.uaQueue.get()
            httpProxies['https'] = http
            #修饰参数
            if ChangeOrNot() == True:#随机触发
                headers=editeHeader(ua,headers,dl['name'])#改变user agent
                cookies=editeCookies(cookies)
            time.sleep(random.randint(5, 20))#随机休眠

            #取出html
            html = str(getPage(dl['url'],httpProxies,headers,cookies))#取出url
            #放回
            self.ipQueue.put(http)
            self.uaQueue.put(ua)
            if html == ' ':#未获取成功，重新放入
                print('put back:'+str(dl['id']))
                self.dlQueue.put(dl)
                #放入
            else:
                print('get successfully:'+str(dl['id']))
                self.htmlQueue.put((html,dl))
            self.dlQueue.task_done()

class analysisWorker(Thread):
    def __init__(self,htmlQueue):
        Thread.__init__(self)
        self.htmlQueue = htmlQueue

    def run(self):
        while True:
            html,dl = self.htmlQueue.get()
            topics = extractTopic(html)
            #processing and put into mysql
            if len(topics)>0:
                saveInstitution(dl['id'],topics)
            else:     
                #如果没有，就占位就行
                saveNull(dl['id'])
            self.htmlQueue.task_done()

def saveNull(eid):
    fileP = saveFile_path + str(eid)+'.csv'
    try:
        with open(fileP, 'a+',newline='',encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile,quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow([''])
            print('Competed null'+str(eid))
    except Exception:
        print('write error!!!!?')
    
def saveInstitution(eid,institution):
    #放入一个一维数组
    fileP = saveFile_path + str(eid)+'.csv'
    try:
        writeMetrx(fileP,institution)
        print('Competed '+str(eid))
    except Exception:
    	print('write error!')

def mainFunction():
    http,uag = getHttpUa()

    for ip in http:
        ipQueue.put(ip)
    for ua in uag:
        uaQueue.put(ua)
    for k in range(1):
        aWorker = analysisWorker(htmlQueue)
        aWorker.daemon = True
        aWorker.start()
    print('ok1')
    for i in range(7):
        pWorker = pageWorker(ipQueue,uaQueue,dlQueue,htmlQueue)
        pWorker.daemon = True
        pWorker.start()
    print('ok2')
    
    #dlList = getResult(sltDLNotCom,cur)#返回url实体的二维数组
    dlList=[]
    with open(expertList_path,newline = '',encoding= 'utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row[2])>5:
                dlList.append({'id':row[0],'name':row[1],'url':row[2]})
                #break
        print('total:'+str(len(dlList)))

    for dl in dlList:
        dlQueue.put(dl)


def extractTopic(html):
    topics = []
    soup = BeautifulSoup(''.join(html),"lxml")
    table = soup.find('table',{'align':"left",'border':"0",'cellpadding':"0",'cellspacing':"0"})
    if len(str(table))< 10:
        return topics
    tr = table.findAll('tr')
    if len(str(tr))< 10:
        return topics
    else:
        if len(tr)<1:
            return topics
    for a in tr[1].findAll('a'):
        try:
            title = a['title']
            parts = title.split('(')
            num = int(parts[-1].replace(')',''))
            topics.append([parts[0].strip(),num])
        except Exception:
            print('a topic analysis failed: '+parts[0].strip())
    #print(topics)
    return topics

if __name__ == '__main__':
    mainFunction()
