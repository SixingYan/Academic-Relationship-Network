# -*- coding: utf-8 -*-
'''
	查找另一种主题词 
	抽取另一种主题词 suburl supply  这个还要分两种情况，这里写的是直接抽取，还有间接从xml中抽取
	标记为空的主题词 suburl null  需要事先添加一行id和status，其中后一个没啥用
    注意，抽取出来的topic是可能会有空的 ！！！！！！！！！！
'''
import csv
from tool import getHttpUa,getPage,ChangeOrNot
from tool import editeHeader,editeCookies,writeList,commHttpProxies,commHeaders,commCookies
from bs4 import BeautifulSoup
from queue import Queue
from threading import Thread
import time
import random
#import os;os.chdir('e:/Code/Python');import analysisTopic_csv;analysisTopic_csv.mainFunction()
#conn,cur = getCursor()
#files_path = 'E:/Code/Data/subUrlNull sub null 2057684 to 2578611.csv' #这个是给empty用的
dict_path = 'E:/Code/Data/su1/topicsupply_' #存放文件夹
expertList_path = 'E:/Code/Data/subUrlSupply last 3.csv' #源列表
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
                print('putback '+str(dl['id']))
                self.dlQueue.put(dl)
            else:
                self.htmlQueue.put((html,dl))
            self.dlQueue.task_done()

class analysisWorker(Thread):
    def __init__(self,htmlQueue):
        Thread.__init__(self)
        self.htmlQueue = htmlQueue

    def run(self):
        while True:
           html,dl = self.htmlQueue.get()
           findOtherTopic(html,dl['id'])
           self.htmlQueue.task_done()
           
def saveNull(path,eid):
    try:
        with open(path, 'a+',newline='',encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile,quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow([''])
            print('Competed null'+str(eid))
    except Exception:
        print('write error!!!!?')
        
def findOtherTopic(html,id):
    #记下那个tag就行，放到txt里
    path = dict_path + str(id) + '.csv'
    if not 'thekeywords' in html:
        saveNull(path,id)
        return None
    
    soup = BeautifulSoup(''.join(html),"lxml")
    div = soup.find('div',{'id':"thekeywords"})
    topic = []
    try:
        if len(str(div))<10 or type(div) == 'NoneType' or div == '' or div == None:
            print('analysis error none!'+str(id))
            saveNull(path,id)
            return None

        for a in div.findAll('a'):
            topicContent = a.string
            if len(str(topicContent))<4 or type(topicContent)=='NoneType':
                print('analysis none'+str(id))
                saveNull(path,id)
                return None

            if ',' in topicContent:
                topicList = topicContent.split(',')
                for tpc in topicList:
                    topic.append(tpc.strip())
            else:
                topic.append(a.string)

        try:
            writeList(path,topic)
            print('complete: '+str(id))
        except Exception:
            print('error!'+str(id))
            
    except Exception:
        print('analysis error!'+str(id))
        saveNull(path,id)

def mainFunction():
    http,uag = getHttpUa()
    for ip in http:
        ipQueue.put(ip)
    for ua in uag:
        uaQueue.put(ua)
    for k in range(2):
        aWorker = analysisWorker(htmlQueue)
        aWorker.daemon = True
        aWorker.start()
    print('ok1')
    for i in range(9):
        pWorker = pageWorker(ipQueue,uaQueue,dlQueue,htmlQueue)
        pWorker.daemon = True
        pWorker.start()
    print('ok2')
    dlList=[]
    #open(expertList_path,newline = '',encoding= 'utf-8') codecs.open(expertList_path,'rb',encoding='utf-8')
    with open(expertList_path,newline = '',encoding= 'utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row[2])>5:
                dlList.append({'id':row[0],'name':row[1],'url':row[2]})
        print('total:'+str(len(dlList)))

    for dl in dlList:
        dlQueue.put(dl)

if __name__ == '__main__':
	mainFunction()