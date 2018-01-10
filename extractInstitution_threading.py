# -*- coding: utf-8 -*-
'''
	直接抽取机构
'''
import re
from queue import Queue
from threading import Thread
import time
import random
import csv
from bs4 import BeautifulSoup
from tool import getHttpUa,getPage,ChangeOrNot
from tool import editeHeader,editeCookies,writeList,commHttpProxies,commHeaders,commCookies
#import os;os.chdir('e:/Code/Python');import extractInstitution_threading;extractInstitution_threading.mainFunction()

saveFile_path = 'e:/Code/Data/institution new2/inst_'
expertList_path = 'E:/temp_csv/重置/institution final.csv'
#conn,cur = getCursor()

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
                self.dlQueue.put(dl)
                print('put back'+str(dl['id']))
            #放入
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
            institution = historyInfo(html)
            #processing and put into mysql
            if len(institution)>0:
                saveInstitution(dl['id'],institution)
            else:     
                #如果没有，就占位就行
                saveNull(dl['id'])
            self.htmlQueue.task_done()

#抽取affiliation history
def historyInfo(html):
    #
    institution = []
    #找到<strong> Affiliation history
    #它的下一个div
    #里面的每一个a
    if not (('Affiliation history' in html) or ('affiliation history' in html)):
        #没有找到
        return institution
    soup = BeautifulSoup(''.join(html),"lxml")
    #history = soup.find('history')
    strongTag = soup.find(text='Affiliation history')
    #print(strongTag)
    if strongTag != None:
        strongTag = strongTag.parent
    else:
        return institution

    while (type(strongTag.nextSibling) != 'NoneType') or (strongTag.nextSibling.name != 'div'):
        #print(' ---loop--- ')
        strongTag = strongTag.nextSibling
        #print(str(strongTag))
        if strongTag.name == 'div':
            break
        if strongTag == None:
            print('no find?')
            break
    try:
        if strongTag.findAll('a') != None:
            for a in strongTag.findAll('a'):
                instName = cleanInstit(a.string) 
                institution.append(instName)
                #print(institution)
    except Exception:
        print('error:'+str(strongTag))
    return institution

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
    #print(fileP)
    #print(institution)
    try:
        writeList(fileP,institution)
        print('Competed '+str(eid))
    except Exception:
    	print('write error!')
    
def cleanInstit(instit):
    #
    institNew = ''
    for inst in instit.split(' '):
        institNew += re.sub('[^a-zA-Z]','',inst)+' '
    return institNew.strip()

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
    for i in range(6):
        pWorker = pageWorker(ipQueue,uaQueue,dlQueue,htmlQueue)
        pWorker.daemon = True
        pWorker.start()
    print('ok2')
    
    #dlList = getResult(sltDLNotCom,cur)#返回url实体的二维数组
    dlList=[]
    with open(expertList_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if int(row['id']) > 0:
                dlList.append(row)
                #break
            #break
        print('total:'+str(len(dlList)))

    for dl in dlList:
        dlQueue.put(dl)

if __name__ == '__main__':
    mainFunction()
