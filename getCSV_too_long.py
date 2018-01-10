'''
	find that too long csv, get them directly
'''
from queue import Queue
from threading import Thread
import time
import random
from bs4 import BeautifulSoup
from tool import getHttpUa,getPage,ChangeOrNot
from tool import editeHeader,editeCookies,commHttpProxies,commHeaders,commCookies
import csv
import requests

ipQueue = Queue()
uaQueue = Queue()
dlQueue = Queue() # id,name,url     get from .csv
htmlQueue = Queue()# id,name,csv   get from url

expertList_path = 'E:/Code/Data/papercsvOthers.csv'
file_path = 'E:/Code/Data/csvTooLong/paperlist_'

class analysisWorker(Thread):
    def __init__(self,uaQueue,ipQueue,htmlQueue):
        Thread.__init__(self)
        self.ipQueue = ipQueue
        self.htmlQueue = htmlQueue
        self.uaQueue = uaQueue
        
    def run(self):
        while True:
           # 从队列中获取任务并扩展tuple
            httpProxies = commHttpProxies.copy()
            headers = commHeaders.copy()
            cookies = commCookies.copy()
            
            ua = self.uaQueue.get()
            dl  = self.htmlQueue.get()
            http = self.ipQueue.get()
            httpProxies['https'] = http
            #修饰参数
            if ChangeOrNot() == True:#随机触发
                headers=editeHeader(ua,headers,dl['name'])#改变user agent
                cookies=editeCookies(cookies)
            time.sleep(random.randint(6, 15))#随机休眠

            #-----------------------------这里就是不一样的地方
            
            #取出csv
            flag = findCSV(dl,httpProxies,headers,cookies)#取出url
            self.ipQueue.put(http)#放回
            self.uaQueue.put(ua)
            if not flag:#未获取成功，重新放入
                self.htmlQueue.put(dl)
            #放入
            self.htmlQueue.task_done()

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
            papercsv = getCsvUrl(html)
            dl['papercsv'] = papercsv
            #放回
            self.ipQueue.put(http)
            self.uaQueue.put(ua)
            if html == ' ':#未获取成功，重新放入
                self.dlQueue.put(dl)
            #放入
            else:
                print('get: '+dl['id'])
            self.htmlQueue.put(dl)
            self.dlQueue.task_done()

def findCSV(dl,httpProxies,headers,cookies):
    #
    url = dl['papercsv']
    if url != None and len(url)> 15:
        try:
            r = requests.get(url.encode().decode('utf-8'), proxies = httpProxies, headers = headers, timeout=30)
            if r.status_code == 200:
                csv_path = file_path+str(dl['id'])+'.csv'
                with open(csv_path,'wb') as csv:
                    csv.write(r.content)
                    print('complete '+str(dl['id']))
                    return True
        except requests.RequestException as e:
            print(e)
            return False
def extractInstitut(html):
    #
    institution = []
    #找到<strong> Affiliation history
    #它的下一个div
    #里面的每一个a 
    soup = BeautifulSoup(''.join(html),"lxml")
    history = soup.find('history')
    
    strongTag = history.find(text='Affiliation history')
    #print('no. 1')
    #print(str(strongTag))
    if strongTag != None:
        strongTag = strongTag.parent
        #print('no. 2')
        #print(str(strongTag))
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
            return institution
    except Exception:
        print('error:'+str(strongTag))  
def mainFunction():
    #
    #0.数据准备
    http,uag = getHttpUa()

    for ip in http:
        ipQueue.put(ip)
    for ua in uag:
        uaQueue.put(ua)

    for k in range(1):
        aWorker = analysisWorker(ipQueue,uaQueue,htmlQueue)
        aWorker.daemon = True
        aWorker.start()

    for i in range(1):
        pWorker = pageWorker(ipQueue,uaQueue,dlQueue,htmlQueue)
        pWorker.daemon = True
        pWorker.start()  

    #1.get that list of error csv
    dlList=[]
    with open(expertList_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dlList.append(row)
            
    
    #dl 是字典型的
    for dl in dlList:
        dlQueue.put(dl)

if __name__ == '__main__': 
    mainFunction()