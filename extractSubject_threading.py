
'''
	访问URL，重新收集subject地址，然后存入.csv
'''
from queue import Queue
from threading import Thread
import time
import random
import csv
from bs4 import BeautifulSoup
from tool import getHttpUa,getCursor,getResult,getPage,ChangeOrNot
from tool import editeHeader,editeCookies,writeMetrx,commHttpProxies,commHeaders,commCookies
#sltDLNotCom = 'select id,url,name from dlurl1 where (subject=" " or subject is null) and id>2810844 limit 50000' #13000 zuoyou
#import os;os.chdir('e:/Code/Python');import extractSubject_threading;extractSubject_threading.mainFunction()

path = 'E:/Code/Data/subUrl last 3.csv'
path2 = 'E:/Code/Data/subUrlSupply last 3.csv'
path3 = 'E:/Code/Data/subUrlNull last 3.csv'
expertList_path = 'E:/temp_csv/重置/subject url last 3.csv'
flag = 2 #选择http的来源文件
ipQueue = Queue()	
uaQueue = Queue()
dlQueue = Queue()
htmlQueue = Queue()
infoQueue = Queue()
#import os;os.chdir('e:/Code/Python');import extractSubject_threading;extractSubject_threading.mainFunction() 
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
                print('put back'+str(dl['id']))
                self.dlQueue.put(dl)
            #放入
            else:
                self.htmlQueue.put((html,dl))
            self.dlQueue.task_done()

class analysisWorker(Thread):
    def __init__(self,htmlQueue,infoQueue):
        Thread.__init__(self)
        self.htmlQueue = htmlQueue

    def run(self):
        while True:
           html,dl = self.htmlQueue.get()
           analysisPage(html,dl['id'],dl['name'])
           self.htmlQueue.task_done()

#获取 subject area的地址
def getSubject(soup,doc,id):
    #
    url = ' '
    stri = soup.find(text='See all subject areas')#找到目标字样
    if stri != None:
        try:
            url = 'http://dl.acm.org/' + stri.parent['href']
        except Exception:
            print('error:'+str(stri.parent))
            
    return url   
#获取 subject area的地址
def getSubject2(soup,doc,id):
    #
    url = ' '
    stri = soup.find(text='See all author supplied keywords')#找到目标字样
    if stri != None:
        try:
            url = 'http://dl.acm.org/' + stri.parent['href']
        except Exception:
            print('error:'+str(stri.parent))
            
    return url
def analysisPage(doc,id,name):
    #
    soup = BeautifulSoup(''.join(doc),"lxml")
    subUrl = getSubject(soup,doc,id)
    if subUrl==' ':
        subUrl = getSubject2(soup,doc,id)
        if subUrl == ' ':
            #空占位
            try:
                #print([id,'Null'])
                writeMetrx(path3,[[id,'Null']])
                print('complete null: '+str(id))
            except Exception:
                print('write list error3'+str(id))
        else:
            try:
                #print([id,subUrl])
                writeMetrx(path2,[[id,name,subUrl]])
                print('complete: '+str(id))
            except Exception:
                print('write list error2'+str(id))
    #
    else:
        try:
            print([id,subUrl])
            writeMetrx(path,[[id,name,subUrl]])
            print('complete: '+str(id))
        except Exception:
            print('write list error1'+str(id))

def mainFunction():
    http,uag = getHttpUa()

    for ip in http:
        ipQueue.put(ip)
    for ua in uag:
        uaQueue.put(ua)
    for k in range(2):
        aWorker = analysisWorker(htmlQueue,infoQueue)
        aWorker.daemon = True
        aWorker.start()
    print('ok1')
    for i in range(8):
        pWorker = pageWorker(ipQueue,uaQueue,dlQueue,htmlQueue)
        pWorker.daemon = True
        pWorker.start()
    print('ok2')
    '''
    conn,cur = getCursor()
    dlList = getResult(sltDLNotCom,cur)#返回url实体的二维数组
    '''
    dlList=[]
    with open(expertList_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if int(row['id']) > 0:
                dlList.append(row)
            #break
        print('total:'+str(len(dlList)))

    for dl in dlList:
        dlQueue.put(dl)
    #cur.close()
    #conn.close()

if __name__ == '__main__':
	mainFunction()








