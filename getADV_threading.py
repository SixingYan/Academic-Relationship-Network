from queue import Queue
from threading import Thread
import time
import random
import requests
import csv
from tool import getHttpUa,ChangeOrNot
from tool import editeHeader,editeCookies,commHttpProxies,commHeaders,commCookies

ipQueue = Queue()   
dlQueue = Queue()
uaQueue = Queue()

global httpProxies
global headers
global cookies
#import os;os.chdir('e:/Code/Python');import getADV_threading;getADV_threading.mainFunction()
file_path = 'E:/Code/Data/adv/advlist_'
expertList_path = 'E:/Code/Data/advURL1.csv'
class pageWorker(Thread):
    def __init__(self,uaQueue,ipQueue,dlQueue):
        Thread.__init__(self)
        self.ipQueue = ipQueue
        self.dlQueue = dlQueue
        self.uaQueue = uaQueue
        
    def run(self):
        while True:
           # Get the work from the queue and expand the tuple
           # 从队列中获取任务并扩展tuple
            httpProxies = commHttpProxies.copy()
            headers = commHeaders.copy()
            cookies = commCookies.copy()
            
            ua = self.uaQueue.get()
            dl  = self.dlQueue.get()
            http = self.ipQueue.get()
            httpProxies['https'] = http
            #ts1 = datetime.datetime.now()
            #修饰参数
            if ChangeOrNot() == True:#随机触发
                #httpProxies=editeProxies(http,httpProxies)#改变http
                headers=editeHeader(ua,headers,dl['name'])#改变user agent
                cookies=editeCookies(cookies)
            time.sleep(random.randint(7, 15))#随机休眠

            #取出html
            flag = findCSV(dl,httpProxies,headers,cookies)#取出url
            #html = readTXT('E:/Code/Test Data/Yu Zheng - ACM author profile page.txt')
            self.ipQueue.put(http)#放回
            self.uaQueue.put(ua)
            if not flag:#未获取成功，重新放入
                self.dlQueue.put(dl)
            #放入
            #print('get: '+str(dl['id']))
            #ts2 = datetime.datetime.now()
            #print('page id:'+str(dl['id'])+' time:'+str(ts2-ts1))
            self.dlQueue.task_done()

def findCSV(dl,httpProxies,headers,cookies):
    #
    url = dl['advisorcsv']
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

def mainFunction():
    http,uag = getHttpUa()

    for ua in uag:
        uaQueue.put(ua)
        
    for ip in http:
        ipQueue.put(ip)
    
    for i in range(5):
        pWorker = pageWorker(uaQueue,ipQueue,dlQueue)
        pWorker.daemon = True
        pWorker.start()

    dlList = []
    with open(expertList_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dlList.append(row)

    for dl in dlList:
        dlQueue.put(dl)
            
            
if __name__ == '__main__': 
    mainFunction()
    