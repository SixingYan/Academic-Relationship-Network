from queue import Queue
import time
import random
import csv
from tool import getHttpUa,getPage,ChangeOrNot,writeTXT
from tool import editeHeader,editeCookies,commHttpProxies,commHeaders,commCookies
#import datetime
#import os;os.chdir('e:/Code/Python');import extractDL_treading;extractDL_treading.mainFunction()

expertList_path = 'e:/test.csv'
dirPath = 'e:/html_'

ipQueue = Queue()	
uaQueue = Queue()
dlQueue = Queue()

global httpProxies
global headers
global cookies
        
if __name__ == '__main__':
    
    http,uag = getHttpUa()
    dlList = []
    
    with open(expertList_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dlList.append(row)
    for dl in dlList:
        dlQueue.put(dl)
        
    httpProxies = commHttpProxies.copy()
    headers = commHeaders.copy()
    cookies = commCookies.copy()

    while not dlQueue.empty():

        dl = dlQueue.get()
        http = random.choice(http)        
        ua = random.choice(uag)
        httpProxies['https'] = http
        #ts1 = datetime.datetime.now()
        #修饰参数
        if ChangeOrNot() == True:#随机触发
            headers=editeHeader(ua,headers,dl['name']) #改变user agent
            cookies=editeCookies(cookies)
        time.sleep(random.randint(5, 20))#随机休眠

        #取出html
        html = str(getPage(dl['url'],httpProxies,headers,cookies))#取出url
        
        #放回
        if html == ' ':#未获取成功，重新放入
            dlQueue.put(dl)
        #放入文件中
        try:
            path = dirPath + str(dl['id'])+'.txt'
            writeTXT(path,html)
            print('complete: '+str(dl['id']))
        except Exception:
            print('error:'+str(dl['id']))
            dlQueue.put(dl)

    
    
    













