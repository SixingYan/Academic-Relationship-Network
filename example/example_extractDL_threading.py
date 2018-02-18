from queue import Queue
from threading import Thread
from bs4 import BeautifulSoup
import requests

#import os;os.chdir('e:/Code/Python');import extractDL_treading;extractDL_treading.mainFunction()
sltDLNotCom = 'select id,name,url from dlurl1 where status=0 and tem=1 limit 200000'

dlQueue = Queue()
htmlQueue = Queue()
infoQueue = Queue()

global httpProxies
global headers
global cookies

httpProxies = {'https':'182.253.121.137:8080'}
headers = {
    'content-type':'application/json',
    'User-Agent':'Opera/9.25 (Windows NT 5.1; U; en)',
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language':'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding':'gzip, deflate, br',
    'Referer':'https://dl.acm.org/results.cfm?query=big+data&Go.x=30&Go.y=11'}
cookies = {
    'CFID':716361429,
    'CFTOKEN':70843524,
    'DEEPCHK':'1'}

class pageWorker(Thread):
    def __init__(self,ipQueue,uaQueue,dlQueue,htmlQueue):
        Thread.__init__(self)
        self.ipQueue = ipQueue
        self.uaQueue = uaQueue
        self.dlQueue = dlQueue
        self.htmlQueue = htmlQueue
        
    def run(self):
        while True:
            #取出html
            html = str(getPage(dl['url'],httpProxies,headers,cookies))#取出url
            #放入
            self.htmlQueue.put((html,dl))
            self.dlQueue.task_done()

class analysisWorker(Thread):
    def __init__(self,htmlQueue,infoQueue):
        Thread.__init__(self)
        self.htmlQueue = htmlQueue
        self.infoQueue = infoQueue

    def run(self):
        while True:
           html,dl = self.htmlQueue.get()
           infoSet = analysisPage(html,dl['id'])
           #放入
           self.infoQueue.put((infoSet,dl))
           self.htmlQueue.task_done()

class mysqlWorker(Thread):
    def __init__(self,infoQueue):
        Thread.__init__(self)
        self.infoQueue = infoQueue

    def run(self):
        while True:
            infoSet,dl = infoQueue.get()
            #处理
            addInfo(infoSet,dl)
            self.infoQueue.task_done()

def getCursor():
    #
    conn = pymysql.connect(host='localhost',user='root',passwd='',db='academic',port=3306,charset='utf8')
    #游标设置为字典类型
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    return conn,cur

def getPage(url,httpProxies,headers,cookies):
    #获取页面信息input is a string of url
    htmlText = ' '
    try:
        r = requests.get(url.encode().decode('utf-8'), proxies=httpProxies, headers=headers, timeout=30)
        if r.status_code == 200:
            r.encoding = 'utf-8'
            htmlText = r.text
            return htmlText
        else:
            print('error: code = ' + str(r.status_code))
            print('error: url = '+ url)
            return htmlText
    except requests.RequestException as e:
        print(e)
        return htmlText

def analysisPage(html,id):
    #
    soup = BeautifulSoup(''.join(html),"lxml")
    name = personInfo(soup)
    return name

def addInfo(infoSet,dl):
    #
    conn,cur = getCursor()#获取数据库连接和游标
    #根据不同情况构造插入语句，使用拼接的方法
    cur.execute('update dlurl1 set status=1 where id='+str(dl['id']))#标记已抽取
    conn.commit()
    cur.close();conn.close()
    print('Competed '+str(dl['id']))

#抽取个人信息
def personInfo(soup):
    #情况：为空/Add personal information/只有homepage/只有邮箱/两者都有
    name = ' '
    #获取名称
    t1 = "#cccccc"
    t2 = "padding-bottom: 5px; padding-top: 5px"

    tdTag = soup.findAll('td',{'bgcolor':t1,'style':t2})#查找条件，使用标签的属性
    for td in tdTag:
        if td.span != None and td.span.strong != None:
            name = td.span.strong.string
            break

    return name

def mainFunction():
    
    for k in range(1):
        aWorker = analysisWorker(htmlQueue,infoQueue)
        aWorker.daemon = True
        aWorker.start()
    print('ok1')
    
    for i in range(4):
        pWorker = pageWorker(ipQueue,uaQueue,dlQueue,htmlQueue)
        pWorker.daemon = True
        pWorker.start()
    print('ok2')

    conn,cur = getCursor()
    dlList = getResult(sltDLNotCom,cur)#返回url实体的二维数组
    for dl in dlList:
        dlQueue.put(dl)
    cur.close()
    conn.close()
    print('ok3')
    
    for j in range(1):
        mWorker = mysqlWorker(infoQueue)
        mWorker.daemon = True
        mWorker.start()
        
if __name__ == '__main__':
    mainFunction()
    