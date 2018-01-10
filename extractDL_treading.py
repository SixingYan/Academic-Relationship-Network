from queue import Queue
from threading import Thread
import time
import random
from bs4 import BeautifulSoup
from tool import getHttpUa,getCursor,getResult,getPage,isTheSame,ChangeOrNot
from tool import editeHeader,editeCookies,writeList,writeTXT,commHttpProxies,commHeaders,commCookies
#import datetime
#import os;os.chdir('e:/Code/Python');import extractDL_treading;extractDL_treading.mainFunction()
sltDLNotCom = 'select * from dlurl1 where status=0 and tem=1 limit 200000'
SQLerror_path = 'E:/Code/Data/SQL_error.csv'
Dir_path = 'E:/Code/Data/Error/'
csvError_path = 'csvErrorHtml_'
collError_path = 'collErrorHtml_'
advError_path = 'advErrorHtml_'
subjError_path = 'subjErrorHtml_'

ipQueue = Queue()	
uaQueue = Queue()
dlQueue = Queue()
htmlQueue = Queue()
infoQueue = Queue()

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
           # Get the work from the queue and expand the tuple
           # 从队列中获取任务并扩展tuple
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
                #httpProxies=editeProxies(http,httpProxies)#改变http
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
            #放入
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
           infoSet = analysisPage(html,dl['id'])
           #放入
           #print('type is '+str(type(infoSet)))
           #infoSet = {}
           self.infoQueue.put((infoSet,dl))
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
            infoSet,dl = infoQueue.get()
            #ts1 = datetime.datetime.now()
            #处理
            addInfo(infoSet,dl)
            #print('infoset:' +str(infoSet['name']))
            #print('mysql: '+str(dl['id']))
            #ts2 = datetime.datetime.now()
            #print('mysql id:'+str(dl['id'])+' time:'+str(ts2-ts1))
            self.infoQueue.task_done()
            
def analysisPage(doc,id):
    #
    infoSet = {}
    soup = BeautifulSoup(''.join(doc),"lxml")
    #aTag = soup.findAll('a')

    name,homepage,email = personInfo(soup)
    institution = historyInfo(soup)
    csvUrl = getCsvUrl(soup,doc,id)
    advUrl = getAdvisor(soup,doc,id)
    collUrl = getColleagues(soup,doc,id)
    subUrl = getSubject(soup,doc,id)
    
    infoSet['name'] = name
    infoSet['homepage'] = homepage
    infoSet['email'] = email
    infoSet['institution'] = institution
    infoSet['csvUrl'] = csvUrl
    infoSet['advUrl'] = advUrl
    infoSet['collUrl'] = collUrl
    infoSet['subUrl'] = subUrl
    
    #print('THIS IS THE Name!!!!'+infoSet['name'])
    #print('type is '+str(type(infoSet)))
    return infoSet

def addInfo(infoSet,dl):
    #
    conn,cur = getCursor()#获取数据库连接和游标
    #根据不同情况构造插入语句，使用拼接的方法
    if type(infoSet) != dict:
        print('error')
    if infoSet['advUrl'] != ' ':
        updateSQL = 'update dlurl1 set colleage="'+infoSet['collUrl']+'",subject="'+infoSet['subUrl']+'",papercsv="'+infoSet['csvUrl']+'",advisorcsv="'+infoSet['advUrl']+'" where id='+str(dl['id'])
    else:
        updateSQL = 'update dlurl1 set colleage="'+infoSet['collUrl']+'",subject="'+infoSet['subUrl']+'",papercsv="'+infoSet['csvUrl']+'",advisorcsv=Null where id='+str(dl['id'])
    rowSQL = updateSQL[:]
    #判断两个姓名是否相同，不同则合并
    comName = isTheSame(infoSet['name'],dl['name'])

    #增加网址
    try:
        cur.execute(updateSQL)
        conn.commit()
    except Exception:
        print('updata error'+'id: '+str(dl['id']))
        #这里专门为插入数据过长而准备的，将非常可能过长的csvurl去除，保存到单独文件中，并不存入数据库中
        try:
            writeList(SQLerror_path,[updateSQL])#保存到单独文件中
            if infoSet['advUrl'] != ' ':
                updateSQL = 'update dlurl1 set colleage="'+infoSet['collUrl']+'",subject="'+infoSet['subUrl']+'",papercsv="Too long",advisorcsv="'+infoSet['advUrl']+'" where id='+str(dl['id'])
            else:
                updateSQL = 'update dlurl1 set colleage="'+infoSet['collUrl']+'",subject="'+infoSet['subUrl']+'",papercsv="Too long",advisorcsv=Null where id='+str(dl['id'])
            cur.execute(updateSQL)
            conn.commit()
            print('another way of update is successful! Here is the papercsv url:')
            print(rowSQL)
        except Exception:
            print(updateSQL)

    #增加专家
    insertSQL = 'insert into expert (name,homepage,dl) values (%s,%s,%s)'
    insertValues = (comName,infoSet['homepage'],dl['url'])
 
    try:
        cur.execute(insertSQL,insertValues)
        conn.commit()
    except Exception:
        print('insert error '+'id: '+str(dl['id']))

    #增加机构
    insertSQL = ''
    result = getResult('select * from expert order by eid desc limit 1',cur)
    eid = result[0]['eid']
    
    for inst in infoSet['institution']:
        try:
            insertSQL = 'insert into experience (eid,institution) values('+str(eid)+', "'+inst+'")'
            cur.execute(insertSQL)
            conn.commit()
        except Exception:
            print('error:'+insertSQL)

    cur.execute('update dlurl1 set status=1 where id='+str(dl['id']))#标记已抽取
    conn.commit()
    cur.close()
    conn.close()
    print('Competed '+str(dl['id']))

#抽取个人信息
def personInfo(soup):
    #
    #情况：为空/Add personal information/只有homepage/只有邮箱/两者都有
    name = ' '
    homepage = ' '
    email = ' '

    #获取名称
    t1 = "#cccccc"
    t2 = "padding-bottom: 5px; padding-top: 5px"
    #t3 = "2"
    tdTag = soup.findAll('td',{'bgcolor':t1,'style':t2})#查找条件，使用标签的属性
    for td in tdTag:
        if td.span != None and td.span.strong != None:
            name = td.span.strong.string
            break
 
    #获取主页
    home = soup.find(text='homepage')
    if home != None:
        a = home.parent
        homepage = a['href']
    
    #获取电子邮件
    imgTag = soup.find('img',{'src':'gifs/at.gif'})
    if imgTag != None:
        div = imgTag.parent.contents
        div[0].replace('\xa0','')
        email = div[0].strip()+'@'+div[2].strip()

    return name,homepage,email

#抽取affiliation history
def historyInfo(soup):
    #
    institution = []
    strong = soup.findAll('strong')
    
    for strong in soup.findAll('strong'):
        if strong.string == 'Affiliation history':#找到目标字样
            stro = strong.nextSibling
            while stro.name != 'div':
                stro = stro.nextSibling
                if stro == None:
                    break            
            aTag = stro.findAll('a')
            for a in aTag:
                institution.append(a.string)            
            break
            
    return institution

#获取csv地址
def getCsvUrl(soup,doc,id):
    #
    url = ' '
    stri = soup.find(text='csv')#找到目标字样
    if stri != None:
        try:
            url = 'http://dl.acm.org/' + stri.parent['href']
        except Exception:
            print('error:'+str(stri.parent))
            path = Dir_path+csvError_path+str(id)+'.txt'
            writeTXT(path,doc)

    return url

#判断role里面是否含有advisor
def getAdvisor(soup,doc,id):
    #
    url = ' '
    stri = soup.find(text='Advisor only')#找到目标字样
    if stri != None:
        try:
            url = 'http://dl.acm.org/' + stri.parent['href']
        except Exception:
            print('error:'+str(stri.parent))
            path = Dir_path+advError_path+str(id)+'.txt'
            writeTXT(path,doc)

    return url

#获取 colleagues 的地址
def getColleagues(soup,doc,id):
    #
    url = ' '
    stri = soup.find(text='See all colleagues of this author')#找到目标字样
    if stri != None:
        try:
            url = 'http://dl.acm.org/' + stri.parent['href']
        except Exception:
            print('error:'+str(stri.parent))
            path = Dir_path+collError_path+str(id)+'.txt'
            writeTXT(path,doc)

    return url

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
            path = Dir_path+subjError_path+str(id)+'.txt'
            writeTXT(path,doc)
    return url   
    
def mainFunction():
    http,uag = getHttpUa()

    for ip in http:
        ipQueue.put(ip)
    for ua in uag:
        uaQueue.put(ua)
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
    
    http,uag = getHttpUa()

    for ip in http:
        ipQueue.put(ip)
    for ua in uag:
        uaQueue.put(ua)
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
    













