# -*- coding: utf-8 -*-
'''
用于获得新的合作专家
'''
from queue import Queue
from threading import Thread
from tool import getCursor,readTXT
import os
from bs4 import BeautifulSoup
#import os;os.chdir('e:/Code/Python');import extractColleagues_xml;extractColleagues_xml.mainFunction()

fileQueue = Queue()

files_path = 'E:/Code/Data/xml'
conn,cur = getCursor()

class pageWorker(Thread):
    def __init__(self,fileQueue):
        Thread.__init__(self)
        self.fileQueue = fileQueue
        
    def run(self):
        while True:
            #取出html
            filePath = self.fileQueue.get()
            html = str(readTXT(filePath))#取出url
            print('get: '+filePath)
            if html.find('<全部同伴的名字>')>0:
                ind = html.find('<全部同伴的名字>')
                html = html[:ind]
            nameLink = analysisPage(html)
            if len(nameLink)>0:
               for nl in nameLink:
                   addInfo(nl)
            self.fileQueue.task_done()

def analysisPage(doc):
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
                    url = 'http://dl.acm.org/' + div.a['href']
                    url,userID = extractUserID(url)

                    nameLink.append([name,url,userID])                        
    except Exception:
        print('error:'+str(a))

    return nameLink

def checkSame(url,userID):
    #
    try:
        checkSQL = 'select id,url from dlurl1 where userid='+str(userID)
        number = cur.execute(checkSQL)
        if number>0:#如果结果集记录大于0
            for c in cur.fetchall():#提取每个记录
                if c['url']==url:#对url是否相同
                    return True
        return False
    except Exception:
        print('check error!:'+url)
        return True
    
def cleanName(name):
    newName = ''
    for n in name.split(' '):
        if len(n)==0:
            continue
        else:
            newName = newName +n.strip()+' '
    return newName.strip()

def addInfo(nl):
    insertSQL='insert into dlurl1 (name,url,status,tem,userID) values("'+nl[0]+'","'+nl[1]+'",0,1,'+str(nl[2])+');'
    try:
        cur.execute(insertSQL)
        conn.commit()
    except Exception:
        print('insert error:')
        print(insertSQL)

def extractUserID(url):
    url = url.split('&')[0]
    userid = url[:]
    userid = userid.replace('http://dl.acm.org/author_page.cfm?id=','')
    userid = userid[4:]#only numbers begin at 4 are considered     
    return url,int(userid)

def readFiles():
    #
    filePathList = []
    for fileName in os.listdir(files_path):
        if len(fileName)>1:
            newFilePath = files_path+'/'+fileName
            filePathList.append(newFilePath)
    return filePathList
def mainFunction():
    filePathList = readFiles()
    for filePath in filePathList:
        fileQueue.put(filePath)
    for i in range(1):
        pWorker = pageWorker(fileQueue)
        pWorker.daemon = True
        pWorker.start()
if __name__ == '__main__':
    #
    filePathList = readFiles()
    for filePath in filePathList:
        fileQueue.put(filePath)
    for i in range(1):
        pWorker = pageWorker(fileQueue)
        pWorker.daemon = True
        pWorker.start()
    
    
    