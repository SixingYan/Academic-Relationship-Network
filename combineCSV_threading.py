# -*- coding: utf-8 -*-
import csv
import os
from queue import Queue
from threading import Thread
from tool import cleanName,combineList,getCursor,writeList
import time
import datetime
files_path = 'E:/Code/Data/csv6'
errorcsv_path = 'E:/Code/Data/Error/insertError_2.19.csv'
#import os;os.chdir('e:/Code/Python');import combineCSV_threading;combineCSV_threading.mainFunction()
fileQueue = Queue()
sqlQueue = Queue()
conn,cur = getCursor()

class readWorker(Thread):
    def __init__(self,fileQueue,sqlQueue):
        Thread.__init__(self)
        self.fileQueue = fileQueue
        self.sqlQueue = sqlQueue

    def run(self):
        while True:
            if self.sqlQueue.qsize()>10000:
                print('too large,sleep a monent')
                time.sleep(15)
            path = self.fileQueue.get()
            #path = infoset['path']
            #eid = infoset['eid']
            paperList = readPaperCSV(path)
            #if os.path.exist(path):#放入一个，删除一个
            #os.remove(path)
            if len(paperList)>0:
                for paper in paperList:
                    self.sqlQueue.put(paper)

            self.fileQueue.task_done()

class saveWorker(Thread):
    def __init__(self,sqlQueue):
        Thread.__init__(self)
        self.sqlQueue = sqlQueue
        
    def run(self):
        while True:
            paper = self.sqlQueue.get()
            addInfo(paper)
            if self.sqlQueue.qsize() == 0:
                print('sqlQueue is empty!!')
            self.sqlQueue.task_done()

#读出csv文件夹
def readFiles():
    #
    filePathList = []
    for fileName in os.listdir(files_path):
        if len(fileName)>1:
            newFilePath = files_path+'/'+fileName
            #infoset = {}
            #infoset['path'] = newFilePath
            #infoset['eid'] = int(fileName.replace('.csv','').replace('paperlist_',''))
            filePathList.append(newFilePath)
            #eidList.append()
    #print(filePathList)
    return filePathList

def readPaperCSV(path):
    #
    paperList = []
    with open(path, encoding= 'utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        eid = path.split('paperlist_')[1].replace('.csv','')
        num = cur.execute('select * from dlurl1 where tem=3 and id='+str(eid))
        if num>0:
            print('pass: '+str(eid))
            return paperList
    
        for row in reader:
            paper = {}
            paper['pid'] = str(row['id']).strip()
            paper['authors'] = cleanAuthors(row['author'])
            paper['title'] = row['title'].strip()
            paper['doi'] = row['doi'].strip()
            if len(str(row['year']))>0:
                paper['year'] = int(row['year'])
            else:
                paper['year'] = 0
            paper['paperid'] = int(extractPaperID(row['id'].strip()))
            paper['eid'] = int(eid)
            paperList.append(paper)

    return paperList

def cleanAuthors(authors):
	#return list of authors
	authorsList = authors.split('and')
	for i in range(len(authorsList)):
		authorsList[i] = cleanName(authorsList[i].strip())

	return authorsList

def extractPaperID(pidStr):
	#
	if type(pidStr) != str:
		pidStr = str(pidStr) 
	newID = pidStr[(len(pidStr)-4):]
	paperID = int(newID)
	return paperID

def addInfo(paper):
    #
    #conn,cur = getCursor()
    #if checkTheSame(cur,paper)==True:
    if False:
        #cur.close();conn.close()
        pass
    else:
    #insertSQL = 'insert into paper (pid,title,time,doi,paperid,authors) values ("","",,"",,"")'
    #insertSQL = 'insert into paper (pid,title,time,doi,paperid,authors) values ("","",,"",,"'
        insertSQL='insert into paper (pid,title,time,doi,paperid,eid) values ("'+paper['pid']+'","'+paper['title']+'",'+str(paper['year'])+',"'+paper['doi']+'",'+str(paper['paperid'])+','+str(paper['eid'])+')' 
    #加入paper记录
        try:		
            cur.execute(insertSQL)
            conn.commit()
            print('complete:'+str(paper['eid'])+' '+str(datetime.datetime.now()))
            cur.execute('update dlurl1 set tem=3 where id='+str(paper['eid']))
            conn.commit()
        except Exception:
            print('error!:'+str(insertSQL))
            writeList(errorcsv_path,[insertSQL])
        
    #加入专家记录
        for author in paper['authors']:
            try:
                insertSQL2 = 'insert into authors (pid,paperid,author) values ("'+paper['pid']+'",'+str(paper['paperid'])+',"'+author+'")'
                cur.execute(insertSQL2)
                conn.commit()
            except Exception:
                print('error!:'+str(insertSQL2))
                writeList(errorcsv_path,[insertSQL2])
            
    #cur.close();conn.close()

def checkTheSame(cur,paper):
	flag = False
	checkSQL = 'select pid from paper where paperid='+str(paper['paperid'])
	num = cur.execute(checkSQL)
	if num>0:
		for c in cur.fetchall():
			if c['pid']==paper['pid']:
				flag = True
				break
	return flag
def mainFunction():
    #
    filePathList = readFiles()
    
    for filePath in filePathList:
        fileQueue.put(filePath)
    
    for j in range(1):
        sWorker = saveWorker(sqlQueue)
        sWorker.daemon = True
        sWorker.start()
    
    for i in range(1):
        rWorker = readWorker(fileQueue,sqlQueue)
        rWorker.daemon = True
        rWorker.start()
        
if __name__ == '__main__':
    #
    filePathList = readFiles()
    
    for filePath in filePathList:
        fileQueue.put(filePath)
    
    for j in range(1):
        sWorker = saveWorker(sqlQueue)
        sWorker.daemon = True
        sWorker.start()
    
    for i in range(1):
        rWorker = readWorker(fileQueue,sqlQueue)
        rWorker.daemon = True
        rWorker.start()
    
    
        




















