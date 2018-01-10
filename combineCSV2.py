# -*- coding: utf-8 -*-
import csv
import os
from tool import cleanName,getCursor,writeList
import datetime

files_path = 'E:/Code/Data/csv dx4'
errorcsv_path = 'E:/Code/Data/Error/insertError_2.22.csv'
#import os;os.chdir('e:/Code/Python');import combineCSV2;combineCSV2.mainFunction()

conn,cur = getCursor()

#读出csv文件夹
def readFiles():
    #
    filePathList = []
    for fileName in os.listdir(files_path):
        if len(fileName)>1:
            newFilePath = files_path+'/'+fileName
            filePathList.append(newFilePath)
    return filePathList

def readPaperCSV(path):
    #
    paperList = []

    eid = path.split('paperlist_')[1].replace('.csv','')
    
    num = cur.execute('select id from dlurl1 where id='+str(eid))
    if int(num)==0:
        print('pass: '+str(eid))
        return paperList

    num = cur.execute('select id from dlurl1 where id='+str(eid)+' and tem=3')
    if int(num)>0:
        print('pass: '+str(eid))
        return paperList

    with open(path, encoding= 'utf-8') as csvfile:
        reader = csv.DictReader(csvfile)       
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
    #if checkTheSame(cur,paper)==True:
    if False:
        pass
    else:
        insertSQL='insert into paper (pid,title,time,doi,paperid,eid) values ("'+paper['pid']+'","'+paper['title']+'",'+str(paper['year'])+',"'+paper['doi']+'",'+str(paper['paperid'])+','+str(paper['eid'])+')' 
    #加入paper记录
        try:		
            cur.execute(insertSQL)
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

def insertPaperSQL(filePath):
    flag = True
    try:
        paperList = readPaperCSV(filePath)
        if len(paperList)==0:
            return flag
        for paper in paperList:
            try:
                addInfo(paper)

            except Exception:
                print('some paper error!')
                flag = False
        
        cur.execute('update dlurl1 set tem=3 where id='+str(paperList[0]['eid']))
        conn.commit()
        print('complete:'+str(paperList[0]['eid'])+' '+str(datetime.datetime.now()))
    
    except Exception:
        print('some csv error')
        flag = False
    return flag
def mainFunction():
    filePathList = readFiles()
    for fileP in filePathList:
        insertPaperSQL(fileP)
        
if __name__ == '__main__':
    #
    filePathList = readFiles()
    
    for fileP in filePathList:
        insertPaperSQL(fileP)
        

















