# -*- coding: utf-8 -*-
import re
from tool import getResult,getCursor,readTXT
from bs4 import BeautifulSoup
import os
files_path = 'E:/Code/Data/dlibrary'
conn,cur = getCursor()
#import os;os.chdir('e:/Code/Python');import extractInstitution;extractInstitution.mainFunction()
def cleanInstit(instit):
    #
    institNew = ''
    for inst in instit.split(' '):
        institNew += re.sub('[^a-zA-Z]','',inst)+' '
    return institNew.strip()
    
def readFiles(files_path):
    #
    filePathList = []
    for fileName in os.listdir(files_path):
        if len(fileName)>1:
            newFilePath = files_path+'/'+fileName
            filePathList.append(newFilePath)
    return filePathList

def insertInstitution(eid,institution,fileP):
    #放入一个一维数组
    insertSQL = ''
    
    for inst in institution:
        try:
            insertSQL = 'insert into experience1 (eid,institution) values('+str(eid)+', "'+inst+'")'
            cur.execute(insertSQL)
            conn.commit()
        except Exception:
            print('error:'+insertSQL)

    #cur.execute('update dlurl1 set status= where id='+str(eid))#标记已抽取
    #conn.commit()
    
    print('Competed '+fileP)

def extractInstitut(html):
    #
    institution = []
    #找到<strong> Affiliation history
    #它的下一个div
    #里面的每一个a 
    soup = BeautifulSoup(''.join(html),"lxml")
    history = soup.find('history')
    
    strongTag = history.find(text='Affiliation history')

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
            return institution
    except Exception:
        print('error:'+str(strongTag))     

def extractUserID(url):
    #
    url = url.split('&')[0]
    urlid = url[:]
    id = urlid.replace('http://dl.acm.org/author_page.cfm?id=','')
    userid = id[4:]#only numbers begin at 4 are considered     
    return urlid,userid

def getID(html):
    #
    eid = -1 #初始化
    indx = '<![CDATA['
    start = html.find(indx)
    end = html.find(']]></fullpath>')
    if start>0:
        subjectURL = html[(start+len(indx)):end]        
        url,userid = extractUserID(subjectURL)#从网址中分离出url地址
        #回查数据库
        selectSQL = 'select t.id from (select id,url from dlurl1 where userid='+str(userid)+') t where t.url="'+url+'"'
        result = getResult(selectSQL,cur)
        
        if len(result)==1:
            eid = int(result[0]['id'])
        else:
            print('error or exist')
    return eid

def mainFunction():
    #
    #读取文件
    filePathList = readFiles(files_path)
    print('read is ready')
    for fileP in filePathList:
        html = readTXT(fileP)
        #print('do here')
        eid = getID(html)
        #print('do here0')
        if eid >0:
            instit = extractInstitut(html)
            
            if len(instit)>0:
                #print('do here1')
                insertInstitution(eid,instit,fileP)
            
            #print(instit)
        #break#只运行一次
    cur.close();conn.close();

if __name__ == '__main__':
    mainFunction()