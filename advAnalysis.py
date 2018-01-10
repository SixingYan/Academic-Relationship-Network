# -*- coding: utf-8 -*-
import os
from tool import getCursor,readTXT,getResult,writeTXT,writeMetrx
from bs4 import BeautifulSoup

files_path = 'E:/Code/Data/advdl1'
errorcsv_path = 'E:/Code/Data/Error/insertError_1.29.csv'
#import os;os.chdir('e:/Code/Python');import advAnalysis;advAnalysis.mainFunction()
SQLerror_path = 'E:/Code/Data/SQL_error.csv'
Dir_path = 'E:/Code/Data/Error/'
csvError_path = 'csvErrorHtml_'
writePath = 'E:/Code/Data/advURL1.csv'

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

def extractPaperID(pidStr):
	#
	if type(pidStr) != str:
		pidStr = str(pidStr) 
	newID = pidStr[(len(pidStr)-4):]
	paperID = int(newID)
	return paperID

def dlInfo(html,soup):
    #
    indx = '<![CDATA['
    start = html.find(indx)
    end = html.find(']]></fullpath>')
    if start>0:
        url = html[(start+len(indx)):end]
        url,userid = extractUserID(url)
        selectSQL = 'select t.id,t.name from (select id,name,url from dlurl1 where userid='+str(userid)+') t where t.url="'+url+'"'
        result = getResult(selectSQL,cur)
        if len(result)==1:
            id = int(result[0]['id'])
            name = result[0]['name']
            advisorcsv = getCsvUrl(soup,html,id)
            writeMetrx(writePath,[[id,name,advisorcsv]])#给二维数组
            print('complete:'+str(id))            
        else:
            print('error or exist')
        
#获取csv地址
def getCsvUrl(soup,doc,id):
    #
    url = ' '
    try:
        csv = soup.find('csv')
        url = 'http://dl.acm.org/'+csv.a['href']
    except Exception:
        print('error:'+str(csv))
        path = Dir_path+csvError_path+str(id)+'.txt'
        writeTXT(path,doc)
            
    return url

def extractUserID(url):
    #
    url = url.split('&')[0]
    urlid = url[:]
    id = urlid.replace('http://dl.acm.org/author_page.cfm?id=','')
    userid = id[4:]#only numbers begin at 4 are considered     
    return urlid,userid

def mainFunction():
    #
    filePathList = readFiles()
    #从列表中读取文件
    for i in range(len(filePathList)):#用下标的形式，方便控制数量
        html = readTXT(filePathList[i])
        soup = BeautifulSoup(''.join(html),"lxml")
        dlInfo(html,soup)#
        
    cur.close();conn.close()
    
if __name__ == '__main__':
    #
    filePathList = readFiles()
    #从列表中读取文件
    for i in range(len(filePathList)):#用下标的形式，方便控制数量
        
        html = readTXT(filePathList[i])
        soup = BeautifulSoup(''.join(html),"lxml")
        dlInfo(html,soup)#
        
    cur.close();conn.close()

        




















