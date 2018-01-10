# -*- coding: utf-8 -*-
'''
	直接抽取主题词
'''
from bs4 import BeautifulSoup
from tool import getCursor,getResult,readFiles,readTXT
#import os;os.chdir('e:/Code/Python');import analysisTopic_xml;analysisTopic_xml.mainFunction()

files_path = 'D:/imported'
conn,cur = getCursor()

def extractTopic(id,html):
    #记下那个tag就行，放到txt里
    topic = []
    if not 'thekeywords' in html:
        return topic
    soup = BeautifulSoup(''.join(html),"lxml")
    div = soup.find('div',{'id':"thekeywords"})
    try:
        if len(str(div))<10 or type(div) == 'NoneType' or div == '' or div == None:
            print('analysis error none!'+str(id))
            return topic
        for a in div.findAll('a'):
            topicContent = a.string
            if len(str(topicContent))<4 or type(topicContent)=='NoneType':
                print('analysis none'+str(id))
                return topic
            if ',' in topicContent:
                topicList = topicContent.split(',')
                for tpc in topicList:
                    topic.append(tpc.strip())
            else:
                topic.append(a.string)
    except Exception:
        print('analysis error!'+str(id))
    return topic

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
        selectSQL = 'select t.id from (select id,url from dlurl1 where status<>2) t where t.url="'+url+'"'
        result = getResult(selectSQL,cur)
        
        if len(result)==1:
            eid = int(result[0]['id'])
        else:
            print('exist')
    return eid
    
def insertTopic(eid,topicIDList,fileP):
    #放入一个一维数组
    insertSQL = ''
    for topicID in topicIDList:
        try:
            insertSQL = 'insert into topic (eid,topic,num) values ('+str(eid)+',"'+topicID+'",'+str(1)+')'
            cur.execute(insertSQL)
            conn.commit()
        except Exception:
            print('error:'+insertSQL)
    try:
        updateSQL = 'update dlurl1 set status=2 where id='+str(eid)
        cur.execute(updateSQL)#标记已抽取
        conn.commit()
    except Exception:
        print('error: '+updateSQL)
    print('Competed '+fileP)

def extractUserID(url):
    #
    url = url.split('&')[0]
    urlid = url[:]
    id = urlid.replace('http://dl.acm.org/author_page.cfm?id=','')
    userid = id[4:]#only numbers begin at 4 are considered     
    return urlid,userid
    
def mainFunction():
    #
    #读取文件
    filePathList = readFiles(files_path)
    print('read is ready')
    for fileP in filePathList:
        html = readTXT(fileP)
        eid = getID(html)
        if eid >0:
            topicID = extractTopic(eid,html)
            if len(topicID)>0:
                insertTopic(eid,topicID,fileP)
            else:
                try:
                    print('empty: '+fileP)
                    updateSQL = 'update dlurl1 set status=2 where id='+str(eid)
                    cur.execute(updateSQL)#标记已抽取
                    conn.commit()
                except Exception:
                    print('error: '+updateSQL)
        else:
            print('eid error: '+fileP)
        #break#只运行一次
    cur.close();conn.close();
    
if __name__ == '__main__':
    mainFunction()
