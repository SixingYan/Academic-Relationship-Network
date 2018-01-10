'''
	抽取可能为空的机构，针对XML
'''
import re
from tool import getCursor,readFiles,getResult,readTXT
from bs4 import BeautifulSoup
files_path = 'E:/Code/Data/institution new1'
conn,cur = getCursor()
#import os;os.chdir('e:/Code/Python');import extractInstitution_xml;extractInstitution_xml.mainFunction()

def extractInstitut(html):
    #
    institution = []
    #找到<strong> Affiliation history
    #它的下一个div
    #里面的每一个a 
    soup = BeautifulSoup(''.join(html),"lxml")
    history = soup.find('affiliation')
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
        selectSQL = 'select t.id from (select id,url from dlurl1 where tem<>5 and userid='+str(userid)+') t where t.url="'+url+'"'
        result = getResult(selectSQL,cur)
        
        if len(result)==1:
            eid = int(result[0]['id'])
        else:
            print('error or exist')
    return eid

def cleanInstit(instit):
    #
    institNew = ''
    for inst in instit.split(' '):
        institNew += re.sub('[^a-zA-Z]','',inst)+' '
    return institNew.strip()
    
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
    try:
        updateSQL = 'update dlurl1 set tem=5 where id='+str(eid)
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
            instit = extractInstitut(html)
            if len(instit)>0:
                insertInstitution(eid,instit,fileP)
            else:
                try:
                    print('empty: '+fileP)
                    updateSQL = 'update dlurl1 set tem=5 where id='+str(eid)
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








































































































