# -*- coding: utf-8 -*-
import os
from tool import getCursor,writeList,isTheSame,readTXT,getResult,writeTXT,cleanName

from bs4 import BeautifulSoup
#files_path = 'E:/Code/Data/html_S2'
files_path = 'E:/Users/Sixing/dl1/dilibrary'
errorcsv_path = 'E:/Code/Data/Error/insertError_1.29.csv'
#import os;os.chdir('e:/Code/Python');import extractDL_analysis;extractDL_analysis.mainFunction()
SQLerror_path = 'E:/Code/Data/SQL_error.csv'
Dir_path = 'E:/Code/Data/Error/'
csvError_path = 'csvErrorHtml_'
collError_path = 'collErrorHtml_'
advError_path = 'advErrorHtml_'
subjError_path = 'subjErrorHtml_'

conn,cur = getCursor()

def analysisPage(doc,id):
    #
    infoSet = {}
    soup = BeautifulSoup(''.join(doc),"lxml")

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

    return infoSet

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

def addInfo(infoSet,dl):
    #
    #conn,cur = getCursor()#获取数据库连接和游标
    #根据不同情况构造插入语句，使用拼接的方法
    if type(infoSet) != dict:
        print('error')
    if len(infoSet['advUrl']) >10 :
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
        #print('here1')
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
    insertSQL = 'insert into expert (name,homepage,dl) values ("'+comName+'","'+infoSet['homepage']+'","'+dl['url']+'")'

    try:
        cur.execute(insertSQL)
        conn.commit()
        #print('here2')
    except Exception:
        print('insert error '+'id: '+str(dl['id']))

    #增加机构
    insertSQL = ''
    result = getResult('select * from expert order by eid desc limit 1',cur)
    eid = result[0]['eid']
    #print('here4')
    if len(infoSet['institution'])>0:
        for inst in infoSet['institution']:
            try:
                insertSQL = 'insert into experience (eid,institution) values('+str(eid)+', "'+inst+'")'
                cur.execute(insertSQL)
                conn.commit()
            except Exception:
                print('error:'+insertSQL)
    #print('here5')
    cur.execute('update dlurl1 set status=1 where id='+str(dl['id']))#标记已抽取
    conn.commit()
    #print('here3')
    print('Completed '+str(dl['id']))

def dlInfo(html):
    dl = {}
    indx = '<![CDATA['
    start = html.find(indx)
    end = html.find(']]></fullpath>')
    if start>0:
        url = html[(start+len(indx)):end]        
        url,userid = extractUserID(url)
        #useridSQL = 'select id,url,name from dlurl1 where status=0 and userid='+str(userid)
        useridSQL = 'select id,url,name from academic.dlurl1 where status=0 and url="'+url+'"'
        try:
            number = cur.execute(useridSQL)
            #print(type(number))
        #print('numberhere:'+str(number))
            if number>0:
                for c in cur.fetchall():
                    if c['url'] == url:
                        dl['id'] = int(c['id'])
                        dl['name'] = c['name']
                        dl['url'] = c['url']  
                        break
            else:
                print('exist!')
        except Exception:
            print('error______:'+useridSQL)
            
    return dl

#抽取个人信息
def personInfo(soup):
    #
    #情况：为空/Add personal information/只有homepage/只有邮箱/两者都有
    name = ' '
    homepage = ' '
    email = ' '
    
    ho = soup.find('homepage')
    name = ho.strong.string
    try:
        if ho.a != None and ho.a.string=='homepage' and len(ho.a['href'])>10:
            homepage = ho.a['href']
        if ho.div != None:
            div = ho.div.contents
            div[0].replace('\xa0','')
            email = div[0].strip()+'@'+div[2].strip()
        #print('yes1')
    except Exception:
        print('error:'+str(name)) 
        print('error:'+str(ho.a))
        print('error:'+str(ho.div)) 
    return name,homepage,email
    
#抽取affiliation history
def historyInfo(soup):
    #
    institution = []
    #print('yes00')
    his = soup.find('history')
    #div = his.find(text='Affiliation history').parent.nextSibling.nextSibling
    #print('yes000')
    div = his.find(text='Affiliation history')
    if div !=None:
        div = div.parent.nextSibling
    else:
        return institution
    #print('yes2')
    while div.name != 'div':
        div = div.nextSibling
        if div == None:
            break
    #print('yes4')
    try:    
        if div.findAll('a') != None:
            for a in div.findAll('a'):
                institution.append(a.string)
        #print('yes3')
    except Exception:
        print('error:'+str(div))     
    return institution

#获取csv地址
def getCsvUrl(soup,doc,id):
    #
    url = ' '
    try:
        csv = soup.find('csv')
        url = csv.a['href']
    except Exception:
        print('error:'+str(csv))
        path = Dir_path+csvError_path+str(id)+'.txt'
        writeTXT(path,doc)
    return url

#判断role里面是否含有advisor
def getAdvisor(soup,doc,id):
    #
    url = ' '
    try:
        left = soup.find('left')
        Advisor = left.find('Advisor only')
        if Advisor != None:
            url = Advisor.parent['href']
    except Exception:
        print('error:'+str(Advisor))
        path = Dir_path+advError_path+str(id)+'.txt'
        writeTXT(path,doc)
    return url

#获取 colleagues 的地址
def getColleagues(soup,doc,id):
    #
    url = ' '
    try:
        left = soup.find('left')
        colleagues = left.find('See all colleagues of this author')
        if colleagues != None:
            url = colleagues.parent['href']
    except Exception:
        print('error:'+str(colleagues))
        path = Dir_path+collError_path+str(id)+'.txt'
        writeTXT(path,doc)
    return url

#获取 subject area的地址
def getSubject(soup,doc,id):
    #
    url = ' '
    try:
        left = soup.find('left')
        subject = left.find('See all subject areas')
        if subject != None:
            url = subject.parent['href']
    except Exception:
        print('error:'+str(subject))
        path = Dir_path+subjError_path+str(id)+'.txt'
        writeTXT(path,doc)
    return url   

def extractUserID(url):
    url = url.split('&')[0]
    userid = url[:]
    userid = userid.replace('http://dl.acm.org/author_page.cfm?id=','')
    userid = userid[4:]#only numbers begin at 4 are considered     
    return url,int(userid)

def mainFunction():
    #
    filePathList = readFiles()
    
    #从列表中读取文件
    for i in range(len(filePathList)):#用下标的形式，方便控制数量
        html = readTXT(filePathList[i])
        dl = dlInfo(html)#查重
        try:
            
            if len(dl)>1:
                #print('ok0')
                infoSet = analysisPage(html,dl['id'])
                #print('ok')
                addInfo(infoSet,dl)
                #print('ok1')
            #if i>5:
                #break
        except Exception:
            print('analysis error:'+str(dl['id']))    
    cur.close();conn.close()
if __name__ == '__main__':
    #
    filePathList = readFiles()
    
    #从列表中读取文件
    for i in range(len(filePathList)):#用下标的形式，方便控制数量
        html = readTXT(filePathList[i])
        dl = dlInfo(html)#查重
        try:
            
            if len(dl)>1:
            
                infoSet = analysisPage(html,dl['id'])
                #print('ok')
                addInfo(infoSet,dl)
                #print('ok1')
            #if i>5:
                #break
        except Exception:
            print('analysis error:'+str(dl['id']))    
    cur.close();conn.close()

        




















