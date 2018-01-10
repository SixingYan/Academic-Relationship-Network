from tool import signal,getHttpUa,httpProxies,headers,readTXT,getCursor,getResult,ChangeOrNot,editeProxies,editeHeader,getPage
import time
import random
from bs4 import BeautifulSoup

#主要的循环方法
def findSubject():
	http,ua = getHttpUa()
    conn,cur = getCursor()
    subList = getResult(sltDLNotCom,cur)#返回二维数组
    for dl in dlList:
        # this is test!!!! read from a txt
        #html = readTXT('E:/Code/Test Data/Paul Robert Barford - ACM author profile page.txt')
        if ChangeOrNot() == True:
            editeProxies(http)
            editeHeader(ua)
        time.sleep(random.randint(1, 20))

        html = str(getPage(dl['subject']))#取出url
        if html != ' ':
            subject = extractSubject(html)
            status = addInfo(conn,cur,subject,dl['id'])
            if status == 1:
                print('Now is '+str(dl['id']))
                
    cur.close()
    conn.close()
    
def extractSubject(doc):
	#
	combineStr = ''
	subject = []
	soup = BeautifulSoup(''.join(doc),"lxml")
	h5 = soup.find(text='Subject Areas')
	td = h5.parent.parent.nextSibling.td
	for a in td.findAll('a'):
		subject.append(a.string)

	combineStr = combineList(subject)
	return combineStr

def addInfo(conn,cur,nl):
    checkSQL = 'select * from dlurl where url=%s'
    number = cur.execute(checkSQL,nl[1])
    if number==0:
        insertSQL = 'insert into dlurl (name,url,status) values (%s,%s,0)'
        insertValues = (nl[0],nl[1])
        try:
            cur.execute(insertSQL,insertValues)
            conn.commit()
        except Exception:
            print('insert error '+str(insertValues))
#定位，获取subject列表

#添加