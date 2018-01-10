
from tool import getResult,getCursor,readTXT
from bs4 import BeautifulSoup
import os
files_path = 'D:/subarea'
#files_path = 'E:/Users/Sixi/g/dl//test'
conn,cur = getCursor()
#import os;os.chdir('e:/Code/Python');import analysisTopic;analysisTopic.mainFunction()

def readFiles(files_path):
    #
    filePathList = []
    for fileName in os.listdir(files_path):
        if len(fileName)>1:
            newFilePath = files_path+'/'+fileName
            filePathList.append(newFilePath)
    return filePathList
    
def extractUserID(url):
    url = url.split('&')[0]
    urlid = url[:]
    id = urlid.replace('http://dl.acm.org/author_page.cfm?id=','')
    userid = id[4:]#only numbers begin at 4 are considered     
    return urlid,userid
    
def extractTopic(html):
	#
	soup = BeautifulSoup(''.join(html),"lxml")
	topic = soup.find('sub')
	topics = []
	for a in topic.findAll('a'):
		try:
			title = a['title']
			parts = title.split('(')
			num = int(parts[-1].replace(')',''))
			topics.append([parts[0].strip(),num])
		except Exception:
			print('a topic analysis failed: '+parts[0].strip())
	return topics

def getID(html):
    #
    eid = -1 #初始化
    indx = '<![CDATA['
    start = html.find(indx)
    end = html.find(']]></fullpath>')
    if start>0:
        subjectURL = html[(start+len(indx)):end]        
        #url,userid = extractUserID(subjectURL)#从网址中分离出url地址
        #回查数据库
        #print('find 1')
        #selectSQL = 'select tem.id from (select id,url from dlurl1 where status<>2 and userid='+str(userid)+') tem where tem.url="'+url+'"'
        selectSQL = 'select tem.id from (select id,subject from dlurl1 where status<>2) tem where tem.subject="'+subjectURL+'"'
        result = getResult(selectSQL,cur)
        eid = []
        for r in result:
        	eid.append(r['id'])
        print('len len is '+str(len(eid)))
    return eid

def insertTopic(eid,topics,fileP):
	#
	for topic in topics:
		try:
			insertSQL = 'insert into topic (topic,eid,num) values ("'+topic[0]+'", '+str(eid)+', '+str(topic[1])+')'
			cur.execute(insertSQL)
			conn.commit()
			#print('do here2')
		except Exception:
			print('insertSQL error: '+insertSQL)
	
	updateSQL = 'update dlurl1 set status=2 where id='+str(eid)
	try:
		cur.execute(updateSQL)
		#print('do here3')
		conn.commit()
	except Exception:
		print('update dlurl1 failed: '+str(eid))

	#print('do here4')
	print('Complete: '+str(eid)+' file: '+fileP)

def mainFunction():
	#
	#读取文件
	filePathList = readFiles(files_path)
	print('read is ready len is '+str(len(filePathList)))
	for fileP in filePathList:
		html = readTXT(fileP)
		#print('do here')
		eid = getID(html)
		#print('do here0')
		if len(eid)>0:
			for e in eid:
				topics = extractTopic(html)
				if len(topics)>0:
					#print('do here1')
					insertTopic(e,topics,fileP)
				else:
					updateSQL = 'update dlurl1 set status=2 where id='+str(e)
					cur.execute(updateSQL)
					conn.commit()	
		#break#只运行一次
	#cur.close();conn.close();

if __name__ == '__main__':
	mainFunction()
	
