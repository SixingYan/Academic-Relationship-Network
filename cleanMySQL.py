# -*- coding: utf-8 -*-
from tool import getCursor,getResult,getResultList
#import os;os.chdir('e:/Code/Python');import cleanMySQL;cleanMySQL.preparePublication()

'''
	给出所有的topic表 (把topic中的other作为新的tid放置)
	给出所有的experience1表 (把experience1中的tem作为新的iid放置)
	。。。paper表
	
	topiclist 

	预处理
		考虑去除没有机构的人
		去除没有文章的人
		去除没有研究主题的人
'''

def prepareInstitution():
	#
	conn,cur = getCursor()
	selectSQL = 'select institution,id from experience1'
	institResult = getResult(selectSQL,cur)
	institList = []

	print('get ready')
	for tr in institResult:
		institList.append(tr['institution'])
	institList = list(set(institList))
	print('begin insert')
	for i in range(len(institList)):
		#不要让id自增！！！调整数据库！！！！！
		insertSQL = 'insert into institution (id, instutition) values ('+str(i)+',"'+institList[i]+'")'
		cur.execute(insertSQL)
		conn.commit()
	print('complete1')
	for ir in institResult:
		if ir['institution'] in institList:
			idx = institList.index(ir['institution']) 
		updateSQL = 'update experience1 set tem='+str(idx)+' where id='+str(ir['id'])
		cur.execute(updateSQL)
		conn.commit()
	print('complete2')
	cur.close();conn.close()

def prepareTopic():
	#获得一个单独的topic列表，一个出现两次以上的列表？
	conn,cur = getCursor()
	selectSQL = 'select id,topic,num from topic'
	topicsResult = getResult(selectSQL,cur)
	topicList = []
	print('get ready')
	for tr in topicsResult:
		topicList.append(tr['topic'])
	topicList = list(set(topicList))
	numList = [0 for i in range(len(topicList))]
	print('begin count')
	for tr in topicsResult:
		idx = topicList.index(tr['topic'])
		numList[idx] += int(tr['num'])
	print('begin insert')
	
	for i in range(len(topicList)):
		#这个地方id不要自增,这样可以同时更新topic的tid 调整原数据库！！！！！！！！！！！！！
		#insertSQL = 'insert into topiclist (id,topic,num) values ('+str(i)+',"'+topicList[i]+'",'+str(numList[i])+')'
		if numList[i] >1:
			#忽略只出现一次的topic
			insertSQL = 'insert into topiclist (id,topic,num) values ('+str(i)+',"'+topicList[i]+'",'+str(numList[i])+')'
		cur.execute(insertSQL)
		conn.commit()
	print('complete1')
	for eachTopic in topicsResult:
		idx = -1
		if eachTopic['topic'] in topicList:
			idx = topicList.index(eachTopic['topic'])
			if numList[idx] <2:
				idx = -1 #忽略只出现一次的topic
		updateSQL = 'update topic set other='+str(idx)+' where id='+str(eachTopic['id'])
		cur.execute(updateSQL)
		conn.commit()
	print('complete2')
	cur.close();conn.close()

def preparation():
	#应该做去除冗余后做
	#给出topic paper institution三个列表的并集，给出dlurl1，用dlurl1-三个的并集，得出的就是需要不考虑的
	pass

def cleanTopic():
	# 
	conn,cur = getCursor()
	selectSQL = 'select id from dlurl1'
	eidList = getResultList(selectSQL,'id',cur)

	for eid in eidList: 
		selectSQL = 'select id,topic from topic where eid='+str(eid) 
		idPidList = getResult(selectSQL,cur)
		#获得单独的pid列表
		pidList = []
		for ip in idPidList:
			pidList.append(ip['topic'])
		newPidList = list(set(pidList))
		print('start '+str(eid))
		if not len(newPidList) < len(pidList): #说明不冗余
			print('not dublicate '+str(eid))
			continue
		#两层循环找相同
		passID = []
		sameEid = []
		for i in range(len(idPidList)):
			if i in passID:
				continue
			for j in range((i+1),len(idPidList)):
				if idPidList[i]['topic'] == idPidList[j]['topic']:
					sameEid.append(idPidList[j]['id']) #从id删掉它
					passID.append(j)
		for sid in sameEid:
			deleteSQL = 'delete from topic where id='+str(sid)
			try:
				cur.execute(deleteSQL)
				conn.commit()
				print('complete delete:'+str(sid))
			except Exception:
				print('delete error '+str(sid))
	cur.close;conn.close()

def cleanPaper():
	# 这个已经完成了
	conn,cur = getCursor()
	selectSQL = 'select id from dlurl1'
	eidList = getResultList(selectSQL,'id',cur)

	for eid in eidList: 
		selectSQL = 'select id from paper where eid='+str(eid) 
		idPidList = getResult(selectSQL,cur)
		#获得单独的pid列表
		
		for ip in idPidList:
			pidList.append([ip['id'],False])
		print('start '+str(eid))
		#sameEid = []
		for i in range(len(pidList)):
			#分配paperid
			updateSQL = 'update paper set paperid='+str(i)+' where id='+str(pidList[i][0])
			cur.execute(updateSQL);conn.commit();
			if pidList[i][1]==True:
				continue
			for j in range((i+1),len(pidList)):
				if pidList[j][1]==True:
					continue
				iR = getResult('select doi,time,title from where id='+str(pidList[i][0]),cur)
				jR = getResult('select doi,time,title from where id='+str(pidList[j][0]),cur)
				if iR[0]['doi']==jR[0]['doi']:
					#相同
					pidList[j][1]==True
					updateSQL = 'update paper set paperid='+str(i)+' where id='+str(pidList[j][0])
					cur.execute(updateSQL);conn.commit();
					continue
				elif (iR[0]['time']==jR[0]['time']) and(iR[0]['title']==jR[0]['title']):
					#相同
					pidList[j][1]==True
					updateSQL = 'update paper set paperid='+str(i)+' where id='+str(pidList[j][0])
					cur.execute(updateSQL);conn.commit();
					continue
				else:
					#不相同
					pass
			pidList[i][1]==True
	cur.close;conn.close()
'''
def cleanPaper():
	# 这个已经完成了
	conn,cur = getCursor()
	selectSQL = 'select id from dlurl1'
	eidList = getResultList(selectSQL,'id',cur)

	for eid in eidList: 
		selectSQL = 'select id from paper where eid='+str(eid) 
		idPidList = getResult(selectSQL,cur)
		#获得单独的pid列表
		
		for ip in idPidList:
			pidList.append([ip['id'],False])
			#pidList['doi'] = ip['doi'])
		#newPidList = list(set(pidList))
		print('start '+str(eid))

		#if not len(newPidList) < len(pidList): #说明不冗余
		#	print('not dublicate '+str(eid))
		#	continue
		#两层循环找相同
		#passID = []
		sameEid = []
		for i in range(len(idPidList)):
			if idPidList[i]:
				continue
			for j in range((i+1),len(idPidList)):
				if idPidList[i]['doi'] == idPidList[j]['doi']:
					#检查
					sameEid.append(idPidList[j]['id']) #从id删掉它
					passID.append(j)
		for sid in sameEid:
			deleteSQL = 'delete from paper where id='+str(sid)
			try:
				cur.execute(deleteSQL)
				conn.commit()
				print('complete delete:'+str(sid))
			except Exception:
				print('delete error '+str(sid))
	cur.close;conn.close()
'''


def preparePublication():
	#建立一个publication表 
	conn,cur = getCursor()
	selectSQL = 'select pid from publication'
	paperResult = getResult(selectSQL,cur)
	print('read completed')
	for i in range(len(paperResult)):
		if i<-1:
			continue
		print(str(paperResult[i]['pid']))
		updateSQL = 'update paper set paperid='+str(i)+' where pid="'+paperResult[i]['pid']+'"'
		cur.execute(updateSQL)
		conn.commit()
		print('completed: '+str(i))
	'''
	paperList = []
	print('get ready')
	for tr in paperResult:
		paperList.append(tr['pid'])
	paperList = list(set(paperList))
	print('begin insert')
	for i in range(len(paperList)):
		#不要让id自增！！！调整数据库！！！！！
		insertSQL = 'insert into publication (id, pid) values ('+str(i)+',"'+paperList[i]+'")'
		cur.execute(insertSQL)
		conn.commit()
	print('complete1')

	pidDict = {}
	selectResult = getResult('select id, pid from publication',cur)
	
	for sr in selectResult:
		pidDict[sr['pid']] = sr['id'] 
	for ir in paperResult:
		if ir['pid'] in paperList:
			idx = paperList.index(ir['pid'])
		updateSQL = 'update paper set paperid='+str(pidDict[ir['pid']])+' where id='+str(ir['id'])
		cur.execute(updateSQL)
		conn.commit()
	print('complete2')
	
	'''
	cur.close();conn.close()
'''
def ():
	#

	pass
'''


if __name__ == '__main__':
	preparePublication()

'''
def cleanInstitu():
	# 这个已经完成了 替代mysql完成基本去除冗余
	conn,cur = getCursor()
	selectSQL = 'select id from dlurl1'
	eidList = getResultList(selectSQL,'id',cur)

	for eid in eidList: 
		selectSQL = 'select id,institution from experience1 where eid='+str(eid) 
		idPidList = getResult(selectSQL,cur)
		#获得单独的pid列表
		pidList = []
		for ip in idPidList:
			pidList.append(ip['institution'])
		newPidList = list(set(pidList))

		print('start '+str(eid))

		if not len(newPidList) < len(pidList): #说明不冗余
			print('not dublicate '+str(eid))
			continue
		#两层循环找相同
		passID = []
		sameEid = []
		for i in range(len(idPidList)):
			if i in passID:
				continue
			for j in range((i+1),len(idPidList)):
				if idPidList[i]['institution'] == idPidList[j]['institution']:
					sameEid.append(idPidList[j]['id']) #从id删掉它
					passID.append(j)
		for sid in sameEid:
			deleteSQL = 'delete from experience1 where id='+str(sid)
			try:
				cur.execute(deleteSQL)
				conn.commit()
				print('complete delete:'+str(sid))
			except Exception:
				print('delete error '+str(sid))
	cur.close;conn.close()
'''



'''
Table: name2name
Columns:
id int(11) AI PK 
gid int(11) 
yid int(11) 
tag int(11) 
nameRate float 
flag int(11) 
paper float 
coauthor float 
institu float 
topic float 
status int(11) 
tem int(11)
'''