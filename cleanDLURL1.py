# -*- coding: utf-8 -*-

#处理相似用户，即名字，研究主题topic，合作者的人authors有大部分（3/4）相同
#先不要进行合并，先记录需要合并的，看看情况

'''
	还不能运行，因为tid和iid还没清理
	然后用一个标记位表示是否已经清理

	有一个这个表checkList 必须有两个tag位，一个记录姓名是否相似，一个记录其它记录是否相似
	索引:tag,flag,xid,yid,(xid,yid)
'''
from nltk.corpus import wordnet as wn
import re
from tool import getCursor,getResult,constructSeriz

#threshold = 0 #全局，不适用于姓名比较，随着名字相同的程度而改变
selectSQL = 'select id, name from dlurl1 limit 1000' #测试
conn,cur = getCursor()
nameThreashold = 0.6
dict_path = 'E:/Code/Data/backup clean dlurl/'
#import os;os.chdir('e:/Code/Python');import cleanDLURL1;cleanDLURL1.countTopic()
#import os;os.chdir('e:/Code/Python');import cleanDLURL1;cleanDLURL1.countCoauther()
#import os;os.chdir('e:/Code/Python');import cleanDLURL1;cleanDLURL1.previousprepare()
'''
	从队列A里拿出一个X，再从队列A里拿出一个Y，（1）相同处理，（2）不相同则放到队列B。处理完成后，将X放入B中	
第一个和第二个比较，如果相同，为第一个建立一个索引，把第二个放到第一个对应的数组
	
	其实都得看数量少的那一方

	目前只在姓名上使用threshold
	其它部分先不使用threshold
'''
def previousprepare():
	#updateSQL = 'update dlurl1 set tem=5'
	#cur.execute(updateSQL)
	#conn.commit()
	pairList = getResult('select xid,yid from new_table where xid<453',cur)
	completedID = []
	print('get !')
	for pl in pairList:
		completedID.append(pl['xid'])
		completedID.append(pl['yid'])
		print('completed '+str(pl['xid'])+' '+str(pl['yid']))
	completedID = list(set(completedID))
	print('len:'+str(len(completedID)))
	print('begin update')
	#completedID.sort()
	for cid in completedID:
		updateSQL = 'update dlurl1 set tem=7 where id='+str(cid)
		cur.execute(updateSQL)
		conn.commit()
		print('completed'+str(cid))

def cleanRedunOthers():
	#这里基于名字的去除重复的基础上检查
	'''
	#把检查列表准备好
	selectSQL = 'select distinct eid,group_concat(tem) temg from experience1 where eid in (select xid from new_table where tag=60) or eid in (select yid from new_table where tag=60) group by eid'
	#selectSQL = 'select id,xid,yid from new_table where tag=60' 
	selectResult = getResult(selectSQL,cur)
	institutionDict = {}
	for sr in selectResult:
		institutionDict[sr['eid']] = sr['temg'].split(',')
	'''
	#找出其它相似对
	selectSQL = 'select id,xid,yid from new_table where tag=60 and id>74331 limit 30000'
	xyPair = getResult(selectSQL,cur)
	valueSet = {}
	for xy in xyPair:
		print('now compare id: '+str(xy['id']))
		xID = xy['xid']
		yID = xy['yid']
		#temlist1 = institutionDict[xID]
		#temlist2 = institutionDict[yID]
		#valueSet['institu'] = str(compareInstit(temlist1,temlist2))
		valueSet['institu'] = str(compareInstit(xID,yID))#做机构比较，对于机构insti位为0的对，再比较机构名称是否包含
		valueSet['id'] = str(xy['id'])
		insertMySQLInstitu(valueSet)
		print(str(valueSet['institu']))

def cleanRedun():
	#这里是清理名字相同
	'''
	pairList = getResult('select xid,yid from new_table where xid<453',cur)
	completedID = []
	#print(pairList)
	for pl in pairList:
		completedID.append(pl['xid'])
		completedID.append(pl['yid'])
	#print(completedID)
	completedID = list(set(completedID))
	#print(completedID)
	completedID.sort()
	#print(completedID)
	'''
	'''
		import os;os.chdir('e:/Code/Python');import cleanDLURL1;cleanDLURL1.cleanRedun()
		cong 666, cong 1000000, cong 100000, cong 2000000, cong 500000, cong 1500000,cong 150000, cong 1700000 cong 5000
	'''
	idList = getResult('select id,name from dlurl1 where tem<>7 and id>1700000',cur)
	print('read ready')
	idPairList = []
	#print(idList)

	for idl in idList:
		#if not idl['id'] in completedID: #这个比对太慢了
		idPairList.append([idl['id'],idl['name'],False])
		#idPairList.append([idl['id'],False])
		# idPairList[X][0] = id, idPairList[X][1] = True or False 原来是看是否被检查过，现在这个项应该已经没用了，不过先保留着
	print('ready')
	for i in range((len(idPairList)-1)):
		#开始检查
		if idPairList[i][2] ==True:
			continue
		if i%100==0:
			print('now is '+str(i))
		for j in range((i+1),len(idPairList)):
			if idPairList[j][2] ==True:
				continue
			if False:
				pass
			else:
				valueSet={}
				#--------------------------------------------------------
				valueSet['nameThreashold'] = str(nameThreashold)
				nameRate = nameSameOrNot(idPairList[i][1],idPairList[j][1])
				valueSet['xid'] = str(idPairList[i][0])
				valueSet['yid'] = str(idPairList[j][0])
				if nameRate==0:
					print('pass: '+valueSet['xid']+' '+valueSet['yid'])
					continue
				else:
					valueSet['flag'] = str(1)
					idPairList[j][2] = True
					updateSQL = 'update dlurl1 set tem=7 where id='+str(idPairList[j][0])
					cur.execute(updateSQL)
					conn.commit()
				valueSet['tag'] = str(int(nameThreashold*100)) #保证一定是整数 放置名字相似度的取舍，用于放置索引
				valueSet['nameRate']= str(nameRate)
				insertMySQLName(valueSet)
				#pickle_path = dict_path+valueSet['xid']+'_'+valueSet['yid']+'_'+valueSet['tag']+'_'+valueSet['flag']+'.pickle'
				#--------------------------------------------------------
				#sql = 'select xid,yid from new_table where tag=60 and flag=1'
				#valueSet['paper']= str(valueSet['paper'])
				#valueSet['coauthor']= str(valueSet['coauthor'])

				'''
				valueSet = compareSame(idPairList[i][0],idPairList[j][0])
				
				if len(valueSet)<1:
					#名字完全不相同
					flag = 0
					valueSet['flag'] = str(0)
					valueSet['nameRate']= str(0)
				else:
					flag = 1
					valueSet['flag'] = str(1)
					valueSet['nameRate']= str(valueSet['nameRate']) #姓名的相似率

				valueSet['xid'] = str(idPairList[i][0]) 
				valueSet['yid'] = str(idPairList[j][0])
				valueSet['tag'] = str(nameThreashold*100) #保证一定是整数 放置名字相似度的取舍，用于放置索引
				valueSet['nameThreashold'] = str(nameThreashold)
				
				if flag==0:
					valueSet['flag'] = str(0)
					insertMySQLDiffer(valueSet)
					constructSeriz(pickle_path,valueSet)
					continue #直接进入下一个循环
				#字符化 这里的处理思想是，保留对比结果，最后根据对比结果来考虑怎么合并
				valueSet['paper']= str(valueSet['paper'])
				valueSet['coauthor']= str(valueSet['coauthor'])
				valueSet['institu']= str(valueSet['institu'])
				valueSet['topic']= str(valueSet['topic'])
				insertMySQL(valueSet)
				'''
				#constructSeriz(pickle_path,valueSet)
		updateSQL = 'update dlurl1 set tem=7 where id='+str(idPairList[i][0])
		cur.execute(updateSQL)
		conn.commit()
		'''
		selectSQL = 'select distinct eid,group_concat(tem) temg from experience1 where eid in (select xid from new_table where tag=60) or eid in (select yid from new_table where tag=60) group by eid'
		#selectSQL = 'select id,xid,yid from new_table where tag=60'
		selectResult = getResult(selectSQL,cur)
		print('total is'+str(len(selectResult)))
		valueSet = {}
		valueSet['tag'] = str(60)
		for i in range(len(selectResult)-1):
			temlist1 = selectResult[i]['temg'].split(',')
			valueSet['xid'] = selectResult[i]['eid']
			for j in range((i+1),len(selectResult)):
				temlist2 = selectResult[j]['temg'].split(',')
				valueSet['institu'] = str(compareInstit(temlist1,temlist2))
				valueSet['yid'] = selectResult[j]['eid']
				insertMySQLInstitu(valueSet)

			#valueSet = {}
			#valueSet['paper'] = comparePaper(sr['xid'],sr['yid'])
			#valueSet['id'] = sr['id']
			#valueSet['coauthor'] = compareCoauthor(sr['xid'],sr['yid'])
			
		'''

def countInstitu():
	selectResult = getResult('select id,gid,yid from name2name',cur)
	print('read completed')
	for sr in selectResult:
		if sr['gid'] == sr['yid']:
			continue
		value = compareInstit(sr['gid'],sr['yid'])
		insertSQL = 'insert into name2compare (xid,yid,nameRate,tag,institu) values ('+str(sr['gid'])+','+str(sr['yid'])+',1.0,100,'+str(value)+')'
		#print(insertSQL)
		cur.execute(insertSQL)
		conn.commit()
		print('completed: '+str(sr['id']))
		#break

def countInstituXXXX():
	selectResult = getResult('select id,gid,yid from name2name',cur)
	print('read completed')
	for sr in selectResult:
		if sr['gid'] == sr['yid']:
			continue
		value = compareInstit(sr['gid'],sr['yid'])
		insertSQL = 'insert into name2compare (xid,yid,nameRate,tag,institu) values ('+str(sr['gid'])+','+str(sr['yid'])+',1.0,100,'+str(value)+')'
		#print(insertSQL)
		cur.execute(insertSQL)
		conn.commit()
		print('completed: '+str(sr['id']))
		#break

def countInstitu2():
	selectResult = getResult('select id,xid,yid,nameRate from namecompare',cur)
	print('read completed')

	for sR in selectResult:
		xid = str(sR['xid'])
		yid = str(sR['yid'])
		nameRate = str(sR['nameRate'])
		tag = str(int(float(nameRate)*100))
		xResult = getResult('select id,yid from name2name where gid='+xid,cur)
		yResult = getResult('select id,yid from name2name where gid='+yid,cur)
		for xR in xResult:
			for yR in yResult:
				value = compareInstit(xR['yid'],yR['yid'])
				insertSQL = 'insert into name2compare (xid,yid,nameRate,tag,institu) values ('+str(xR['yid'])+','+str(yR['yid'])+','+nameRate+','+tag+','+str(value)+')'
				#print(insertSQL)
				cur.execute(insertSQL)
				conn.commit()
				print('completed: '+str(xR['yid'])+' '+str(yR['yid']))
		print('completedID: '+str(sR['id']))
		#break

def countPaper():
	#selectResult = getResult('select id,xid,yid from name2compare where institu=-1 or (institu>0.1 and institu<0.8)',cur)
	selectResult = getResult('select id,xid,yid from name2compare where institu>-1 and institu<0.1',cur)
	
	print('read completed')
	total = len(selectResult)
	i = 0
	for sR in selectResult:
		i+=1
		xid = str(sR['xid'])
		yid = str(sR['yid'])
		id = str(sR['id'])
		value = comparePaper(xid,yid)
		updateSQL = 'update name2compare set paper='+str(value)+' where id='+id 
		print(updateSQL)
		cur.execute(updateSQL)
		conn.commit()	
		print('completed: '+str(round(i/total,3))+' ||  id:'+id)

def countCoauther():
	#selectResult = getResult('select id,xid,yid from name2compare where paper=0 and id>946697',cur)
	#selectResult = getResult('select id,xid,yid from name2compare where institu>-1 and institu<0.1 and paper>0.1 and paper<0.2',cur)
	selectResult = getResult('select id,xid,yid from name2compare where institu>-1 and institu<0.1 and paper>=0 and paper<0.1 and id>1066227',cur)
	
	print('read completed')
	total = len(selectResult)
	i = 0
	for sR in selectResult:
		i+=1
		xid = str(sR['xid'])
		yid = str(sR['yid'])
		id = str(sR['id'])
		value = compareCoauthor(xid,yid)
		updateSQL = 'update name2compare set coauthor='+str(value)+' where id='+id 
		print(updateSQL)
		cur.execute(updateSQL)
		conn.commit()	
		print('completed: '+str(round(i/total,3))+' ||  id:'+id)

def countTopic():
	#selectResult = getResult('select id,xid,yid from name2compare where paper=0 and id>946697',cur)
	selectResult = getResult('select id,xid,yid from name2compare where paper<=0',cur)
	print('read completed')
	total = len(selectResult)
	i = 0
	for sR in selectResult:
		i+=1
		xid = str(sR['xid'])
		yid = str(sR['yid'])
		id = str(sR['id'])
		value = compareTopic(xid,yid)
		updateSQL = 'update name2compare set topic='+str(value)+' where id='+id 
		print(updateSQL)
		cur.execute(updateSQL)
		conn.commit()	
		print('completed: '+str(round(i/total,3))+' ||  id:'+id)

def compareLen(list1,list2):
	# 返回长短
	if len(list1) <= len(list2):
		shortList = list1[:]
		longList = list2[:]
	else:
		shortList = list2[:]
		longList = list1[:]
	return shortList,longList

def compareSame(xID,yID):
	# 考虑了名字完全相同，名字相像问题
	#当姓名几乎不一样的时候，返回一个空的字典
	#当姓名很相似的时候，进行各种对比
	xName = getResult('select name from dlurl1 where id='+str(xID),cur)[0]['name']
	yName = getResult('select name from dlurl1 where id='+str(yID),cur)[0]['name']
	#flag = False
	'''
		应该给出一个N*N矩阵，每个格有一个4位的向量
		对应数据库中的就是
		id xid yid nameThreashold(0-1) nameRate(0-1) threshold(0-1) paper coauthor institu topic 后四个都用0、1表示
	'''
	valueSet = {}
	
	#应该是如果名字完全相同，则匹配率稍低，不然则适当升高
	nameSameRate = nameSameOrNot(xName, yName)
	if nameSameRate == 0:
		return valueSet #直接返回
	#开始比较
	valueSet['nameRate'] = nameSameRate
	valueSet['paper'] = comparePaper(xID,yID)
	valueSet['coauthor'] = compareCoauthor(xID,yID)
	valueSet['institu'] = compareInstit(xID,yID)
	valueSet['topic'] = compareTopic(xID,yID)
	return valueSet

def insertMySQL(valueSet):
	#
	insertSQL = 'insert into (tag,xid,yid,nameThreashold,nameRate,paper,coauthor,institu,topic,flag) values('+valueSet['tag']+','+valueSet['xid']+','+valueSet['yid']+','+valueSet['nameThreashold']+','+valueSet['nameRate']+','+valueSet['paper']+','+valueSet['coauthor']+','+valueSet['institu']+','+valueSet['topic']+','+valueSet['flag']+')'
	try:
		cur.execute(insertSQL)
		conn.commit()
		print('complete: '+valueSet['xid'])
	except Exception:
		print()

def insertMySQLDiffer(valueSet):
	#
	insertSQL = 'insert into (tag,xid,yid,nameThreashold,nameRate,flag) values('+valueSet['tag']+','+valueSet['xid']+','+valueSet['yid']+','+valueSet['nameThreashold']+','+valueSet['nameRate']+','+valueSet['flag']+')'
	try:
		cur.execute(insertSQL)
		conn.commit()
		print('complete: '+valueSet['xid'])
	except Exception:
		print()

def insertMySQLPaper(valueSet):
	#
	updateSQL = 'update new_table set paper='+valueSet['paper']+', coauthor='+valueSet['coauthor']+' where id='+valueSet['id']
	try:
		cur.execute(updateSQL)
		conn.commit()
		print('complete: '+valueSet['xid']+' '+valueSet['yid'])
	except Exception:
		print('update error '+updateSQL)

def insertMySQLInstitu(valueSet):
	#
	updateSQL = 'update new_table set institu='+valueSet['institu']+' where id='+valueSet['id']
	try:
		cur.execute(updateSQL)
		conn.commit()
		print('complete: '+valueSet['id'])
	except Exception:
		print('update error '+updateSQL)

def insertMySQLName(valueSet):
	#
	insertSQL = 'insert into new_table(tag,xid,yid,nameThreashold,nameRate,flag) values('+valueSet['tag']+','+valueSet['xid']+','+valueSet['yid']+','+valueSet['nameThreashold']+','+valueSet['nameRate']+','+valueSet['flag']+')'
	try:
		cur.execute(insertSQL)
		conn.commit()
		print('complete: '+valueSet['xid']+' '+valueSet['yid'])
	except Exception:
		print('insert error '+insertSQL)
def cheakAmount(list1,list2): #only for check name 测试一下2个单位和3个单位的名字的值
	# if similar (less than threshold) then return True
	# a string as treated as list
	flag = True
	theShort,theLong = compareLen(list1,list2)
	if len(theShort) < (len(theLong)*nameThreashold):
		flag = False
	return flag

def nameSameOrNot(name1,name2):
	#比较名字相似度，低于阈值的姓名直接返回0
	#分出长短
	#name1 = getResult('select name from dlurl1 where id='+str(xID),cur)[0]['name']
	#name2 = getResult('select name from dlurl1 where id='+str(yID),cur)[0]['name']
	name1 = name1.strip()
	name2 = name2.strip()
	if not cheakAmount(name1, name2): #数量过于不相似 
		#直接返回
		return 0
	theShort,theLong = compareLen(name1, name2)

	if (theShort in theLong) or (theShort==theLong):# 包含关系
		return 0
	else:
		theShortNames = re.split(',|\.| ',theShort) #多个分隔符
		theLongNames = re.split(',|\.| ',theLong) #多个分隔符
		s = 0
		#t = 0
		for sn in theShortNames:
			for ln in theLongNames:
				if sn in ln:
					s += 1
	
	if s/len(theShortNames) > nameThreashold:
		return s/len(theShortNames)
	else:
		return 0

def comparePaper(xID,yID):
	# 对比paper 的year和title，比较相同率 sameRatio
	xCheckSQL = 'select paperid from paper where eid='+str(xID)
	yCheckSQL = 'select paperid from paper where eid='+str(yID)
	xResult = getResult(xCheckSQL,cur)
	yResult = getResult(yCheckSQL,cur)
	if len(xResult) * len(yResult) == 0:
		return -1
	sResult,lResult = compareLen(xResult,yResult)
	single = 0
	for sR in sResult:
		for lR in lResult:
			flag = False
			if sR['paperid'] == lR['paperid']:
				#基本相同了
				flag = True
			if flag:
				single += 1
				break #不用往下比较了
	return round(single/len(sResult),4)

def comparePaper2(xID,yID):
	# 对比paper 的year和title，比较相同率 sameRatio
	xCheckSQL = 'select title,doi,time from paper where eid='+str(xID)
	yCheckSQL = 'select title,doi,time from paper where eid='+str(yID)
	xResult = getResult(xCheckSQL,cur)
	yResult = getResult(yCheckSQL,cur)
	if len(xResult) * len(yResult) == 0:
		return -1
	sResult,lResult = compareLen(xResult,yResult)
	single = 0
	for sR in sResult:
		for lR in lResult:

			flag = False
			if sR['doi'].strip() == lR['doi'].strip():
				#基本相同了
				flag = True
			elif (int(sR['time']) == int(lR['time'])) and sR['title']==['title']:
				#相同
				flag = True
			else:
				pass

			if flag:
				single += 1
				break #不用往下比较了
	return single/len(sResult)

def compareCoauthor(xID,yID):
	#
	xCheckSQL = 'select eid from paper where paperid in (select paperid from paper where eid='+str(xID)+')'
	yCheckSQL = 'select eid from paper where paperid in (select paperid from paper where eid='+str(yID)+')'
	xResult = getResult(xCheckSQL,cur)
	yResult = getResult(yCheckSQL,cur)
	if len(xResult) * len(yResult) == 0:
		return -1
	sResult,lResult = compareLen(xResult,yResult)
	single = 0

	for sR in sResult:
		if (sR['eid'] == xID) or (sR['eid'] == yID): #跳过自己
			continue
		for lR in lResult:
			if (lR['eid'] == xID) or (lR['eid'] == yID):
				continue
			flag = False
			if sR['eid'] == lR['eid']:
				#相同了
				flag = True

			if flag:
				single += 1
				break #不用往下比较了

	return round(single/len(sResult),4)

def compareInstit(xID,yID):
	#
	xCheckSQL = 'select institution from experience1 where eid='+str(xID)
	yCheckSQL = 'select institution from experience1 where eid='+str(yID)
	xResult = getResult(xCheckSQL,cur)
	yResult = getResult(yCheckSQL,cur)
	if len(xResult) * len(yResult) == 0:
		return -1
	sResult,lResult = compareLen(xResult,yResult)
	single = 0

	for sR in sResult:
		for lR in lResult:
			flag = False
			shortI,longI = compareLen(sR['institution'],lR['institution'])
			if shortI in longI:
				#相同了
				flag = True
			if flag:
				single += 1
				break #不用往下比较了
	return single/len(lResult)

def compareInstit2(xID,yID):
	#需要返回-1区别是否是有一个为空
	xCheckSQL = 'select tem from experience1 where eid='+str(xID)
	yCheckSQL = 'select tem from experience1 where eid='+str(yID)
	instList1 = getResult(xCheckSQL,cur)
	instList2 = getResult(yCheckSQL,cur)
	if len(instList1) * len(instList2) == 0:
		return -1
	sResult,lResult = compareLen(instList1,instList2)
	single = 0
	for sR in sResult:
		for lR in lResult:
			flag = False
			if sR == lR:
				#相同了
				flag = True
			if flag:
				single += 1
				break #不用往下比较了

	return single/len(sResult)

def compareTopic(xID,yID):
	xCheckSQL = 'select topic from topic where eid='+str(xID)
	yCheckSQL = 'select topic from topic where eid='+str(yID)
	xResult = getResult(xCheckSQL,cur)
	yResult = getResult(yCheckSQL,cur)
	if len(xResult) * len(yResult) == 0:
		return -1
	sResult,lResult = compareLen(xResult,yResult)
	single = 0

	for sR in sResult:
		for lR in lResult:
			flag = False
			sTopic,lTopic = compareLen(sR['topic'],lR['topic'])
			if sTopic in lTopic:
				#包含关系也算相同了
				flag = True
			if flag:
				single += 1
				break #不用往下比较了

	return single/len(sResult)

def deleteNullAll():
	#在所有记录合并后，只保留全有的记录

	#先做一个复制 dlurl 然后从里面删除
	eidInstitList = 'select distinct eid from experience1'
	eidPaperList = 'select distinct eid from paper'
	eidTopicList = 'select distinct eid from topic'

	for rs in selectResult:
		if not rs['id'] in eidInstitList:	
			pass
		elif not rs['id'] in eidPaperList:
			pass
		elif not rs['id'] in eidTopicList:
			pass
		else:
			continue
		deleteSQL = 'delete from dlurl where id='+str(rs['id'])
		cur.execute(deleteSQL)
		conn.commit()		

def cleanTopic():
    #
    #tokenizer = nltk.RegexpTokenizer(r'w+')
    selectResult = getResult('select id,topic from topic',cur)
    print('read completed')
    i = 0
    total= len(selectResult)
    for sr in selectResult:
        i+=1
        topic = sr['topic']
        #topic = 'get and got'
        newTopic = ''
        for t in topic.split():
        	nt = wn.morphy(t)
        	try:
        		newTopic += (nt+' ')
        	except Exception:
        		newTopic += (t+' ')	
        #print(newTopic)
        #topic = tokenizer.tokenize(topic)
        #topic = nltk.PorterStemmer().stem(topic)
        updateSQL = 'update topic set topic="'+newTopic.strip()+'" where id='+str(sr['id'])
        print(updateSQL)
        cur.execute(updateSQL)
        conn.commit()
        print('completed: '+str(round(i/total,3))+' ||  id:'+str(sr['id']))
        
if __name__ == '__main__':
	cleanRedun()

























