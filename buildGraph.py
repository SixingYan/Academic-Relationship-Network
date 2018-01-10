'''
	构建全图
	基于时间构建不同的全图
	建立合著和同事网络
	net 是二维数组
	graph 是图
'''
import networkx as nx
from tool import constructSeriz,readSeriz,getResult,getResultList,getCursor
import gc
conn,cur = getCursor()

idList_pickle = 'E:/Code/Pickle/idList_pickle.pickle'
expertNet_pickle = 'E:/Code/Pickle/expertNet_pickle.pickle'
paperNetYear_pickle = ''
def buidPaperNet():
	#
	#papernet  id xid yid perid year
	paperNet = readSeriz(expertNet_pickle)#这相当于初始化模板
	paperNetYear = readSeriz(expertNet_pickle)#这相当于初始化模板
	paperGraph=nx.Graph() #初始化一个图
	#预读
	totalSet = []
	for i in range(len(idList)):
		xResult = getResult('select id,time from paper where eid='+str(),cur)
		#需要有个pid list 建立一个字典 key是id,time是内容
		xDict = {}
		for x in xResult:
			xDict[x['id']] = x['time']
		xID = xDict.keys()
		totalSet.append((xDict,xID))
	#预存一次表， 方便用时间这个条件进行控制
	for i in range(len(idList)):
		for j in range((i+1),len(idList)):
			coauthorList = list(set(totalSet[i][1]).intersection(totalSet[j][1]))
			if len(coauthorList) >0:
				#全部插入数据库中
				for cid in coauthorList:
					year = totalSet[i][0][cid]
					insertSQL = 'insert into papernet (xid,yid,perid,year) values('+str(idList[i])+','+str(idList[j])+','+str(cid)+','+str(year)+')'
				paperNet[i][j] = len(coauthorList)
				paperNetYear[i][j] = coauthorList
				paperGraph.add_edge(*(i,j),coauthoryear=coauthorList) #这个增加一条边
	
	constructSeriz(paperNet_pickle,paperNet)
	constructSeriz(paperNetYear_pickle,paperNetYear)
	constructSeriz(paperGraph_pickle,paperGraph)

def buildCoauthorByYear(year):
	#coauthornet xid,yid,score
	paperNetYear = readSeriz(paperNetYear_pickle)
	idList = readSeriz(idList_pickle)
	coauthorNet = readSeriz(expertNet_pickle)#这相当于初始化模板
	'''
	#----------------------------------------
	#---不考虑数据库 start
	coauthorGraph = nx.Graph()
	paperGraph = readSeriz(paperGraph_pickle)
	for tup in paperGraph.edges():
		coauthorList = paperGraph[tup[0]][tup[1]]['coauthoryear']
		coauthorGraph.add_edge(*(tup[0],tup[1]),score=calculateCoauthor(coauthorList,tagYear))
	constructSeriz(coauthorGraph_pickle,coauthorGraph)
	#---不考虑数据库 end
	#----------------------------------------
	'''
	for i in range(len(idList)):
		xid = str(idList[i])
		for j in range((i+1),len(idList)):
			yid = str(idList[j])
			#yearList = getResultList('select year where xid='+xid+', yid='+yid+', year>'+str(year),'year',cur) 
			coauthorList = paperNetYear[i][j]
			#这里就是具体计算了
			if len(yearList)==0:
				continue
			score = calculateCoauthor(coauthorList)
			#xid yid 已经转成str
			insertSQL = 'insert into coauthornet (xid,yid,score) values('+xid+','+yid+','+str(score)+')'
			cur.execute(insertSQL)
			conn.commit()
			coauthorNet[i][j] = score #这里也做一个记录

	coauthorNet_pickle = '' #这个地址要重写，因为需要加入year这个变量
	constructSeriz(coauthorNet_pickle,coauthorNet)

def calculateCoauthor(coauthorList,tagYear):
	#这个地方是计算coauthor的具体算法
	pass

def basePrepare():
	#expertNet xid,yid,status,attr
	#selectSQL = 'select id from dlurl1'
	#idList = getResultList(selectSQL,'id',cur)
	#constructSeriz(idList_pickle,idList)
	#print('read complete!')
	#构建学者的id网
	idList = readSeriz(idList_pickle)
	idLength = len(idList)
	#expertNet = [[0 for j in range(idLength)] for i in range(idLength)] #用于最后的加和
	for i in range(idLength):
		for j in range()
		insertSQL = 'insert into expertNet (xid,yid) values ('+str(idList[i])+','+str(yid)+')'
	#序列化
	constructSeriz(expertNet_pickle,expertNet)
	del expertNet
	gc.collect()	
	print('all complete')

def buildInstitution():
	# colleaguenet xid,yid,score
	'''
	institutionNet = readSeriz(expertNet_pickle)
	institutionGraph = nx.Graph()
	#预读
	totalSet = []
	for i in range(len(idList)):
		#iid还没有设置，应该是status之类的项
		xResult = getResult('select iid from experience1 where eid='+str(idList[i]),cur)
		#需要有个pid list 建立一个字典 key是id,time是内容
		xDict = {}
		for x in xResult:
			xDict[x['id']] = x['time']
		xID = xDict.keys()
		totalSet.append((xDict,xID))
	#这里与paper/coauthor不一样，不用进行一次控制，可以直接放入，类似coauthornet那种
	for i in range(len(idList)):
		for j in range((i+1),len(idList)):
			institutionList = list(set(totalSet[i][1]).intersection(totalSet[j][1]))
			if len(institutionList)>0:
				#全部插入数据库中
				score = calculateColleague(institutionList)
				#xid yid 已经转成str
				insertSQL = 'insert into colleaguenet (xid,yid,score) values('+xid+','+yid+','+str(score)+')'
				cur.execute(insertSQL)
				conn.commit()
				institutionNet[i][j] = score #这里也做一个记录
				institutionGraph(*(i,j),coinstitution=institutionList, weight=score)
	constructSeriz(institutionNet_pickle,institutionNet)
	constructSeriz(institutionGraph_pickle,institutionGraph)
	'''
def calculateColleague(institutionList):
	#具体的计算方法
	pass

def buildFinalGraph():
	#对两个图进行加和
	#coauthorGraph = readSeriz(coauthorGraph_pickle)
	#institutionGraph = readSeriz(institutionGraph_pickle)
	finalLen = len(coauthorNet)
	coauthorNet = readSeriz(coauthorNet_pickle)
	institutionNet = readSeriz(institutionNet_pickle)
	finalNet = readSeriz(expertNet_pickle)  #初始化一个模板
	finalGraph = nx.Graph()

	for i in range(finalLen):
		for j in range((i+1),finalLen):
			if i==j:
				continue
			finalNet[i][j] = coauthorNet[i][j] + institutionNet[i][j]
			finalNet[j][i] = finalNet[i][j]
			if finalNet[i][j] != 0:
				finalGraph.add_edge(*(i,j),score=finalNet[i][j])

	constructSeriz(finalGraph_pickle,finalGraph)

def mainFunction():
	#
	basePrepare()

	buidPaperNet()
	buildCoauthorByYear(2012) #定下大于多少年

	buildInstitution()

def analysisGraph(finalGraph):

	if nx.is_connected(finalGraph):
		print('Yes')
	else:
		number_connected_components(finalGraph)
		nx.connected_component_subgraphs(finalGraph) #这个返回的是什么类型？

def buildTopic():
	# colleaguenet xid,yid,score
	topicNet = readSeriz(expertNet_pickle)
	topicNetMore = readSeriz(expertNet_pickle)
	topicGraph = nx.Graph()
	#预读
	totalSet = []
	for i in range(len(idList)):
		#other还没有设置，相当于tid
		xResult = getResult('select other from topic where eid='+str(idList[i]),cur)
		#需要有个other list 建立一个字典 key是other,num是内容
		xDict = {}
		for x in xResult:
			xDict[x['other']] = x['num']
		xID = xDict.keys()
		totalSet.append((xDict,xID))
	
	for i in range(len(idList)):
		for j in range((i+1),len(idList)):
			topicList = list(set(totalSet[i][1]).intersection(totalSet[j][1]))
			if len(topicList)>0:
				#全部插入数据库中
				score = calculateTopic(topicList)
				#xid yid 已经转成str
				insertSQL = 'insert into topicnet (xid,yid,score) values('+xid+','+yid+','+str(score)+')'
				cur.execute(insertSQL)
				conn.commit()
				topicNet[i][j] = score #这里也做一个记录
				topicGraph(*(i,j),similar=score)
				topicNetMore[i][j] = topicList

	constructSeriz(topicNet_pickle,topicNet)
	constructSeriz(topicGraph_pickle,topicGraph)
	constructSeriz(topicNetMore_pickle,topicNetMore)
	
def calculateTopic():
	pass
if __name__ == '__main__':
	basePrepare()



















