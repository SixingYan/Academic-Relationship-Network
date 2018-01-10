'''
	构建COI网
'''
M = float("inf")
YEAR = '2010'
MAX_TIME = 7

from tool import readSeriz,constructSeriz,getResultList,getCursor
import numpy as np
conn,cur = getCursor()
dictPath = 'E:/Code/experience/pickle/'
totalEID_pickle = dictPath + 'totalEID.pickle'
personDict_pickle = dictPath + 'personDict.pickle'
paperAuthorDict_pickle = dictPath + 'paperAuthorDict.pickle'
paperDict_pickle = dictPath + 'paperDict.pickle'
reviewerDict_pickle = dictPath + 'reviewerDict.pickle'
reviewer_ID_pickle = dictPath + 'reviewer_ID.pickle'
distanceMetrx_pickle = dictPath + 'distanceMetrx.pickle'
shortDistanceMetrx_pickle = dictPath + 'shortDistanceMetrx.pickle'
COI_pickle = dictPath + 'COI.pickle'
totalEID_new_pickle = dictPath + 'totalEID_new.pickle'
#import os;os.chdir('e:/Code/Python');import geng_coi;geng_coi.selectEID()

def cleanName(name):
    newName = ''
    for n in name.split(' '):
        if len(n)==0:
            continue
        else:
            newName = newName +n.strip()+' '
    return newName.strip()
def indexEID():
	# 根据author的institute对应dlurl1里面的id
	paperDict = readSeriz(paperDict_pickle)
	reviewerDict = readSeriz(reviewerDict_pickle)
	reviewer_ID = {}
	totalEID = []
	personDict = {} #存放人
	paperAuthorDict = {}
	#paper 部分 ------------
	#if False:
	for title in paperDict.keys():
		#title = ''
		authorsInstitu = paperDict[title]
		#print(authorsInstitu)
		paperAuthorDict[title] = []
		for ai in authorsInstitu:
			name = ai[0]
			
			if name in personDict.keys():
				continue
			Institu = ai[1]
			eidList = getResultList('select distinct eid from experience1 where eid in (select id from dlurl1 where name like "'+cleanName(name)+'") and institution like "'+Institu+'" limit 1','eid',cur)
			if len(eidList)>0:
				personDict[name] = eidList[0]
				totalEID += eidList
				paperAuthorDict[title].append(name)
				print('comppleted: '+name)
			else:
				eidList = getResultList('select id from dlurl1 where name like "'+cleanName(name)+'" limit 1','id',cur)
				if len(eidList)>0:
					personDict[name] = eidList[0]
					totalEID += eidList
					print('comppleted: '+name)
					paperAuthorDict[title].append(name)
				else:
					print('error!!!!!!1  '+name);
		print('-----------completed:'+title)
		#break
	totalEID = list(set(totalEID));constructSeriz(totalEID_pickle,totalEID);
	constructSeriz(personDict_pickle,personDict)
	constructSeriz(paperAuthorDict_pickle,paperAuthorDict);print('completed Paper')

	#reviewer部分
	for name in reviewerDict.keys():
		#print(reviewerDict[name])
		Institu = reviewerDict[name][0]
		eidList = getResultList('select distinct eid from experience1 where eid in (select id from dlurl1 where name like "'+cleanName(name)+'") and institution like "'+Institu+'" limit 1','eid',cur)
		if len(eidList)>0:
			reviewer_ID[name] = eidList[0]
			totalEID += eidList
			print('comppleted: '+name)
		else:
			eidList = getResultList('select id from dlurl1 where name like "'+cleanName(name)+'" limit 1','id',cur)
			if len(eidList)>0:
				reviewer_ID[name] = eidList[0]
				totalEID += eidList
				print('comppleted: '+name)
			else:
				print('error!!!!!!1  '+name);reviewer_ID[name] = -1
		#break

	constructSeriz(totalEID_pickle,totalEID)
	constructSeriz(reviewer_ID_pickle,reviewer_ID)
	

def selectEID():
	#authours X reviewers 根据eid 找到其合作者
	totalEID = readSeriz(totalEID_new_pickle)
	'''
	Dict = readSeriz(reviewer_ID_pickle)
	#Dict = readSeriz(paperAuthorDict_pickle) #这里包有[]
	totalEID = list(set(totalEID))
	print('begin totleEID len is '+str(len(totalEID)))
	for name in Dict.keys():
		eid = Dict[name]
		if eid ==-1:
			continue
		selectEIDList1 = []
		selectEIDList2 = [eid] #下一次要查询的列表
		selectEIDCompleted = [] #已经完成查询的列表
		for i in range(1):
			if len(selectEIDList2)==0:
				break
			selectEIDList1 = selectEIDList2[:]
			#print(selectEIDList1)
			selectEIDList2 = []
			for j in selectEIDList1:
				print('--query: '+str(j))
				selectSQL = 'select distinct eid from tmp_paper where paperid in (select distinct paperid from tmp_paper where eid='+str(j)+' ) limit 100'
				newEidList = getResultList(selectSQL,'eid',cur)
				newList = 0
				for kid in newEidList:
					if (not kid in selectEIDCompleted) and (not kid in totalEID) and (not kid in selectEIDList2):
						selectEIDList2.append(kid)
						newList += 1
						if newList>300:
							break
				selectEIDCompleted.append(j)
				print('++add: '+str(newList))
				totalEID += selectEIDList2
				totalEID = list(set(totalEID))
			totalEID = list(set(totalEID))	
			#print('selectEIDList2 len is '+str(len(selectEIDList2)))
			#print('now len is '+str(len(selectEIDCompleted)))

		print('===========completed: '+name)
		print('now totleEID len is '+str(len(totalEID)))
		totalEID = list(set(totalEID))
		constructSeriz(totalEID_new_pickle,totalEID)
	print('total totleEID len is '+str(len(totalEID)))
	totalEID = list(set(totalEID))
	constructSeriz(totalEID_new_pickle,totalEID)
	'''
	DictAuthor = readSeriz(paperAuthorDict_pickle) #这里包有[]
	DictID = readSeriz(personDict_pickle) #这里包有[]
	totalEID = list(set(totalEID))
	print('begin totleEID len is '+str(len(totalEID)))
	for title in DictAuthor.keys():
		names = DictAuthor[title]
		#print(eid)
		if len(names)==0:
			continue
		selectEIDList1 = []
		selectEIDList2 = [DictID[na] for na in names] #下一次要查询的列表
		selectEIDCompleted = [] #已经完成查询的列表
		for i in range(1):
			if len(selectEIDList2)==0:
				break
			selectEIDList1 = selectEIDList2[:]
			#print(selectEIDList1)
			selectEIDList2 = []
			for j in selectEIDList1:
				print('--query: '+str(j))
				selectSQL = 'select distinct eid from tmp_paper where paperid in (select distinct paperid from tmp_paper where eid='+str(j)+' ) limit 100'
				newEidList = getResultList(selectSQL,'eid',cur)
				newList = 0
				for kid in newEidList:
					if (not kid in selectEIDCompleted) and (not kid in totalEID) and (not kid in selectEIDList2):
						selectEIDList2.append(kid)
						newList += 1
						if newList>300:
							break
				selectEIDCompleted.append(j)
				print('++add: '+str(newList))
				totalEID += selectEIDList2
				totalEID = list(set(totalEID))
			totalEID = list(set(totalEID))	
			#print('selectEIDList2 len is '+str(len(selectEIDList2)))
			#print('now len is '+str(len(selectEIDCompleted)))

		print('===========completed: '+title)
		print('now totleEID len is '+str(len(totalEID)))
		totalEID = list(set(totalEID))
		constructSeriz(totalEID_new_pickle,totalEID)
		#break
	print('total totleEID len is '+str(len(totalEID)))
	totalEID = list(set(totalEID))
	constructSeriz(totalEID_new_pickle,totalEID)	

def prepareDistanceMetrx():
	# 计算合作者之间的关联度
	#import os;os.chdir('e:/Code/Python');import geng_coi;geng_coi.prepareDistanceMetrx()
	PART = 5
	totalEID = readSeriz(totalEID_pickle)
	totalEID = list(set(totalEID))
	lenEID = len(totalEID)
	#distanceMetrx = [[0 for i in range(lenEID)] for j in range(lenEID)]
	limit = int(lenEID/10)
	print('len is '+str(lenEID))
	distanceMetrx_pickle = dictPath + 'distanceMetrx0.pickle'
	distanceMetrx = readSeriz(distanceMetrx_pickle)
	#distanceMetrx_pickle_new = dictPath + 'distanceMetrx'+str(1)+'.pickle'
	distanceMetrx_pickle_new = dictPath + 'distanceMetrx'+str(PART)+'.pickle'
	#print('here0')
	#for i in range(lenEID-1):
	for i in range((limit*(PART-1)),lenEID-1):
		#print('i is '+str(i))
		py1 = getResultList('select paperid from tmp_paper where eid = '+str(totalEID[i]),'paperid',cur)
		set1 = set(py1)
		#print(set1)
		if i > (limit*PART):
			#print('???')
			break
		distanceMetrx[i][i] = 0
		#print('here1')
		for j in range(i+1,lenEID):
			py2 = getResultList('select paperid from tmp_paper where eid = '+str(totalEID[j]),'paperid',cur)
			set2 = set(py2)
			sameset = list(set1 & set2)
			value = 0
			if len(sameset)==0:
				#print('here2')
				distanceMetrx[i][j] = 0 #做完标准化后再附成M
				distanceMetrx[j][i] = 0
			else:
				#print('here3')
				value = measureCoauthor(py1,py2,sameset)
				distanceMetrx[i][j] = value
				distanceMetrx[j][i] = value
			if j%100==0:		
				constructSeriz(distanceMetrx_pickle_new,distanceMetrx)
			print('completed: '+str(i)+', '+str(j)+' value: '+str(value))
			#break
		#break
		constructSeriz(distanceMetrx_pickle_new,distanceMetrx)
	constructSeriz(distanceMetrx_pickle_new,distanceMetrx)
	#distanceMetrx_nor = distanceMetrx / np.linalg.norm(distanceMetrx)
	#constructSeriz(distanceMetrx_pickle_new,distanceMetrx_nor)
	
def measureCoauthor(py1,py2,sameList):
	# 
	value = len(sameList) * (1/len(py1)+1/len(py2))/2
	return round(value,4)

def flody():
	#运行floyd 算最短距离
	distanceMetrx = readSeriz(distanceMetrx_pickle)
	lenD = len(distanceMetrx)
	DSet = []
	D0 = [[0 for i in range(lenD)] for j in range(lenD)]
	DSet.append(distanceMetrx)

	A0 = [[[] for i in range(lenD)] for j in range(lenD)]
	ASet = []
	ASet.append(A0)

	for k in range(1,MAX_TIME):#从1开始，迭代次数
	#初始化一个矩阵
		DSet.append(D0)
		ASet.append(A0)
		for i in range(lenD):
			for j in range(lenD):
				if i==j:
					DSet[k][j][j] = 0
					ASet[k][j][j] = 0
					continue

				MIN_old = DSet[k-1][i][j]
				for x in range(lenD):
					if (x==i) or (x==j):
						continue
					if (DSet[k-1][i][x]>=MIN_old) or (DSet[k-1][x][j]>=MIN_old):
						DSet[k][i][j] = DSet[k-1][i][j]
					else:
						MIN_new = min(MIN_old,(DSet[k-1][i][x]+DSet[k-1][x][j]))
						DSet[k][i][j] = MIN_new
						if MIN_new < MIN_old:
							#记下x
							ASet[k][i][j] = [ASet[k-1][i][x],x,ASet[k-1][x][j]]
						else:
							ASet[k][i][j] = ASet[k-1][i][j]
						MIN_old = MIN_new

		print('now is '+str(k))
		shortDistanceMetrx = DSet[k][:]
		if DSet[k] == DSet[k-1]:
			break

	constructSeriz(shortDistanceMetrx_pickle,shortDistanceMetrx)

def measureCOI():
	#计算COI矩阵 
	reviewerDict = readSeriz(reviewerDict_pickle)
	personDict = readSeriz(personDict_pickle)
	reviewer_ID = readSeriz(reviewer_ID_pickle)
	totalEID = readSeriz(totalEID_pickle)
	paperDict = readSeriz(paperDict_pickle)
	shortDistanceMetrx = readSeriz(shortDistanceMetrx_pickle)

	paperList = list(paperDict.keys())
	reviewerList = list(reviewerDict.keys())
	COI = [[0 for y in range(len(reviewerList))] for x in range(len(paperList))]
	for title in paperDict.keys():
		i = paperList.index(title)
		for rName in reviewerDict.keys():
			j = reviewerList.index(rName)
			rid = reviewer_ID[rName]
			idy = totalEID.index(rid)
			coiList = []
			for name in paperDict[title]:
				eid = personDict[name]
				idx = totalEID.index(eid)
				coi = shortDistanceMetrx[idx][idy]
				coiList.append(coi)
			mcoi = 1 - min(coiList) #这个COI就是这个评审对这个文章的COI
			COI[i][j] = mcoi
	constructSeriz(COI_pickle,COI)
				
if __name__ == '__main__':
	prepareDistanceMetrx()















