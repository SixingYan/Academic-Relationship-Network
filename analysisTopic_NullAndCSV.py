'''
	标记空 subject url 和读入topicsupply_xx.csv文件
'''
import csv
from tool import getCursor,readList,readFiles,readMetrix
conn,cur = getCursor()
files_path = 'E:/Code/Data/subUrlNull last.csv'
#import os;os.chdir('e:/Code/Python');import analysisTopic_NullAndCSV;analysisTopic_NullAndCSV.markedEmpty()
dict_path = 'E:/Code/Data/su1'
#import os;os.chdir('e:/Code/Python');import analysisTopic_NullAndCSV;analysisTopic_NullAndCSV.mainFunction()
def markedEmpty():
    #
    idList = []
    with open(files_path,newline='',encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            idList.append(row['id'])
        print('total:'+str(len(idList)))

    for idl in idList:
        updateSQL = 'update dlurl1 set status=2 where id='+str(idl)
        cur.execute(updateSQL)
        conn.commit()
        print('complete null : '+str(idl))

def insertPaperSQL(eid,filePath):
    #
    flag = False
    topicIDList = extractTopicFromCsv(eid,filePath) #现在是第1种方法
    try:
        
        if not len(topicIDList)>0:
            cur.execute('update dlurl1 set status=2 where id='+str(eid))
            conn.commit()
            return flag
        for topic in topicIDList:
            try:
                addInfo(topic,1) #用第1种方法
            except Exception:
                print('some topic error!'+filePath)

        cur.execute('update dlurl1 set status=2 where id='+str(eid))
        conn.commit()
        print('complete:'+str(eid))
    except Exception:
        print('some csv error'+filePath)
        print(str(eid))
        print(topicIDList)
        flag = False
    return flag

def extractTopicFromCsv(eid,path):
    #
    topicIDList = []
    checkSQL = 'select id from dlurl1 where status<>2 and id='+str(eid)
    try:
        num = cur.execute(checkSQL)
    except Exception:
        print('check error '+checkSQL)
        return topicIDList
    if int(num)==0:
        print('pass: '+str(eid))
        return topicIDList
        
    topicList = readList(path)
    if not len(topicList) >0:
        return topicIDList
    for tpc in topicList:
        if len(tpc)>3:
            topicIDList.append({'eid':eid,'topic':tpc})
    return topicIDList

def extractTopicFromCsv2(eid,path):
    #这个是有数量的
    topicIDList = []
    num = cur.execute('select id from dlurl1 where status<>2 and id='+str(eid))
    if int(num)==0:
        print('pass: '+str(eid))
        return topicIDList
    topicList = readMetrix(path)
    if not len(topicList) >0:
        return topicIDList
    for tpc in topicList:
        if len(tpc)>1:
            topicIDList.append({'eid':eid,'topic':tpc[0],'num':tpc[1]})
    return topicIDList

def addInfo(topicID,flag=1):
    #
    if flag == 1:
        insertSQL='insert into topic (eid,topic,num) values ('+str(topicID['eid'])+',"'+topicID['topic']+'",'+str(1)+')'
    else:
        insertSQL='insert into topic (eid,topic,num) values ('+str(topicID['eid'])+',"'+topicID['topic']+'",'+str(topicID['num'])+')' 
    try:        
        cur.execute(insertSQL)
        conn.commit()
    except Exception:
        print('error!:'+str(insertSQL))
def cleanID(mixstr):
    #将混有unicode
    id = ''
    for i in mixstr:
        try:
            a=int(i)
        except Exception:
            continue
        id += str(a)
    return id
def mainFunction():
    filePathList = readFiles(dict_path)
    for fileP in filePathList:
        eid = fileP.split('topicsupply_')[1].replace('.csv','')
        id = cleanID(eid)
        insertPaperSQL(id,fileP)
        #break
    
if __name__ == '__main__':
    #
    mainFunction()