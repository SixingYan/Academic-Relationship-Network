# -*- coding: utf-8 -*-
'''
'''
from queue import Queue
from threading import Thread
from tool import getResult,getCursor,constructSeriz,readSeriz,assignNum,readFiles

nidQueue = Queue()
pidQueue = Queue()
gloabID = 0
#import os;os.chdir('E:/Code/Python');import geng_cleanPaper;geng_cleanPaper.insertNull()
#import os;os.chdir('E:/Code/Python');import geng_cleanPaper;geng_cleanPaper.insertNull()
selectResult_pickle = 'F:/Code/Data/pickle/selectResult_pickle.pickle'
pidList_pickle = 'F:/Code/Data/pickle/pidList_pickle.pickle'
nidList_pickle = 'F:/Code/Data/pickle/nidList_pickle.pickle'
doiDict_pickle = 'F:/Code/Data/pickle/doiDict_pickle.pickle'
idPaperid_pickle = 'E:/Code/Pickle/idPaperid_pickle.pickle'
nullDOI_pickle = 'E:/Code/Pickle/nullDoiDict_pickle.pickle'
conn,cur = getCursor()
idyeartitle_path = 'E:/Code/Pickle/id_year_title/idyeartitle_'
yearList_pickle = 'E:/Code/Pickle/yearList_pickle.pickle'
yearList_pickle_null = 'E:/Code/Pickle/yearList_pickle_null.pickle'
idyeartitle_path_null = 'E:/Code/Pickle/id_year_title_null/idyeartitle_'
nullDict = 'E:/Code/Pickle/id_year_title_null'
sameList_pickle = 'E:/Code/Pickle/sameList_pickle_'
single_pickle = 'E:/Code/Pickle/single_pickle_'
class analysisWorker(Thread):
    def __init__(self,pidQueue,nidQueue):
        Thread.__init__(self)
        self.pidQueue = pidQueue
        self.nidQueue = nidQueue
        
    def run(self):
        while True:
            idList = self.pidQueue.get()
            sameList = checkDOI(idList)
            if len(sameList)>0:
                for sl in sameList:
                    self.nidQueue.put(sl)
                    self.pidQueue.task_done()

class mysqlWorker(Thread):
    def __init__(self,nidQueue,gloabID):
        Thread.__init__(self)
        self.nidQueue = nidQueue
        self.gloabID = gloabID
        
    def run(self):
        while True:
            sameList = self.nidQueue.get()
            if len(sameList)>0: #这里只是以防万一
                  for sid in sameList:
                      #
                      updateSQL = 'update paper set paperID='+str(self.gloabID)+' where id='+str(sid)
                      cur.execute(updateSQL)
                      conn.commit()
                      '''
                      updateSQL = 'update paper set pid=null where id='+str(sid)
                      cur.execute(updateSQL)
                      conn.commit()
                      '''
                      print('update:'+str(sid))
                  self.gloabID += 1
            self.nidQueue.task_done()
def clean(sid):
    #
    total = ''
    for s in sid:
        try:
            total += str(int(s))
        except Exception:
            print('somewhere error')
    return total

def getDOI(id):
    #
    doi=''
    result=[]
    id = clean(id)
    str1 = 'select doi from paper where id='+str(id)
    #print('here0')
    #try:
        #print('here!!!!!!!!!')
    cur.execute(str1)
        #print('here')
    for c in cur.fetchall():            
        result.append(c)
        #print('here1')
    #except Exception:
    print('error on getResult, str is ' + str1)
    if len(result)==0:
        print('error: '+id)
    #gr = getResult('select doi from paper where id='+str(id),cur)
    #print('len: '+str(len(result)))
    if len(result)>0:
        doi = result[0]['doi']
    #doi=''
    return doi

def checkDOI(idList):
    #
    doiList = [getDOI(id) for id in idList]
    passList = []
    sameList = []

    for i in range(len(doiList)-1):
        if i in passList:
            continue
        passList.append(i)
        temSameList = []	
        for j in range((i+1),len(doiList)):
            if doiList[i].strip()==doiList[j].strip():
                temSameList.append(i)
                temSameList.append(j)
                passList.append(j)

        if len(temSameList)>0:
            sameList.append(list(set(temSameList)))
            #print('same !')
        else:
            sameList.append([doiList[i]]) #只放入自己的
            #print('no same !')
    return sameList

def preparePid():
    #
    #
    pidList = []
    nidList = []
    selectResult = getResult('select tem.pid,group_concat(tem.id) ids from (select pid, id from paper) tem group by tem.pid',cur)
    constructSeriz(selectResult_pickle,selectResult)
    for sr in selectResult:
        if ',' in sr['ids']:
            #说明不止一个
            idList = sr['ids'].split(',')
            #pidQueue.put(idList)
            #print(idList)
            pidList.append(idList)
        else:
            #nidQueue.put([sr['ids']])
            #print([sr['ids']])
        #print('pid: '+sr['pid'])
            nidList.append(sr['ids'])
    constructSeriz(pidList_pickle,pidList)
    constructSeriz(nidList_pickle,nidList)
    print('completed')

def mainFunction():
    #
    for i in range(6):
        pWorker = analysisWorker(pidQueue,nidQueue)
        pWorker.daemon = True
        pWorker.start()
    print('ok2')

    for j in range(2):
        mWorker = mysqlWorker(nidQueue,gloabID)
        mWorker.daemon = True
        mWorker.start()

    preparePid()


def cleanDOI():
    #id,doi
    paperid = 0
    selectResult = getResult('select id,doi from paper',cur)
    print('read completed')
    doiDict = {}
    idPaperid = []
    for i in range(len(selectResult)):
        if i%5000==0:
            print('now is '+str(i))
        doi = selectResult[i]['doi']
        if doi in doiDict:
            newPaperid = doiDict[doi][1]
            idPaperid.append([selectResult[i]['id'],newPaperid])
        else:
            #add, assign paperid
            paperid += 1
            doiDict[doi] = (selectResult[i]['id'],paperid)
            idPaperid.append([selectResult[i]['id'],paperid])

    #print(doiDict)
    #print(idPaperid)
    constructSeriz(doiDict_pickle,doiDict)
    constructSeriz(idPaperid_pickle,idPaperid)

def updateDOI():
    idPaperid = readSeriz(idPaperid_pickle)
    '''
    for ip in idPaperid:
        updateSQL = 'update paper set paperid='+str(ip[0])+' where id='+str(ip[1])
        cur.execute(updateSQL)
        conn.commit()
        print('completed: '+str(ip[1]))
        #break
    '''
    paperid = []
    id = []
    for ip in idPaperid:
        paperid.append(ip[0])
        id.append(ip[1])

    maxPid = max(paperid)
    print('max is '+str(maxPid))


def analysisNull():
    #
    '''
    nullDOI = readSeriz(nullDOI_pickle)
    print('num is '+str(len(nullDOI)))
    
    nullDOI = readSeriz(nullDOI_pickle)
    for nid in nullDOI:
        updateSQL = 'update paper set paperid=null where id='+str(nid)
        cur.execute(updateSQL)
        conn.commit()
        print('completed: '+str(nid))
        #break
    '''
    yearTitle = [[[] for j in range(27)] for i in range(100)]
    yearList = []
    # 
    selectResult = getResult('select id,time,title from paper where paperid is not null',cur)
    print('read completed, the total is '+str(len(selectResult)))
    num = 0
    for sr in selectResult:
        if not (len(sr['title'])>0 and sr['time']>0):
            continue
        if sr['time'] in yearList:
            yidx = yearList.index(sr['time'])
        else:
            yearList.append(sr['time'])
            yidx = yearList.index(sr['time'])
        nidx = assignNum(sr['title'])

        yearTitle[yidx][nidx].append((sr['id'],sr['time'],sr['title']))
        print('now is '+str(num)+' id is '+str(sr['id']))
        num += 1
    print('begin saving')
    constructSeriz(yearList_pickle,yearList)
    for i in range(len(yearList)):
        for j in range(27):
            path = idyeartitle_path+str(i)+'_'+str(j)+'.pickle'
            if len(yearTitle[i][j])>0:
                constructSeriz(path,yearTitle[i][j])
            else:
                constructSeriz(path,[])

def prepareNULL():
    #
    nullDOI = readSeriz(nullDOI_pickle)
    yearTitle = [[[] for j in range(27)] for i in range(60)]
    yearList = []
    num = 0
    for nid in nullDOI:
        #print(str(nid))
        getR = getResult('select time,title from paper where id='+str(nid),cur)
        sr = getR[0]
        if not (len(sr['title'])>0 and sr['time']>0):
            continue
        if sr['time'] in yearList:
            yidx = yearList.index(sr['time'])
        else:
            yearList.append(sr['time'])
            yidx = yearList.index(sr['time'])
        nidx = assignNum(sr['title'])
        yearTitle[yidx][nidx].append((nid,sr['time'],sr['title']))
        print('now is '+str(num)+' id is '+str(nid))
        num += 1
    print('begin saving')

    constructSeriz(yearList_pickle_null,yearList)    
    for i in range(len(yearList)):
        for j in range(27):
            path = idyeartitle_path_null+str(i)+'_'+str(j)+'.pickle'
            if len(yearTitle[i][j])>0:
                constructSeriz(path,yearTitle[i][j])
def extractYearTitle(fp):
    #
    tem = fp.replace(idyeartitle_path_null,'').replace('.pickle','').split('_')
    yidx_null = int(tem[0])
    nidx_null = int(tem[1])
    return (yidx_null,nidx_null)

def compareNull():
    
    yearListNull = readSeriz(yearList_pickle_null)
    yearList = readSeriz(yearList_pickle)
    for fp in readFiles(nullDict):
        sameList = []
        single = []
        print('now begin: '+str(fp))
        nullYearTitle = readSeriz(fp)
        yidx_null,nidx_null = extractYearTitle(fp)
        if yidx_null >len(yearListNull):
            print('error!!!!!!!!!!????')
            continue
        year = yearListNull[yidx_null]
        if year in yearList:
            yidx = yearList.index(year)
        else:
            print('error!!!!!!!!!!')
            continue

        path = idyeartitle_path+str(yidx)+'_'+str(nidx_null)+'.pickle'
        yearTitle = readSeriz(path)

        if len(yearTitle)<1:
            for i in range(len(nullYearTitle)):
                single.append(nullYearTitle[i][0])

        for i in range(len(nullYearTitle)):
            flag = False
            for j in range(len(yearTitle)):
                if nullYearTitle[i][2] == yearTitle[j][2]:
                    sameList.append([nullYearTitle[i][0],yearTitle[j][0]])
                    flag = True
                    continue
            if flag==False:
                single.append(nullYearTitle[i][0])

        sameList_path = sameList_pickle+str(yidx)+'_'+str(nidx_null)+'.pickle'
        single_path = single_pickle+str(yidx)+'_'+str(nidx_null)+'.pickle'
        constructSeriz(sameList_path,sameList)
        constructSeriz(single_path,single)

def insertNull():
    #
    '''
    fileList = readFiles('E:/Code/Pickle/samesingle/same')
    for fp in fileList:
        sameList = readSeriz(fp)
        for sl in sameList:
            selectResult = getResult('select paperid from paper where id='+str(sl[1]),cur)
            updateSQL = 'update paper set paperid='+str(selectResult[0]['paperid'])+' where id='+str(sl[0])
            print(updateSQL)
            cur.execute(updateSQL)
            conn.commit()
            print('completed: '+str(selectResult[0]['paperid'])+' '+str(sl[0]))
        print('update: '+fp)

    '''
    maxPID = 4263215
    fileList = readFiles('E:/Code/Pickle/samesingle/single')
    for fp in fileList:
        print('update: '+fp)
        single = readSeriz(fp)
        for s in single:
            maxPID += 1
            updateSQL = 'update paper set paperid='+str(maxPID)+' where id='+str(s)
            cur.execute(updateSQL)
            conn.commit()
            print('now is '+str(maxPID))
    
if __name__ == '__main__':
	cleanDOI()
