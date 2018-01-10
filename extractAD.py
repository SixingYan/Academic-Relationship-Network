'''
	从adv.csv中抽取指导关系


'''
from tool import readFiles,cleanName,getCursor
import datetime
import csv

#import os;os.chdir('e:/Code/Python');import extractAD;extractAD.mainFunction()
files_path = 'E:/Code/Data/adv'

conn,cur = getCursor()

def readPaperCSV(path):
    #
    paperList = []
    eid = path.split('advlist_')[1].replace('.csv','')

    with open(path, encoding= 'utf-8') as csvfile:
        reader = csv.DictReader(csvfile)       
        for row in reader:
            paper = {}
            paper['degree'] = row['type'].strip()
            
            paper['sname'] = cleanName(row['author'])
            paper['tname'] = cleanName(row['advisor'])
            paper['time'] = int(row['year'])
            paper['thesis'] = row['title'].strip()
            
            paper['teacher'] = int(eid)
            
            if len(str(row['year']))>0:
                paper['time'] = int(row['year'])
            else:
                paper['time'] = 0
            
            paper['school'] = row['publisher'].strip()
            
            paperList.append(paper)
    return paperList
          
def insertPaperSQL(filePath):
    flag = True
    try:
        paperList = readPaperCSV(filePath)
        
        if len(paperList)==0:
            return flag
        for paper in paperList:
            try:
                addInfo(paper)
            except Exception:
                print('some paper error!')
                flag = False
        
        print('complete:'+str(paperList[0]['teacher'])+' '+str(datetime.datetime.now()))
    
    except Exception:
        print('some csv error')
        flag = False
    return flag

def addInfo(paper):
    #
    if False:
        pass
    else:
        insertSQL='insert into adsadv (teacher,thesis,time,sname,tname,degree,school) values ('+str(paper['teacher'])+',"'+paper['thesis']+'",'+str(paper['time'])+',"'+paper['sname']+'","'+paper['tname']+'","'+str(paper['degree'])+'","'+paper['school']+'")' 
    #加入paper记录
        try:		
            cur.execute(insertSQL)
            conn.commit()
        except Exception:
            print('error!:'+str(insertSQL))

def mainFunction():
    conn,cur = getCursor()
    filePathList = readFiles(files_path)
    for fileP in filePathList:
        insertPaperSQL(fileP)
        #break#只运行一次
    
    cur.close()
    conn.close()

if __name__ == '__main__':
	mainFunction()
























