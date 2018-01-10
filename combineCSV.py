import csv
import os
from tool import cleanName,combineList
def analysisCSV():
	#
	conn,cur = getCursor()
	filePathList = readFiles(csvFiles_path)
	for csvFile in filePathList:
		paperList = readPaperCSV(csvFile)
		for i in range 
#读出csv文件夹
def readFiles(path):
	#
	filePathList = []
	for fileName in os.listdir(path):
		newFilePath = path+'/'+fileName
		filePathList.append(newFilePath)
	return filePathList

def readPaperCSV(path):

	paperList = []
	with open(path) as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			paper = {}
			paper['pid'] = int(row['id'].strip())
			paper['authors'] = cleanAuthors(row['author'])
			paper['title'] = row['title'].strip()
			paper['doi'] = row['doi'].strip()
			paper['year'] = int(row['year'].strip())
			paperList.append(paper)

	return paperList

def cleanAuthors(authors):
	combinedName = ' '
	authorsList = authors.split('and')
	for i in range(len(authorsList)):
		authorsList[i] = authorsList[i].strip()
		authorsList[i] = cleanName(authorsList[i])

	combinedName = combineList(authorsList)
	return combinedName

def addInfo(conn,cur,paper):
	checkSQL = 'select * from '
	insertSQL = 'insert into paper (pid,title,authors,time,doi) values (%d%s%s%d%s)'
	insertValue = (paper['pid'],paper['title'],paper['authors'],paper['year'],paper['doi'])
	try:
		cur.execute(insertSQL,insertValue)
		conn.commit()
	except Exception:
		print('error!:'+str(insertValue))

if __name__ == '__main__':	
	analysisCSV()