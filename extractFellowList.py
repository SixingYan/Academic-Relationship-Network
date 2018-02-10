'''
prepare seed
'''
from bs4 import BeautifulSoup
import csv

html_path = 'e:/Code/Data/ACM Fellows.txt'
seedList_path = 'e:/Code/Data/seedList.csv'
ulText_path = 'e:/Code/Data/onlyForTest.txt'

def writeMetrx(path,Metrx):
	#add continuely
    with open(path, 'a+',newline='') as csvfile:
        writer = csv.writer(csvfile,quoting=csv.QUOTE_NONNUMERIC)
        for row in Metrx:
            writer.writerow(row)

def readTXT(path):
	#
	fp = open(path,'r')
	docs = fp.readlines()
	fp.close()

	return docs
 
def writeTXT(path,text):
    fp = open(path, 'w')
    fp.write(text)
    fp.close()
    
def getTag(soup, target, attribute, label):
	#
     for t in soup.findAll(target):
         if label in t[attribute]:
             #writeTXT(ulText_path, str(t))             
             return str(t)
    
     return ''

def getTagSet(docs, target, attribute):
	#
     tagSet = []
     soup = BeautifulSoup(''.join(docs),"lxml")
      
     for a in soup.findAll('a'):
         tagSet.append([a.string.strip('\n'), a[attribute]])
	
     return tagSet

if __name__ == '__main__':
	htmlDocs = readTXT(html_path)
	soup = BeautifulSoup(''.join(htmlDocs),"lxml")
	ulTagCont = getTag(soup,'ul','class','award-winners-list')
	aTagSet = getTagSet(ulTagCont,'a','href')
	writeMetrx(seedList_path, aTagSet)

	