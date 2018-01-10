# -*- coding: utf-8 -*-
file_path = 'F:/Code/experience/Subject areas 2012.txt'
subPickle_path = 'F:/Code/experience/pickle/subPickle 2012.pickle'
accepted_path = 'F:/Code/experience/acceptedpaper 2013.txt'
paperDict_pickle = 'F:/Code/experience/pickle/acceptedpaper 2013.pickle'
reviewerDict_pickle = 'E:/Code/experience/pickle/reviewerDict.pickle'
path = 'E:/BaiduYunDownload/geng/experience/'
pathList = [path+'Industry & Government Track Program Committee 2015.txt',
			path+'Industry & Government Track Senior Program Committee 2015.txt',
			path+'Research Track Program Committee 2015.txt',
			path+'Research Track Senior Program Committee 2015.txt']
pathListICDM = [path+'Research Track Program Committee 2015 ICDM.txt',
				path+'Research Track Senior Program Committee 2015 ICDM.txt']
#import os;os.chdir('e:/Code/Python');import geng_extractsubject;geng_extractsubject.extractReviewer()

from tool import constructSeriz
def extractSub2012():
	topicSet = []
	file = open(file_path)
	for fl in file:
		#按行读取
		if len(fl)>3:
			fl = fl.lower()
			text = fl.split(': ')[1]
			text=text.replace(',',' and')
			text=text.replace('//',' and ')
			if 'and' in text:
				for word in text.split('and'):
					topicSet.append(word.strip())
			else:
				topicSet.append(text.strip())

	topicSet = list(set(topicSet))
	for tp in topicSet:
		print(tp)
	constructSeriz(subPickle_path,topicSet)

def extractSub():
	topicSet = []
	file = open(file_path,encoding='utf-8')
	for fl in file:
		#按行读取
		fl = fl.strip()
		fl = fl.replace('/t',' and')
		if len(fl)>3:
			text = fl.lower()
			#text = fl.split(': ')[1]
			text=text.replace(',',' and')
			text=text.replace('//',' and ')
			text=text.replace('for','and')
			if 'and' in text:
				for word in text.split('and'):
					topicSet.append(word.strip())
			else:
				topicSet.append(text.strip())

	topicSet = list(set(topicSet))
	for tp in topicSet:
		print(tp)
	constructSeriz(subPickle_path,topicSet)

def extractPaper2015():
	#
	rfile = open(accepted_path,encoding='utf-8')
	rfile = list(rfile)
	paperDict = {}
	i=0
	for j in range(len(rfile)):
		i+=1
		if len(rfile[j])<10 or ('<h2 id=' in rfile[j]):
			continue
		else:
			parts = rfile[j].split('</strong><br/>')
			title = parts[0].replace('<strong>','').strip()
			authorInstit = parts[1].split('<br/>')[0].strip()
			authors = []
			aipairs = authorInstit.split(';')
			for k in range(len(aipairs)):
				if ',' in aipairs[k]:
					idx = aipairs[k].index(',')
					name = aipairs[k][:idx].strip()
					institu = aipairs[k][idx+1:].strip()
				else:
					try:
						institu = aipairs[k].strip()
						idx = aipairs[k-1].rindex(',')
						name = aipairs[k][idx+1:].strip()
						authors[len(authors)-1][1] = authors[len(authors)-1][1][:idx]
					#print('error: '+authorInstit)
					except Exception:
						print(authorInstit)
						break
				authors.append([name,institu])
			if title in paperDict.keys():
				title += (' '+str(i))
				paperDict[title]=authors
			else:
				paperDict[title]=authors
		print('completed: '+title)
		#break
	constructSeriz(paperDict_pickle,paperDict)
	print('OK')

def extractPaper2014():
	#
	rfile = open(accepted_path,encoding='utf-8')
	rfile = list(rfile)
	paperDict = {}
	i=0
	for j in range(len(rfile)):
		i+=1
		if not '<strong>' in rfile[j]:
			continue
		else:
			parts = rfile[j].split('</strong> <br/>')
			title = parts[0].replace('<strong>','').replace('<s>','').strip()
			authorInstit = parts[1].split('<br/>')[0].strip()
			authors = []
			aipairs = authorInstit.split(';')
			for k in range(len(aipairs)):
				if len(aipairs[k])<5:
					continue
				if ',' in aipairs[k]:
					idx = aipairs[k].index(',')
					name = aipairs[k][:idx].strip()
					institu = aipairs[k][idx+1:].strip()
				else:
					try:
						institu = aipairs[k].strip()
						idx = aipairs[k-1].rindex(',')
						name = aipairs[k][idx+1:].strip()
						authors[len(authors)-1][1] = authors[len(authors)-1][1][:idx]
					#print('error: '+authorInstit)
					except Exception:
						print(authorInstit)
						break
				authors.append([name,institu])
			if title in paperDict.keys():
				title += (' '+str(i))
				paperDict[title]=authors
			else:
				paperDict[title]=authors
		print('completed: '+title)
		#break
	constructSeriz(paperDict_pickle,paperDict)
	print('OK')

def extractPaper2013():
	#
	rfile = open(accepted_path,encoding='utf-8')
	rfile = list(rfile)
	paperDict = {}
	i=0
	title = ''
	for j in range(len(rfile)):
		i+=1
		if '<strong>' in rfile[j] and '<br />' in rfile[j]:
			#title
			start = rfile[j].index('</strong>')
			end = rfile[j].index('<br />')
			title = rfile[j][start+len('</strong>'):end].strip()
			continue
		elif 'Authors: ' in rfile[j]:
			authorInstit = rfile[j].replace('Authors: ','').replace('<br />','').replace('</h5>','')
			authors = []
			aipairs = authorInstit.split(';')
			for k in range(len(aipairs)):
				if len(aipairs[k])<5:
					continue
				if ',' in aipairs[k]:
					idx = aipairs[k].index(',')
					name = aipairs[k][:idx].strip()
					institu = aipairs[k][idx+1:]
					if len(institu)>1:
						institu = institu.strip()
					else:
						institu = 'Null'
				else:
					try:
						institu = aipairs[k].strip()
						idx = aipairs[k-1].rindex(',')
						name = aipairs[k][idx+1:].strip()
						authors[len(authors)-1][1] = authors[len(authors)-1][1][:idx]
					#print('error: '+authorInstit)
					except Exception:
						print(authorInstit)
						break
				authors.append([name,institu])
			if title in paperDict.keys():
				title += (' '+str(i))
				paperDict[title]=authors
			else:
				paperDict[title]=authors
			print('completed: '+title)
			print(authors)
			print()
		else:
			#none
			pass
		
		#break
	constructSeriz(paperDict_pickle,paperDict)
	print('OK')

def extractReviewer():
	reviewerDict = {}
	for p in pathList:
		rfile = open(p,encoding='utf-8')
		rfile = list(rfile)
		for j in range(len(rfile)):
			if '(' in rfile[j]:
				parts = rfile[j].split(' (')
				name = parts[0].strip()
				if not name in reviewerDict.keys():
					reviewerDict[name] = parts[1].replace(')','').strip()
	for p in pathListICDM:
		rfile = open(p,encoding='utf-8')
		rfile = list(rfile)
		for j in range(len(rfile)):
			if len(rfile[j])>4:
				parts = rfile[j].split(' 	')
				name = parts[0].strip()
				if not name in reviewerDict.keys():
					reviewerDict[name] = parts[1].strip()
	print(reviewerDict)
	constructSeriz(reviewerDict_pickle,reviewerDict)

if __name__ == '__main__':
	extractPaper2013()




	#&quot;