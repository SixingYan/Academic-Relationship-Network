# -*- coding: utf-8 -*-
'''
	计算专家与投稿论文的研究主题
	放入两个字典，主题词列表，
'''
#import os;os.chdir('e:/Code/Python');import topicP2R;topicP2R.measure_topicSIM()
#保留多少个主题数量，30个
from tool import readSeriz,constructSeriz
from nltk.corpus import wordnet as wn
from nltk.corpus import stopwords
from gensim import corpora, models, similarities
stopword = stopwords.words('english')
dictPath = 'E:/Code/experience/pickle/'
word_pickle = dictPath+'word.pickle'

paperDictT_pickle = dictPath+'paperDictT.pickle'
reviewerDictT_pickle = dictPath+'reviewerDictT.pickle'
paperDictTopic_pickle = dictPath+'paperDictTopic.pickle'
reviewerDictTopic_pickle = dictPath+'reviewerDict_new.pickle'

def prepareTopic():
	subL1 = 'E:/Code/experience/pickle/subPickle 2012.pickle'
	subL2 = 'E:/Code/experience/pickle/subPickle 2013.pickle'
	subL3 = 'E:/Code/experience/pickle/subPickle 2014.pickle'
	subL4 = 'E:/Code/experience/pickle/subPickle 2015.pickle'
	#stopword = readStopWord()
	#准备保留词表数量
	sub1 = readSeriz(subL1)
	sub2 = readSeriz(subL2)
	sub3 = readSeriz(subL3)
	sub4 = readSeriz(subL4)
	sub = sub1+sub2+sub3+sub4
	sub = list(set(sub))
	topicWord = []

	#停用词
	for s in sub:
		s = s.replace('\ufeff','').replace('/',' ')
		for w in s.split():
			if not w in stopword and len(w)>2:
				newTopic = ''				
				nw = wn.morphy(w)#词干化
				try:
					newTopic += (nw+'') #变成新的
				except Exception:
					newTopic += (w+'') #不变
				
				topicWord.append(newTopic)
	newTopic = list(set(newTopic))
	#print(topicWord)
	constructSeriz(word_pickle,topicWord)#序列化

def function():
	#paperDictTopic reviewerDictTopic
	key = paperDictTopic

def measure_topicSIM(topicsNum = 30):
	#
	paperDictTopic = readSeriz(paperDictTopic_pickle)
	reviewerDictTopic = readSeriz(reviewerDictTopic_pickle)
	wordOfBagSet = []
	
	i = 0
	paperID = {}
	#通过wordofbad的序号，索引paperID列表，然后知道这是paper还是reviewer的paper
	for title in paperDictTopic:
		wordOfBagSet.append(paperDictTopic[title])
		paperID[i] = (True,title)
		i += 1
	
	for reviewer in reviewerDictTopic:
		for title in reviewerDictTopic[reviewer]:
			wordOfBagSet.append(title)
			paperID[i] = (False,title,reviewer)
			i += 1
	
	texts = wordOfBagSet[:]

	dictionary = corpora.Dictionary(texts)
	#dictionary.save('F:/newsAnalysis/data/newswordsall.dict')
	corpus = [dictionary.doc2bow(text) for text in texts]
	#corpora.MmCorpus.serialize('/tmp/deerwester.mm', corpus)

	tfidf = models.TfidfModel(corpus)
	corpus_tfidf = tfidf[corpus]
	
	ldaModel = models.LdaModel(corpus_tfidf, id2word=dictionary, num_topics=topicsNum)

	ldaModel.show_topics()
	corpus_lda = ldaModel[corpus] #得到各文本的倾向
	index = similarities.MatrixSimilarity(ldaModel[corpus_lda])
	#corpora.MmCorpus.serialize('/tmp/deerwester.mm', corpus)
	#corpus_lda = [[(d[0],round(float(d[1]),5)) for d in doc] for doc in corpus_lda0]
	#for doc in corpus_lda:
	#	print(doc)
	'''
	#参考部分
	doc = "Human computer interaction"
	vec_bow = dictionary.doc2bow(doc.lower().split())
	vec_lsi = ldaModel[vec_bow] # convert the query to LSI space
	index = similarities.MatrixSimilarity(ldaModel[corpus_lda])
	sims = index[vec_lsi]
	print(sims)
	'''
	'''
	paperList = list(paperDictT.keys())
	reviewerList = list(reviewerDictT.keys())

	TOPIC = [[0 for j in range(len(reviewerList)) ] for i in range(len(paperList))]
	paperDictT = {}
	#reviewerDictT = {}

	for i in range(len(corpus_lda)):
		if paperID[i][0] == True:
			#投稿文章    {title:[1,2,3,4,5],}
			#paperDictT[paperID[i][1]] = corpus_lda[i]
			paperDictT[paperID[i][1]] = 
		else:
			#pass
			if paperID[i][2] in reviewerDictT.keys():
				reviewerDictT[paperID[i][2]].append(corpus_lda[i])
			else:
				reviewerDictT[paperID[i][2]] = []
				reviewerDictT[paperID[i][2]].append(corpus_lda[i])
	'''
	corpusLen = 0
	TOPIC = [[0 for j in range(len(reviewerList)) ] for i in range(len(paperList))]
	#metrx = [[0 for i in range(corpusLen)] for ]
	for i in range(len(paperList)):
		paperList[i]
		corpus_lda_vector = paperDictT[paperList[i]]
		sims = index[corpus_lda_vector]

		for j in range(len(reviewerList)):
			TOPIC[i][j] = sims[j]
	return 


	#print(paperDictT_pickle)
	constructSeriz(paperDictT_pickle,paperDictT)
	constructSeriz(reviewerDictT_pickle,reviewerDictT)
	constructSeriz(corpus_lda_pickle,corpus_lda)
	
if __name__ == '__main__':
	measure_topicSIM()















