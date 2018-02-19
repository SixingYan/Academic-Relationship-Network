# -*- coding: utf-8 -*-
import csv
import random
import pymysql
import requests
import string
import os
import pickle
import gc



signal = '#bun#'
letter = {'a':0,'b':1,'c':2,'d':3,'e':4,'f':5,'g':6,'h':7,'i':8,'j':9,'k':10,'l':11,'m':12,'n':13,'o':14,'p':15,'q':16,'r':17,'s':18,'t':19,'u':20,'v':21,'w':22,'x':23,'y':24,'z':25,'other':26}
#commHeaders = {'content-type': 'application/json','User-Agent': 'Opera/9.25 (Windows NT 5.1; U; en)'}
commHttpProxies = {'https':'182.253.121.137:8080'}
#headers = {'content-type': 'application/json','User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
#httpProxies = {'https':'103.230.62.82:8080'}
commHeaders = {}
commHeaders['content-type']='application/json'
commHeaders['User-Agent']='Opera/9.25 (Windows NT 5.1; U; en)'
commHeaders['Accept']='text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
commHeaders['Accept-Language']='zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3'
commHeaders['Accept-Encoding']='gzip, deflate, br'
commHeaders['Referer']='https://dl.acm.org/results.cfm?query=big+data&Go.x=30&Go.y=11'

commCookies = {}
commCookies['CFID']=''
commCookies['CFTOKEN']=''
commCookies['DEEPCHK']=''

cookiePairs=[
    [716361429,70843524],
    [804126486,91182901],
    [887589065,66576478],
    [569750122,89888184],
    [451513619,23451315],
            ]
def assignNum(name):
    #
    name = name.strip().lower()
    char = name[0]
    if not char in letter.keys():
        return letter['other']
    else:
        return letter[char]
def cleanMem(target):
    del target
    gc.collect()
#188.39.117.101:8080 203.142.70.202:8080 103.230.62.82:8080
def readSeriz(pickle_path):
    #
    with open(pickle_path,'rb') as f:
        targetFile = pickle.load(f)
    return targetFile
    
def constructSeriz(pickle_path,targetFile):
    #
    with open(pickle_path,'wb') as f:
        pickle.dump(targetFile,f)

def cleanURL(url):
    indx = url.find('&CFID=')
    if indx >-1:
        url = url[:indx] 
    return url

def cleanName(name):
    #用于去除名字之间的双空格
    if type(name)!=str:
        return name

    newName = ''
    for n in name.split(' '):
        if len(n)==0:
            continue
        else:
            newName = newName +n.strip()+' '

    newName2 = ''
    for na in newName:
        if na not in (string.punctuation+''+string.digits):
            newName2 += na
    return newName2.strip()

def combineList(strList):
    #input one dimension list
    comStr = ''
    for strl in strList:
        comStr += strl
        comStr += signal
    return comStr
    
def isTheSame(str1,str2):
    #if same, return only one;else return the combined one
    if type(str1)!=str or type(str2)!=str:
        return str1
        
    str0 = ' '
    if str1 == str2:
        str0 = str1
    else:
        str0 = str1 + signal + str2

    return str0

def getPage(url,httpProxies,headers,cookies):
    #获取页面信息input is a string of url
    htmlText = ' '
    try:
        #requests.encoding = 'utf-8'
        if ChangeOrNot==True:
            #r = requests.get(url.encode().decode('utf-8'), proxies=httpProxies, headers=headers, cookies=cookies,timeout=30)
            r = requests.get(url.encode().decode('utf-8'), proxies=httpProxies, headers=headers, timeout=45)
        else:
            r = requests.get(url.encode().decode('utf-8'), proxies=httpProxies, headers=headers, timeout=45)
        if r.status_code == 200:
            r.encoding = 'utf-8'
            htmlText = r.text
            return htmlText
        else:
            print('error: code = ' + str(r.status_code))
            print('error: url = '+ url)
            return ' '
    except requests.RequestException as e:
        print(e)
        return ' '
    else:
        pass

def getResult(str1,cur):
    #
    #默认返回数据是一维数据，里面有字典组成
    #所以return二维数组
    result = []
    try:
        cur.execute(str1)
        for c in cur.fetchall():            
            result.append(c)
    except Exception:
        print('error on getResult, str is ' + str1)

    return result

def getCursor():
    #
    conn = pymysql.connect(host='localhost',user='root',passwd='',db='academic',port=3306,charset='utf8')
    #游标设置为字典类型
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    return conn,cur

def getHttpUa(flag=1):
    #
    if flag ==1:
        http_path = 'E:/Code/Data/http.txt'
    else:
        http_path = 'E:/Code/Data/http_geng.txt'
    ua_path = 'E:/Code/Data/userAgent.csv'
    http=readTXT(http_path) # read txt
    ua = readList(ua_path) # read csv
    return http,ua

def getResultList(str1,volume,cur):
    #当volum只有一个的时候，应该返回一个一维的列表
    #所以return一维数组
    result = []
    try:
        cur.execute(str1)
        for c in cur.fetchall():            
            result.append(c[volume])
    except Exception:
        print('error on getResult, str is ' + str1)
    return result
    
def editeProxies(http,httpProxies):
    #
    httpProxies["https"] = http.strip()
    return httpProxies
    
def editeHeader(ua,headers,dlName):
    #
    headers['User-Agent'] = ua.strip()
    headers['Referer'] = randomReferer(dlName)   
    return headers
#改变referer
def randomReferer(name):
    #input name
    word = []
    index = name.find(' ')
    if index != -1:
        word.append(name[:index])
        word.append(name[(index+1):])
    else:
        index = int(len(word)/2)
        word.append(name[:index])
        word.append(name[(index+1):])

    if type(word)!=list or len(word)!=2:
        return ''
        
    choice = random.randint(0,5)
    #choice = 3
    if choice==0:
        return 'https://www.baidu.com/s?ie=UTF-8&wd='+word[0]+'%20'+word[1]
    elif choice==1:
        return 'https://www.google.com/?gws_rd=ssl#q='+word[0]+'+'+word[1]
    elif choice==2:
        return 'http://cn.bing.com/search?q='+word[0]+'+'+word[1]+'&qs=n&form=QBLH&sp=-1&pq='+word[0]+'+'+word[1]+'&sc=8-15&sk=&cvid=F90E87E078CF42929C149AACD841CB46'
    elif choice==3:
        return 'https://dl.acm.org/results.cfm?query='+word[0]+'+'+word[1]+'&Go.x=30&Go.y=11'
    else:
        return ''
def readStopWord():
    #
    stopword_path = 'F:/newsAnalysis/data/stopwordChinese.txt'
    stopword = [sw.strip() for sw in readTXT(stopword_path)]
    return stopword
#改变cookies
def editeCookies(cookies):
    #
    if ChangeOrNot==True:
    #if False:
        choice =  random.choice(cookiePairs)
        cookies['CFID']=choice[0]
        cookies['CFTOKEN']=choice[1]
        cookies['DEEPCHK']='1'
    else:
        cookies['CFID']=''
        cookies['CFTOKEN']=''
        cookies['DEEPCHK']=''
    return cookies

def readFiles(files_path):
    #
    filePathList = []
    for fileName in os.listdir(files_path):
        if len(fileName)>1:
            newFilePath = files_path+'/'+fileName
            filePathList.append(newFilePath)
    return filePathList

def readTwoLines(path):
    line1 = []
    line2 = []    
    with open(path, newline = '',encoding= 'utf-8') as f:
        reader = csv.reader(f)        
        for row in reader:
            line1.append(row[0])
            line2.append(row[1])
        return line1,line2
        
def readMetrix(path):
    Metrix = []
    try:
        with open(path, 'r', newline='',encoding='utf-8') as f:
            reader = csv.reader(f)        
            for row in reader:
                Metrix.append(row)
            
    except Exception:
        print('readError '+path)
    return Metrix

def readList(path):
    List = []
    with open(path, 'r', newline='',encoding='utf-8') as f:
        reader = csv.reader(f)        
        for row in reader:
            List.append(row[0])
        return List

def readTXT(path):
    #
    #fp = open(path,'r',encoding= 'utf-8');docs = fp.readlines();fp.close()
    fp = open(path,'r',encoding= 'utf-8')
    #docs = fp.readlines()
    docs = fp.read()
    fp.close()

    return docs

def writeList(path,List):
    with open(path, 'a+',newline='',encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile,quoting=csv.QUOTE_NONNUMERIC)
        for row in List:
            writer.writerow([row])
            
def writeMetrx(path,Metrx):
    try:
        with open(path, 'a+',encoding='utf-8',newline='') as csvfile:
            writer = csv.writer(csvfile,quoting=csv.QUOTE_NONNUMERIC)
            for row in Metrx:
                writer.writerow(row)
    except Exception:
        pass
def writeTXT(path,text):
    fp = open(path,'w',encoding="utf-8")
    #fp.write(text.encode("GBK", 'ignore'))
    fp.write(text)
    fp.close()

def ChangeOrNot():
	#
	if random.randint(0,1) == 0:
		return True
	else:
		return False    

def str2floatMetrx(strM):
    intM = [[] for i in range(len(strM))]
    for i in range(len(strM)):
        for j in range(len(strM[i])):
            intM[i].append(float(strM[i][j]))            
    return intM

def str2intMetrx(strM):
    intM = [[] for i in range(len(strM))]
    for i in range(len(strM)):
        for j in range(len(strM[i])):
            intM[i].append(int(strM[i][j]))            
    return intM

def str2floatList(strL):
    intL = [0.0 for i in range(len(strL))]
    for i in range(len(strL)):
        intL[i] = float(strL[i])            
    return intL

def isContain(word1,word2):
    if word1 == word2:
        return True
    
    l1 = len(word1)
    l2 = len(word2)
    
    if l1>=l2:
        if word2 in word1:
            return True
        else:
            return False
    else:
        if word1 in word2:
            return True
        else:
            return False
        
def change(t1,t2):
    tem = t1
    t1 = t2
    t2 = tem 
    return t1,t2

def coSortB2S(list1,list2):
    for i in range(len(list1)):
        for j in range(len(list1)):
            if list1[i] < list1[j]:
                list1[i],list1[j] = change(list1[i],list1[j])
                list2[i],list2[j] = change(list2[i],list2[j])
    return list1,list2
