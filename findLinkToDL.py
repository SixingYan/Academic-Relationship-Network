from bs4 import BeautifulSoup
from tool import getTag,findTag,writeMetrx,readMetrix,readTXT,readList,ChangeOrNot
import requests
import random
import time

headers = {'content-type': 'application/json',
           'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}

httpProxies = {'https':'http://165.138.65.233:3128'}
#httpsProxies = {'http':'','https':''}

#cookie = {'CFID':'882151223', 'CFTOKEN':'41643175', 'DEEPCHK':'1'} 

seedList_path = 'e:/Code/Data/seedList.csv'
dl_path = 'E:/Code/Data/dlList.csv'
http_path = 'E:/Code/Data/http.txt'
ua_path = 'E:/Code/Data/userAgent.csv'

def getHttpUa(http_path,ua_path):
	#
	http=readTXT(http_path) # read txt
	ua = readList(ua_path) # read csv

	return http,ua

def editeProxies(http):
	#
	i = random.randint(0,len(http)-1)
	httpProxies["https"] = http[i]

def editeHeader(ua):
    #
	i = random.randint(0,len(ua)-1)
	headers['User-Agent'] = ua[i]	

def findDL():
    #
    http,ua = getHttpUa(http_path,ua_path)
    seedList = readMetrix(seedList_path)
    total = len(seedList)
    print('total is '+ str(total))
    i = 0
    for s in seedList:
        i += 1
        print('now is '+ str(i))

        if ChangeOrNot() == True:
            editeProxies(http)
            editeHeader(ua)

        time.sleep(random.randint(1, 10))
        url = findLink(s)
        try:
            writeMetrx(dl_path,[[s[0],url]])
        except:
            print('write error! it is '+ s[0])
            writeMetrx(dl_path,[[s[0],'error']])
            
def findLink(nameAndLink):
    #
    try:
        r = requests.get(nameAndLink[1], proxies = httpProxies, headers = headers, timeout = 15)
        if r.status_code == 200:
            r.encoding = 'utf-8'
            htmlText = r.text
            url = extractDL(htmlText)
            return url
        else:
            print('error: code = ' + str(r.status_code))
            print('error position = ' + nameAndLink[0])
            return 'extract error!'
    except requests.RequestException as e:
        print(e)
        return 'requests error!'
    else:
        pass

def extractDL(htmlDocs):#
    url = ''
    soup = BeautifulSoup(''.join(htmlDocs),"lxml")
    tagContent = getTag(soup,'div','class','dl')
    soup = BeautifulSoup(''.join(tagContent),"lxml")
    url = findTag(soup,'a','href')
    
    return url

if __name__ == '__main__':	
    findDL()