'''
抽取专家的Digital library上的profile页面，e.g.,http://dl.acm.org/author_page.cfm?id=81100253776
'''
import random
import time
from bs4 import BeautifulSoup
from tool import getHttpUa,getCursor,getResult,getPage,isTheSame,ChangeOrNot
from tool import editeProxies,editeHeader,editeCookies,writeList,writeTXT,commHttpProxies,commHeaders,commCookies

sltDLNotCom = 'select * from dlurl1 where status=0 and tem=1 limit 5000'
SQLerror_path = 'E:/Code/Data/SQL_error.csv'

Dir_path = 'E:/Code/Data/Error/'

#txtID = ''
#currHtml = ''

csvError_path = 'csvErrorHtml_'
collError_path = 'collErrorHtml_'
advError_path = 'advErrorHtml_'
subjError_path = 'subjErrorHtml_'

#循环获取主方法
def findPage():
    httpProxies = commHttpProxies.copy()
    headers = commHeaders.copy()
    cookies = commCookies.copy()
    #从数据库中获取预访问的url列表，循环访问url地址
    http,ua = getHttpUa()#获取伪装的备选http，userAgent列表
    conn,cur = getCursor()#获取数据库连接和游标
    dlList = getResult(sltDLNotCom,cur)#返回url实体的二维数组
    #for i in range(15):
    #i = 0
    for dl in dlList:
        # this is test!!!! read from a txt
        #html = readTXT('E:/Code/Test Data/Hsinchun Chen.txt')
        #html = readTXT('E:/Code/Test Data/Yu Zheng - ACM author profile page.txt')
        #html = readTXT('E:/Code/Test Data/A. Smolic - ACM author profile page.txt')
        
        if ChangeOrNot() == True:#随机触发
            httpProxies=editeProxies(http,httpProxies)#改变http
            headers=editeHeader(ua,headers,dl['name'])#改变user agent
            cookies = editeCookies(cookies)
        time.sleep(random.randint(2, 12))#随机休眠
        
        print(str(httpProxies['https']))
        print(str(headers['User-Agent']))
        print(str(headers['Referer']))
        print(str(cookies['CFID']))
        print()
        '''
        html = str(getPage(dl['url'],httpProxies,headers,cookies))#取出url
        
        if html != ' ':
            infoSet = analysisPage(html,int(dl['id']))#分析页面
            addInfo(conn,cur,infoSet,dl)#存入数据库
            cur.execute('update dlurl1 set status=1 where id='+str(dl['id']))#标记已抽取
            conn.commit()
            print('Now is '+str(dl['id']))            
        #break#only run one time
        '''
    cur.close()
    conn.close()
    
#分析页面主方法
def analysisPage(doc,id):
    #
    infoSet = {}
    soup = BeautifulSoup(''.join(doc),"lxml")
    #aTag = soup.findAll('a')

    name,homepage,email = personInfo(soup)
    institution = historyInfo(soup)
    csvUrl = getCsvUrl(soup,doc,id)
    advUrl = getAdvisor(soup,doc,id)
    collUrl = getColleagues(soup,doc,id)
    subUrl = getSubject(soup,doc,id)
    
    infoSet['name'] = name
    infoSet['homepage'] = homepage
    infoSet['email'] = email
    infoSet['institution'] = institution
    infoSet['csvUrl'] = csvUrl
    infoSet['advUrl'] = advUrl
    infoSet['collUrl'] = collUrl
    infoSet['subUrl'] = subUrl
    
    return infoSet

#抽取个人信息
def personInfo(soup):
    #
    #情况：为空/Add personal information/只有homepage/只有邮箱/两者都有
    name = ' '
    homepage = ' '
    email = ' '

    #获取名称
    t1 = "#cccccc"
    t2 = "padding-bottom: 5px; padding-top: 5px"
    #t3 = "2"
    tdTag = soup.findAll('td',{'bgcolor':t1,'style':t2})#查找条件，使用标签的属性
    for td in tdTag:
        if td.span != None and td.span.strong != None:
            name = td.span.strong.string
            break
 
    #获取主页
    home = soup.find(text='homepage')
    if home != None:
        a = home.parent
        homepage = a['href']
    
    #获取电子邮件
    imgTag = soup.find('img',{'src':'gifs/at.gif'})
    if imgTag != None:
        div = imgTag.parent.contents
        div[0].replace('\xa0','')
        email = div[0].strip()+'@'+div[2].strip()

    return name,homepage,email

#抽取affiliation history
def historyInfo(soup):
    #
    institution = []
    strong = soup.findAll('strong')
    
    for strong in soup.findAll('strong'):
        if strong.string == 'Affiliation history':#找到目标字样
            stro = strong.nextSibling
            while stro.name != 'div':
                stro = stro.nextSibling
                if stro == None:
                    break
            
            aTag = stro.findAll('a')
            for a in aTag:
                institution.append(a.string)
            
            break
            
    return institution

#获取csv地址
def getCsvUrl(soup,doc,id):
    #
    url = ' '
    stri = soup.find(text='csv')#找到目标字样
    if stri != None:
        try:
            url = 'http://dl.acm.org/' + stri.parent['href']
        except Exception:
            print('error:'+str(stri.parent))
            path = Dir_path+csvError_path+str(id)+'.txt'
            writeTXT(path,doc)

    '''
    for a in aTag:
        if a.string != '' and a.string=='csv':
            url = 'http://dl.acm.org/'+a['href']
            break
    '''
    #print('csv'+url)
    return url

#判断role里面是否含有advisor
def getAdvisor(soup,doc,id):
    #
    url = ' '
    stri = soup.find(text='Advisor only')#找到目标字样
    if stri != None:
        try:
            url = 'http://dl.acm.org/' + stri.parent['href']
        except Exception:
            print('error:'+str(stri.parent))
            path = Dir_path+advError_path+str(id)+'.txt'
            writeTXT(path,doc)
    '''
	for a in aTag:
         if a.string != '' and a.string =='Advisor only':
             url = 'http://dl.acm.org/' + a['href']
             break
    '''    
    #print('Advisor:'+url)
    return url

#获取 colleagues 的地址
def getColleagues(soup,doc,id):
    #
    url = ' '
    stri = soup.find(text='See all colleagues of this author')#找到目标字样
    if stri != None:
        try:
            url = 'http://dl.acm.org/' + stri.parent['href']
        except Exception:
            print('error:'+str(stri.parent))
            path = Dir_path+collError_path+str(id)+'.txt'
            writeTXT(path,doc)
    '''
    for a in aTag:
        if a.string!='' and a.string!=None and 'See all colleagues' in a.string:
        #if a.string!='' and a.string=='See all colleagues of this author':
            url = 'http://dl.acm.org/' + a['href']
            break
    '''
    #print(url)
    return url

#获取 subject area的地址
def getSubject(soup,doc,id):
    #
    url = ' '
    stri = soup.find(text='See all subject areas')#找到目标字样
    if stri != None:
        try:
            url = 'http://dl.acm.org/' + stri.parent['href']
        except Exception:
            print('error:'+str(stri.parent))
            path = Dir_path+subjError_path+str(id)+'.txt'
            writeTXT(path,doc)
    '''
    for a in aTag:
		if a.string!='' and a.string == 'See all subject areas':
			url = 'http://dl.acm.org/' + a['href']
			break
    '''
    #print('subject'+url)
    return url

def addInfo(conn,cur,infoSet,dl):
    #
    #根据不同情况构造插入语句，使用拼接的方法
    if infoSet['advUrl'] != ' ':
        updateSQL = 'update dlurl1 set colleage="'+infoSet['collUrl']+'",subject="'+infoSet['subUrl']+'",papercsv="'+infoSet['csvUrl']+'",advisorcsv="'+infoSet['advUrl']+'" where id='+str(dl['id'])
    else:
        updateSQL = 'update dlurl1 set colleage="'+infoSet['collUrl']+'",subject="'+infoSet['subUrl']+'",papercsv="'+infoSet['csvUrl']+'",advisorcsv=Null where id='+str(dl['id'])
    rowSQL = updateSQL
    #判断两个姓名是否相同，不同则合并
    comName = isTheSame(infoSet['name'],dl['name'])
	
    insertSQL = 'insert into expert (name,homepage,dl) values (%s,%s,%s)'
    insertValues = (comName,infoSet['homepage'],dl['url'])
 
    try:
        cur.execute(insertSQL,insertValues)
        conn.commit()
    except Exception:
        print('insert error '+'id: '+str(dl['id']))
    
    try:
        cur.execute(updateSQL)
        conn.commit()
    except Exception:
        print('updata error'+'id: '+str(dl['id']))
        #这里专门为插入数据过长而准备的，将非常可能过长的csvurl去除，保存到单独文件中，并不存入数据库中
        try:
            writeList(SQLerror_path,[updateSQL])#保存到单独文件中
            if infoSet['advUrl'] != ' ':
                updateSQL = 'update dlurl1 set colleage="'+infoSet['collUrl']+'",subject="'+infoSet['subUrl']+'",papercsv="Too long",advisorcsv="'+infoSet['advUrl']+'" where id='+str(dl['id'])
            else:
                updateSQL = 'update dlurl1 set colleage="'+infoSet['collUrl']+'",subject="'+infoSet['subUrl']+'",papercsv="Too long",advisorcsv=Null where id='+str(dl['id'])
            cur.execute(updateSQL)
            conn.commit()
            print('another way of update is successful! Here is the papercsv url:')
            print(rowSQL)
            
        except Exception:
            print(updateSQL)
    
    addInstitution(infoSet['institution'],cur,conn)#增加机构

def addInstitution(instInfo,cur,conn):
    #
    insertSQL = ''
    result = getResult('select * from expert order by eid desc limit 1',cur)
    eid = result[0]['eid']
    
    for inst in instInfo:
        try:
            insertSQL = 'insert into experience (eid,institution) values('+str(eid)+', "'+inst+'")'
            cur.execute(insertSQL)
            conn.commit()
        except Exception:
            print('error:'+insertSQL)

   
if __name__ == '__main__':	
    findPage()