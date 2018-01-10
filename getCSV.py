
'''
填充，status更新为1
逐个抽取collea,status 更新为0
逐个抽取csv，status更新为1
'''
from tool import getHttpUa,getCursor,getResult,ChangeOrNot,editeProxies,editeHeader,commHttpProxies,commHeaders
import requests

import time
import random
sltDLNotCom = 'select * from dlurl1 where id>2561 and id<12000 and papercsv<>"Too long" '
file_path = 'E:/Code/Data/csv/paperlist_'

#主方法
def findCSV():
    httpProxies = commHttpProxies
    headers = commHeaders
    
    http,ua = getHttpUa()#获取伪装的备选http，userAgent列表
    conn,cur = getCursor()#获取数据库连接和游标
    dlList = getResult(sltDLNotCom,cur)#返回url实体的二维数组

    for dl in dlList:
        if ChangeOrNot() == True:#随机触发
            httpProxies=editeProxies(http,httpProxies)#改变http
            headers=editeHeader(ua,headers)#改变user agent
        time.sleep(random.randint(1, 12))#随机休眠
        
        url = dl['papercsv']

        if url != None and len(url)> 15:
            try:
                r = requests.get(url, proxies = httpProxies, headers = headers, timeout=30)
                if r.status_code == 200:
                    csv_path = file_path+str(dl['id'])+'.csv'
                    with open(csv_path,'wb') as csv:
                        csv.write(r.content)
                        print('Now is '+str(dl['id']))
            except requests.RequestException as e:
                print(e)
                httpProxies=editeProxies(http,httpProxies)#改变http
                headers=editeHeader(ua,headers)#改变user agent
                time.sleep(random.randint(1, 12))#随机休眠
                try:
                    r = requests.get(url, proxies = httpProxies, headers = headers, timeout=30)
                    if r.status_code == 200:
                        csv_path = file_path+str(dl['id'])+'.csv'
                        with open(csv_path,'wb') as csv:
                            csv.write(r.content)
                            print('Now is '+str(dl['id']))
                except Exception:
                    print('another try is failed! id:'+str(dl['id']))
        #break# only run one time
    
    cur.close()
    conn.close()        

if __name__ == '__main__':	
    findCSV()


