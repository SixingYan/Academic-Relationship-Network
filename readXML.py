# -*- coding: utf-8 -*-
from tool import readTXT
from bs4 import BeautifulSoup

if __name__ == '__main__':
    path = 'e:/test.xml'
    xml = readTXT(path)
    #print(xml)
    if xml.find('<全部同伴的名字>')>-1:
        ind = xml.find('<全部同伴的名字>')
        xml = xml[:ind]
    else:
        print('error')
    
    soup = BeautifulSoup(''.join(xml),"lxml")
    a = soup.find('a',{'name':"collab"})
    try:
        divAb = a.parent.parent
        tr = divAb.table.tr
        for td in tr.findAll('td'):
            for div in td.findAll('div'):
                if div.a.string != None:
                    print(div.a.string)
                    url = 'http://dl.acm.org/' + div.a['href']
                    print(url)
    except Exception:
        print('error')