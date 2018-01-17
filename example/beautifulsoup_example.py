from bs4 import BeautifulSoup
import re

doc = ['<html><head><title>Page title</title></head>',
       '<body><p id="firstpara" align="center">This is paragraph <b>one</b>.',
       '<div class="inner"><ul class="award-winners-list "><a href="http://awards.acm.org/award_winners/alon_6955744.cfm">Noga Alon</a>',
       '<a href="http://awards.acm.org/award_winners/alon_6955744.cfm">Noga Alon</a>',
       '</ul></div>',
       '<p id="secondpar" align="blah" class = "award-winners-list ">This is paragraph <b>two</b>.',
       '<p id="secondpara" align="blah">This is paragraph <b>two</b>.',
       '</html>']

soup = BeautifulSoup(''.join(doc),"lxml")

#print (soup.prettify())

#len(soup('p'))

#list = soup.find('ul',class="award-winners-list ")


soup.find('ul')
#Out[10]: <ul class="award-winners-list "><a href="http://awards.acm.org/award_winners/alon_6955744.cfm">Noga Alon</a></ul>

soup.find('ul').a.string
#Out[12]: 'Noga Alon'

soup.find('ul').a
#Out[11]: <a href="http://awards.acm.org/award_winners/alon_6955744.cfm">Noga Alon</a>

soup.find('ul').a['href']
#Out[14]: 'http://awards.acm.org/award_winners/alon_6955744.cfm'

soup.find('ul')['class']
#Out[15]: ['award-winners-list', '']

def readList(path):
    List = []
    with open(path, newline = '') as f:
        reader = csv.reader(f)        
        for row in reader:
            List.append(row[0])
        return List
def readHtml(path):
	#
	fp = open(path,'r')
	docs = fp.readlines()
	fp.close()

	return docs

def initialSoup(docs):
	#
	soup = BeautifulSoup(''.join(docs),"lxml")
	return soup


