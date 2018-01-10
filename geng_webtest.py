# -*- coding: utf-8 -*-
from selenium import webdriver
import time
from tool import writeTXT,readSeriz
#import os;os.chdir('e:/Code/Python');import geng_webtest;geng_webtest.mainFunction()
paperDict_pickle = 'E:/Code/experience/pickle/acceptedpaper 2013.pickle'
#path = 'E:/Code/experience/pickle/page.txt'
#paper_path = ''
html_path = 'E:/Code/experience/paper 2013/'
def cleanKw(keyword):
	keyword = keyword.replace('\\',' ').replace('/',' ').replace('?',' ').replace('\t',' ').replace('\n',' ')
	keyword = keyword.replace(':',' ').replace('<',' ').replace('>',' ').replace('|',' ').replace('\"',' ').replace('*',' ')
	return keyword

def searchPage(browser,keyword):
	#
	#'D:/Program Files/Mozilla Firefox/firefox.exe'
	browser.get('http://xueshu.baidu.com/')
	#browser.get(url)
	time.sleep(3)
	#browser.find_element_by_id('kw').send_keys('Predicting Future Scientific Discoveries Based on a Networked Analysis of the Past Literature')
	browser.find_element_by_id('kw').send_keys(keyword)
	time.sleep(3)
	browser.find_element_by_id('su').click()
	page = str(browser.page_source)
	#print(page)
	#keyword = cleanKw(keyword)
	#path = html_path+keyword+'.html'
	#writeTXT(path,page)
	time.sleep(1)

def mainFunction():
	#
	#获得要查询的列表
	browser = webdriver.Chrome()
	paperDict = readSeriz(paperDict_pickle)
	for title in paperDict.keys():
		try:
			searchPage(browser,title)
			print('completed: '+title)
		except Exception:
			print('error: '+title)
		#break
	browser.quit()

if __name__ == '__main__':
	mainFunction()