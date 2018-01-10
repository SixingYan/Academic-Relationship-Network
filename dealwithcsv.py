# -*- coding: utf-8 -*-
"""
Created on Fri Mar 17 09:39:41 2017

@author: Sixing
"""

from tool import readTXT,writeTXT
expertList_path = 'E:/Code/Data/subUrlSupply last 3.csv' #源列表
#import os;os.chdir('e:/Code/Python');import dealwithcsv;dealwithcsv.mainFunction()
def mainFunction():
	string = open(expertList_path,'r',encoding= 'utf-8').read()
	#string = string.replace('\"','').encode('utf-8')
	string = string.replace('\"','')
	fp = open(expertList_path, 'w',newline='',encoding='utf-8')
	fp.write(string)
	fp.close()
if __name__ == '__main__':
    
	mainFunction()
