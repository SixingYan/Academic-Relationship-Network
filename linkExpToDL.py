# -*- coding: utf-8 -*-
'''
	将experience1中已有的eid对应到dlurl1表 tem=5
'''

selectSQL = 'select distinct eid from experience1'
#import os;os.chdir('e:/Code/Python');import linkExpToDL;linkExpToDL.linkP2D()
from tool import getResult,getCursor

def linkP2D():
	#
	conn,cur = getCursor()
	resuID = getResult(selectSQL,cur)
	for re in resuID:
		updateSQL = 'update dlurl1 set tem=5 where id='+str(re['eid'])
		cur.execute(updateSQL)
		conn.commit()
		print('complete: '+str(re['eid']))
		
if __name__ == '__main__':
	linkP2D()
























