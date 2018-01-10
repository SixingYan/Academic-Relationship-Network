# -*- coding: utf-8 -*-
'''
	将isntitution表中的id与dlurl1中的id对应, tem=3
'''

selectSQL = 'select distinct eid from paper'

from tool import getResult,getCursor

def linkP2D():
	#
	conn,cur = getCursor()
	resuID = getResult(selectSQL,cur)
	for re in resuID:
		updateSQL = 'update dlurl1 set tem=3 where id='+str(re['eid'])
		cur.execute(updateSQL)
		conn.commit()
		print('complete: '+str(re['eid']))
		
if __name__ == '__main__':
	linkP2D()