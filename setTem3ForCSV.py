# -*- coding: utf-8 -*-
from tool import getCursor,getResult
conn,cur = getCursor()

def updateCSV():
	sql = 'select DISTINCT eid from paper'
	result = getResult(sql,cur)
	print('ID list is ready!')
	for res in result:
		updateSQL = 'update dlurl1 set tem=3 where id='+str(res['eid'])
		try:
			cur.execute(updateSQL)
			conn.commit()
			print('complete: '+str(res['eid']))
		except Exception:
			print('error: '+str(res['eid']))
		#break

if __name__ == '__main__':
	updateCSV()
























