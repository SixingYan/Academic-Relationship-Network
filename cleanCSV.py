# -*- coding: utf-8 -*-
from tool import getCursor,getResult
conn,cur = getCursor()
'''
	给地址加上http前缀
'''
def cleanCSV():
	lastID = 0

	basicSQL = "select id, papercsv from dlurl1 where id>"
	sql = basicSQL + str(0) + ' limit 3000' #combine sql
	num = 0
	while True:
		#如果数量不足，则结束循环		
		result = getResult(sql,cur)
		print('now is '+str(num))
		#每一千步做一次抽取
		if len(result) > 0:
			for res in result:
				csvURL = res['papercsv']
				id = res['id']
				if csvURL.startswith('exportformats'):
					print('find it: '+str(id))
					csvURL = 'http://dl.acm.org/' + csvURL
					updateSQL = 'update dlurl1 set papercsv="'+csvURL+'" where id='+str(id)
					try:
						cur.execute(updateSQL)
						conn.commit()
						#print('done= '+updateSQL)      
					except Exception:
						print('error')
					#lastID = id

		else:
			print('all is visited')
			cur.close()
			conn.close()
			break
		#
		num += 3000
		sql = basicSQL + str(num) + ' limit 3000'

		#print(sql)
		#break#一次就退出

if __name__ == '__main__':
	cleanCSV()



























