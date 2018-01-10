'''
	合并一些可以确定的
	update 表 a1 set a1.d=(select d from ((select d from 表 where k23=3) as a2)) where a1.k23=1;
	(1)所有姓名在0.8以上的，机构在0.5以上的
	'select xid,yid from name2compare where institu>-1 and institu<0.5 and paper>0.2 and nameRate>0.6'
	'select xid,yid from name2compare where institu>-1 and institu<0.5 and paper<0.2 and coauthor>0.01 and nameRate>0.6'
	(2)机构在0.5以下，paper在0.2以上的
	(3)
'''
from tool import getCursor,getResult
conn,cur = getCursor()
#import os;os.chdir('e:/Code/Python');import geng_combine;geng_combine.combine()
def combine():
	#
	#selectSQL = 'select id,xid,yid from name2compare where institu>0.5 and nameRate>0.75'
	#selectSQL = 'select id,xid,yid from name2compare where institu>-1 and institu<0.5 and paper>0.2 and nameRate>0.6'
	#selectSQL = 'select id,xid,yid from name2compare where paper<0.2 and paper>0 and coauthor >0.01 and nameRate>0.6'
	#selectSQL = 'select id,xid,yid from name2compare where coauthor<0.01 and topic>0.06 and nameRate>0.6'
	selectSQL = 'select id,xid,yid from name2compare where institu<0.1 and institu>-1 and paper>=0 and coauthor>0'
	
	selectResult = getResult(selectSQL,cur)
	i = 0
	total= len(selectResult)
	for sr in selectResult:
		i+=1
		fg = updateCombine(sr['xid'],sr['yid'])
		if fg:
			print('completed: '+str(round(i/total,3))+' ||  id:'+str(sr['id']))
		else:
			print('somewhere error: '+str(round(i/total,3))+' ||  id:'+str(sr['id']))
		#break
		

def updateCombine(xid,yid):
	#
	flag1 = True
	flag2 = True
	flag3 = True
	flag4 = True
	updateSQL = 'update dlurl1 set id='+str(xid)+' where id='+str(yid)
	flag4 = updateSQLCommit(updateSQL)
	
	updateSQL = 'update paper set eid='+str(xid)+' where eid='+str(yid)
	flag1 = updateSQLCommit(updateSQL)
	updateSQL = 'update topic set eid='+str(xid)+' where eid='+str(yid)
	flag2 = updateSQLCommit(updateSQL)
	updateSQL = 'update experience1 set eid='+str(xid)+' where eid='+str(yid)
	flag3 = updateSQLCommit(updateSQL)
	
	return flag1 * flag2 * flag3 * flag4

def updateSQLCommit(updateSQL):
	#
	flag = False
	try:
		cur.execute(updateSQL)
		conn.commit()
		flag = True
	except Exception:
		print('update error: '+updateSQL)

	return flag

def cleanPaperInstituNull():
	#
	selectResult = getResult('select distinct id from dlurl1',cur)
	i = 0
	total= len(selectResult)
	for sr in selectResult:
		i += 1
		result = getResult('select id from paper where eid='+str(sr['id']),cur)
		if not len(result)>0:
			#delete
			cur.execute('delete from dlurl1 where id='+str(sr['id']))
			conn.commit()
			cur.execute('delete from experience1 where eid='+str(sr['id']))
			conn.commit()
			cur.execute('delete from topic where eid='+str(sr['id']))
			conn.commit()
			print('deleted: '+str(round(i/total,3))+' ||  id:'+str(sr['id']))
			continue

		result = getResult('select id from experience1 where eid='+str(sr['id']),cur)
		if not len(result)>0:
			#delete
			cur.execute('delete from dlurl1 where id='+str(sr['id']))
			conn.commit()
			cur.execute('delete from paper where eid='+str(sr['id']))
			conn.commit()
			cur.execute('delete from topic where eid='+str(sr['id']))
			conn.commit()
			print('deleted: '+str(round(i/total,3))+' ||  id:'+str(sr['id']))
			continue

		print('completed check: '+str(round(i/total,3))+' ||  id:'+str(sr['id']))
		#break
if __name__ == '__main__':
	combine()








