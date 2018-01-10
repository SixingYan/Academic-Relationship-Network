# -*- coding: utf-8 -*-
'''
'''
#import os;os.chdir('e:/Code/Python');import insertName;insertName.mainFunction()
from tool import readSeriz,getCursor
conn,cur = getCursor()
NO = '26'
similarity_pickle_path = 'E:/Code/Data/pickle/similarity_pickle_'+NO+'.pickle'
GN_Amount_pickle_path = 'E:/Code/Data/pickle/GN_Amount_pickle_'+NO+'.pickle'
GN_pickle_path = 'E:/Code/Data/pickle/GN_pickle_'+NO+'.pickle'
def insertSimilarity():
	#
	similarity = readSeriz(similarity_pickle_path)
	#print(str(GN))
	i = 0
	for tp in similarity:
		#print(str(tp))
		xid = tp[0]
		yid = tp[1]
		rate = tp[2]
		#tag = int(tp[2]*100)
		insertSQL = 'insert into namecompare (xid,yid,nameRate) values ('+str(xid)+','+str(yid)+','+str(rate)+')'
		cur.execute(insertSQL)
		conn.commit()
		print('completed '+str(xid)+','+str(yid)+','+str(rate))
		print('now is '+str(i))
		i+=1
		#break

def insertGN_Amount():
	#
	GN_Amount = readSeriz(GN_Amount_pickle_path)
	#print(str(GN))
	i = 0
	for tp in list(GN_Amount.keys()):
		#print(str(tp))
		amount = GN_Amount[tp]
		insertSQL = 'insert into namegroup (gid,amount) values ('+str(tp)+','+str(amount)+')'
		cur.execute(insertSQL)
		conn.commit()
		print('completed '+str(tp)+','+str(amount))
		print('now is '+str(i))
		i+=1
		#break

def insertGN():
	#
	GN = readSeriz(GN_pickle_path)
	for gid in GN.keys():
		yidList = GN[gid]
		for yid in yidList:
			insertSQL = 'insert into name2name (gid,yid) values ('+str(gid)+','+str(yid)+')'
			cur.execute(insertSQL)
			conn.commit()
		print('now is '+str(gid))
		#break

def mainFunction():
	#insertSimilarity()
	#insertGN_Amount()
	insertGN()
if __name__ == '__main__':
	insertSimilarity()

'''
Table: new_table
Columns:
id int(11) AI PK 
xid int(11) 
yid int(11) 
tag int(11) 
nameThreashold float 
nameRate float 
flag int(11) 
paper float 
coauthor float 
institu float 
topic float 
status int(11) 
tem int(11)


Table: namecompare
Columns:
id int(11) AI PK 
xid int(11) 
yid int(11) 
tag int(11) 
nameThreashold float 
nameRate float 
flag int(11) 
paper float 
coauthor float 
institu float 
topic int(11) 
status int(11) 
tem int(11)

Table: namegroup
Columns:
id int(11) AI PK 
gid int(11) 
amount int(11) 
status int(11) 
tem int(11)

'''





















