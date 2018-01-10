'''
	从.csv文件中抽取机构列表
'''
import re
from tool import getCursor,readList,readFiles
files_path = 'E:/Code/Data/institution new2'

conn,cur = getCursor()
#import os;os.chdir('e:/Code/Python');import extractInstitution_csv;extractInstitution_csv.mainFunction()
def cleanInstit(instit):
    #
    institNew = ''
    for inst in instit.split(' '):
        institNew += re.sub('[^a-zA-Z]','',inst)+' '
    return institNew.strip()

def mainFunction():
	#
	filesPath = readFiles(files_path)
	
	for fileP in filesPath:
		eid = fileP.split('inst_')[1].replace('.csv','')
		institList = readList(fileP)

		for inst in institList:
			if len(inst)<4:
				break
			inst = cleanInstit(inst)
			insertSQL = 'insert into experience1 (eid,institution) values('+str(eid)+',"'+inst+'")'
			try:
				cur.execute(insertSQL)
				conn.commit()
				#print('complete:' +str(eid))
			except Exception:
				print('insert error'+str(eid))
		print('complete:' +str(eid))
		cur.execute('update dlurl1 set tem=5 where id='+str(eid))
		conn.commit()
		#break #only once

	cur.close();conn.close()
	
if __name__ == '__main__':
    mainFunction()


