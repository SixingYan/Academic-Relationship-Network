# -*- coding: utf-8 -*-
'''
	将topic表中的id与dlurl1中的id对应，set status=2
'''

selectSQL = 'select distinct eid from topic'
#import os;os.chdir('e:/Code/Python');import linkTpcToDL;linkTpcToDL.linkP2D()
from tool import getResult,getCursor

def linkP2D():
    #
    conn,cur = getCursor()
    resuID = getResult(selectSQL,cur)
    for re in resuID:
        updateSQL = 'update dlurl1 set status=2 where id='+str(re['eid'])
        try:
            cur.execute(updateSQL)
            conn.commit()
            print('complete: '+str(re['eid']))
        except Exception:
            print('error: '+str(re['eid']))
            
    cur.close()
    conn.close()
if __name__ == '__main__' :
    linkP2D()