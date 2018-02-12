'''
	如何建立网
	判断是否是强连通图
	深度遍历
'''
	
	'select xid,yid,score from colleaguenet where score <>0'
import networkx as nx
G=nx.Graph()

G.add_node(1) #这个1只是编号，不是数量

G.add_nodes_from([2,3]) #2和3之间有条边



if nx.is_connected(G): #return True or False
	
	
	









