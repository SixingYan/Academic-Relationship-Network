# -*- coding: utf-8 -*-
'''
	simrank
'''
import networkx
from collections import defaultdict
import copy
#import os;os.chdir('e:/Code/Python');import simrank_example
def simrank(G, r=0.9, max_iter=100):
	# init. vars
	sim_old = defaultdict(list) #默认值是list的字典
	sim = defaultdict(list) #默认值是list的字典
	for n in G.nodes():
		sim[n] = defaultdict(int) #默认值是int的字典
		sim[n][n] = 1
		sim_old[n] = defaultdict(int) #默认值是int的字典
		sim_old[n][n] = 0

	# recursively calculate simrank
	for iter_ctr in range(max_iter):
		if _is_converge(sim, sim_old):
			break
		sim_old = copy.deepcopy(sim)
		for u in G.nodes():
			for v in G.nodes():
				if u == v:
					continue
				s_uv = 0.0
				for n_u in G.neighbors(u):
					for n_v in G.neighbors(v):
						s_uv += sim_old[n_u][n_v]
				sim[u][v] = (r * s_uv / (len(G.neighbors(u)) * len(G.neighbors(v))))
	return sim

def _is_converge(s1, s2, eps=1e-4):
	for i in s1.keys():
		for j in s1[i].keys():
			if abs(s1[i][j] - s2[i][j]) >= eps:
				return False
	return True

G = networkx.Graph()
G.add_edges_from([('a','b'), ('b', 'c'), ('c','a'), ('c','d')])
sim = simrank(G)
print(sim)

