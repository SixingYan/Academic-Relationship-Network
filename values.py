cookiePairs=[
	[716361429,70843524],
	[804126486,91182901],
	[887589065,66576478],
	[569750122,89888184],
	[451513619,23451315]
			]

#改变referer
def randomReferer(word,choice):
	#input two elements of list
	if choice==0:
		return 'https://www.baidu.com/s?ie=UTF-8&wd='+word[0]+'%20'+word[1]
	else if choice==1:
		return 'https://www.google.com/?gws_rd=ssl#q='+word[0]+'+'+word[1]
	else if choice==2:
		return 'http://cn.bing.com/search?q='+word[0]+'+'+word[1]+'&qs=n&form=QBLH&sp=-1&pq='+word[0]+'+'+word[1]+'&sc=8-15&sk=&cvid=F90E87E078CF42929C149AACD841CB46'
	else:
		return 'https://dl.acm.org/results.cfm?query='+word[0]+'+'+word[1]+'&Go.x=30&Go.y=11'

#改变cookies
def editeCookies(cookies):
	#
	if ChangeOrNot==True:
		choice =  ramdon.choice(cookiePairs)
		cookies['CFID']=choice[0]
		cookies['CFTOKEN']=choice[1]
		cookies['DEEPCHK']='1'
	else:
		cookies['CFID']=''
		cookies['CFTOKEN']=''
		cookies['DEEPCHK']=''
	return cookies

'''
https://www.google.com
https://www.google.com
https://www.baidu.com/s?ie=UTF-8&wd=digital%20library
https://www.google.com/?gws_rd=ssl#q=digital+library
http://cn.bing.com/search?q=digital+library&qs=n&form=QBLH&sp=-1&pq=digital+library&sc=8-15&sk=&cvid=F90E87E078CF42929C149AACD841CB46
'''