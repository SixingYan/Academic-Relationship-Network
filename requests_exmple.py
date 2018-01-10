# -*- coding: utf-8 -*-
"""

http://blog.csdn.net/alpha5/article/details/24964009
python requests 的安装与简单运用

"""

import requests
r = requests.get('http://www.zhidaow.com')  # 发送请求
r.status_code  # 返回码 
r.headers['content-type']  # 返回头部信息
r.encoding  # 编码信息
r.encoding = 'utf-8'  # 设置编码信息
r.text
r.content