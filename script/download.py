import json

import time
import urllib.request

userlistfile = open('user.ratedList.json',encoding = 'utf8')
userlist = json.load(userlistfile)

print(len(userlist['result']))

#proxy = urllib.request.ProxyHandler( {'http': '10.10.78.62:3128'} )
#opener = urllib.request.build_opener( proxy )
#urllib.request.install_opener(opener)

cnt = 0
flag = False
for user in userlist['result']:
    handle = user['handle']
    if handle == 'NaoQv': # Started from 'delta_mj'
        flag = True
    if handle == 'headchef':
        flag = False
    if flag:
        url = 'http://codeforces.com/api/user.status?handle=' + handle
        print('Downloading from ' + url)
        while True:
            repeat = False
            try:
                urllib.request.urlretrieve(url, 'data/' + handle + '.json')
            except:
                print('  Some problem downloading...')
                repeat = True
                time.sleep(0.2)
            if not repeat:
                break
        cnt = cnt + 1
        time.sleep(0.2)
        #if cnt > 10:
        #    break
        


