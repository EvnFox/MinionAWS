import os 

try: 
    os.system('python --version')
except:
    print('install python')
    exit()
try: 
    os.system('pip --version')
except: 
    print('install pip')
    exit()
try: 
    os.system('aws')
except: 
    print('you need to install aws cli\nrefer to readme.md')
    exit()

for i in ['pandas', 'simplekml', 'tkinter', 'numpy', 'boto3', 'datetime', 'glob']:
    os.system('pip install {}'.format(i))

os.system('aws configure')