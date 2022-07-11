import os 

try: 
    os.system('python --version')
except:
    print('install python')

try: 
    os.system('pip --version')
except: 
    print('install pip')
    
for i in ['pandas', 'simplekml', 'tkinter', 'numpy', 'boto3', 'datetime', 'glob']:
    os.system('pip install {}'.format(i))