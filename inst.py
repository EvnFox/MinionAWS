import os 

try: 
    os.system('python --version')
except:
    print('install python')


for i in ['pandas', 'simplekml', 'tkinter', 'numpy', 'boto3', 'datetime']:
    os.system('pip install {}'.format(i))