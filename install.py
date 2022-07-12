import os 


# Check that python is installed 
try: 
    os.system('python --version')
except:
    print('install python')
    exit()

# Check that pip is installed 
try: 
    os.system('pip --version')
except: 
    print('install pip')
    exit()

# Check that the aws cli is installed
try: 
    os.system('aws')
except: 
    print('you need to install aws cli\nrefer to readme.md')
    exit()

# Install python packages that are needed
for i in ['pandas', 'simplekml', 'numpy', 'boto3', 'datetime', 'glob']:
    os.system('pip install {}'.format(i))

os.system('aws configure')