import boto3
import botocore
import pandas as pd
import os 
import json
import glob
import shutil
from datetime import datetime

import platform
if platform.system() == 'Windows':
    _SLASH = '\\'
else: 
    _SLASH = '/'
class TransferManager: 
    '''
    This class handles Iridium SBD data contained in jsons on a SQS server and transfers it to S3 storage for later cleaning
    it also facilitates accessing the S3 data once it is saved. 

    qname: name to SQS queue 

    sname: name to S3 storage

    self.Dir needs to be set to a directory can be local or total path, defult is 'defult_dir', which will be located in the same dir of the 
    python script. 
    '''

    def __init__(self, qname, sname):

        self._s3 = boto3.resource('s3')
        self._sqs = boto3.resource('sqs')

        self.Dir = 'defult_dir'

        self.data = None
        self.bucket = None
        self.devices = []
        self.messages = []
        self.IMEI = None
        self.failed_connections = 0; self.successful_connections = 0
        self.mt = 0

        
       
        self.qname = qname
        self.sname = sname

        try:
            self.queue = self._sqs.Queue(qname)
            self.bucket = self._s3.Bucket(sname)
        except: 
            print("Unable to access AWS")
    # puts time into unix time so that it can be sorted correclty 
    def unix_time(self, time : str):
        #012345678912345678912
        #6/16/2022  2:14:02 PM
        if time[19:21] == 'PM':
            try: 
                time = time[0:11]+ str(int(time[11:13]) + 12) + time[13:19]
            except:
                
                time = time[0:11]+ str(int(time[11:12]) + 12) + time[12:19]
           
        dt_tuple=tuple([int(x) for x in time[:10].split('-')])+tuple([int(x) for x in time[11:].split(':')])
        d = datetime(dt_tuple[0],dt_tuple[1], dt_tuple[2], dt_tuple[3], dt_tuple[4], dt_tuple[5])
        date = (d.timestamp())
        return str(date)
    # cleans directory that will have files created
    def clean_dir(self):
        print('cleaning set directory')

        dels = os.listdir(self.Dir)
        for i in dels:
            if i == 'sbd_' or i[0:4] == 'txt_' or i == 'csv':
                
                shutil.rmtree(self.Dir + i)
            elif i[0:10] == 'inaccurate':
                os.remove(self.Dir+i)
               


    # Resevered for future use
    def set_credentials(self, usr, pas): 
        try:
            os.system('aws credentials')
        except:
            print('placeholder')

    
    def queue_to_s3(self):
        # take items in queue and save to s3 obj  
        
       
        while True: 
            # Get messages
            self.messages = self.queue.receive_messages(WaitTimeSeconds = 20, MaxNumberOfMessages = 10, VisibilityTimeout = 10)
            
            if self.messages != []:
                
                for message in self.messages: 
                    try: 
                        #get IMEI so that the message is saved to the correct folder on S3
                        data = json.loads(message.body)
                        header = data['data']['mo_header']
                        imei = header['imei']

                        # Save obj to S3
                        self.bucket.put_object(Body = message.body, Key = 'imei_' + imei +'/'+str(message.message_id))
                        self.successful_connections += 1
                        print('uploaded to s3')

                        # Remove message from SQS
                        message.delete()
                    except: 
                        self.failed_connections += 1
                        print('error uploading to s3')
                        raise
                self.mt = 1

            else: 
                
                print("There are no new messages\n")
                print("Succesfull Connections: {0} \nFailed Connections: {1}".format(self.successful_connections, self.failed_connections))
                self.mt = 2
                break
        

    def access_s3(self):
        
        # Create directory to store SBD data
        self.create_dir('sbd', _SLASH )
        
        if self.bucket is not None:
            
            # Creates paginator obj that allows us to automate looping through everything in S3
            # handles mulitple pages
            paginator = boto3.client('s3').get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.sname)

            for page in page_iterator:
                for j in range(len(page['Contents'])):
                    # Get the list of keys
                    item = page['Contents'][j]['Key']
                    try:
                        
                        # Downloads items
                        if item[21:] == '':
                            continue
                        if item[0:7] == 'archive':
                            continue
                        
                        #print(item[21:])
                        self.bucket.download_file(item, '{0}{1}{2}'.format(self.Dir + 'sbd_', _SLASH,item[21:]))
                        
                    except botocore.exceptions.ClientError as e:

                        if e.response['Error']['Code'] == "404":

                            print("The object does not exist.")
                        else:
                            raise

    def create_dir(self, type : str, IMEI):
        try: 
            os.mkdir(self.Dir + _SLASH +"{}_".format(type) + str(IMEI))
            print("created \\{}_".format(type) + str(IMEI))
            
        except: 
            print("directory already exists")

    def jsontocsv(self):

        path = self.Dir + _SLASH + "csv"
        try: 
            os.mkdir(path)
            print('directory created')
        except:
            print("csv directory already exists")
        # Get messages 
        try: 
            # Get the sbd directory, this returns everythin in arbitrary order so it has to be resorted later.
            messages = glob.glob(self.Dir +_SLASH +'sbd_{}*'.format(_SLASH))
        except: 
            print('Failed to load directory')
            exit()
        # Save messages as CSV

        for message in messages:
            try:
                # Load the data
                with open(message, 'r') as f:
                    self.data = json.load(f)
                f.close()  #print(data)
            except: 
                print('failed to process json')
                raise

            header = self.data['data']['mo_header']
            location = self.data['data']['location_information']

            try:
                payload = self.data['data']['payload']
            
                if header['imei'] not in self.devices:
                    self.devices.append(header['imei'])       
            except: 
                # We are here if there was no payload
                payload = 'NaN'

            print(header['session_status'])

            utime = self.unix_time(header['time_of_session'])

            arr = [header['imei'], utime, header['session_status'], location['latitude'], location['longitude'], payload]
                
            IMEI = header['imei']
            file_name = "imei_" + IMEI + ".csv"
            file_path = path + _SLASH + file_name

            df = pd.DataFrame(arr)
            df.T
            #print(df.iloc[0])
            try: 
                x = pd.read_csv(self.Dir + _SLASH + 'csv{}imei_'.format(_SLASH) + IMEI  + '.csv')
                df = pd.concat([x,df], axis = 1)
                #
                df.to_csv(file_path, encoding='utf-8', index = False)
            except:
                df.to_csv(file_path, mode = 'a', encoding='utf-8', index=False)
    
    def archive(self, imei : str, name : str):

        if self.bucket is not None:
            
            # Creates paginator obj that allows us to automate looping through everything in S3
            # handles mulitple pages
            client = boto3.client('s3')
            paginator = client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.sname)

            #client.put_object(Bucket=self.sname, Key='archive_{}_{}/'.format(name,imei))

            for page in page_iterator:
                for j in range(len(page['Contents'])):
                    # Get the list of keys
                    item = page['Contents'][j]['Key']
                    if item[0:20] == 'imei_'+ str(imei): 
                        new_id = 'archive_{}_{}/'.format(name, imei) + item[21:]
                        #rename file
                        try:
                        
                            response = client.copy_object(Bucket=self.sname,CopySource=self.sname +'/'+item, Key=new_id)
                            print(response)
                        except:
                            print('Problem copying to archive')
                            raise
                        #handle response
                        #this can only happen if copy is sucsessful
                        delete_req = client.delete_object(Bucket=self.sname,Key=item)
                        print(delete_req)
        ## loop through s3 
        ## if message id starts with imei_<imei> 
        ## message id = 'archive_' + name + message_id
        ##
        ## in self.Dir rename txt_<imei> --> name_txt_<imei>
        
        

    ## ^^^ this is going to need an undo method so that the effects can be reversed. 
    def unarchive(self, imei : str, name : str):

        if self.bucket is not None:
            
            # Creates paginator obj that allows us to automate looping through everything in S3
            # handles mulitple pages
            client = boto3.client('s3')
            paginator = client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.sname)


            for page in page_iterator:
                for j in range(len(page['Contents'])):
                    # Get the list of keys
                    item = page['Contents'][j]['Key']
                    k = 'archive_' + name + '_' + imei
                    if item[0:len(k)] == k: 
                        new_id =  'imei_'+ imei + '/' +item[len(k)+2:]
                        #rename file
                        try:
                        
                            response = client.copy_object(Bucket=self.sname,CopySource=self.sname +'/'+item, Key=new_id)
                            print(response)
                        except:
                            print('Problem copying to archive')
                            raise

                        #handle response
                        #this can only happen if copy is sucsessful
                        delete_req = client.delete_object(Bucket=self.sname,Key=item)
                        print(delete_req) 
        else: 
            print('S3 bucket not set by constructor')