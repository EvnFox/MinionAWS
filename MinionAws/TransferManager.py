import boto3
import botocore
import pandas as pd
import os 
import json
import glob
import shutil
from datetime import datetime



class TransferManager: 
    '''
    This class handles Iridium SBD data contained in jsons on a SQS server and transfers it to S3 storage for later cleaning
    it also facilitates accessing the S3 data once it is saved. 

    qname: name to SQS queue 

    sname: name to S3 storage

    self.Dir needs to be set to a directory. 
    '''

    def __init__(self, qname, sname):

        self._s3 = boto3.resource('s3')
        self._sqs = boto3.resource('sqs')

        self.Dir = ''

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
            print("Unanble to access AWS")
    def unix_time(self, time):
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
     
    def clean_dir(self):
        print('cleaning set directory')

        dels = os.listdir(self.Dir)
        for i in dels:
            if i == 'sbd_' or i[0:4] == 'txt_' or i == 'csv':
                shutil.rmtree(self.Dir + i)


    # Resevered for future use
    def set_credentials(self, usr, pas): 
        try:
            os.system('aws credentials')
        except:
            print('placeholder')

    
    def queue_to_s3(self):
        # take items in queue and save to s3 obj  
        
       
        while True: 
            self.messages = self.queue.receive_messages(WaitTimeSeconds = 20, MaxNumberOfMessages = 10, VisibilityTimeout = 10)
            
            if self.messages != []:
                
                for message in self.messages: 
                    try: 
                        self.bucket.put_object(Body = message.body, Key = str(message.message_id))
                        self.successful_connections += 1
                        print('uploaded to s3')
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

        self.create_dir('sbd', '\\' )
        
        if self.bucket is not None:
             
            paginator = boto3.client('s3').get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.sname)

            for page in page_iterator:
                for j in range(len(page['Contents'])):
                    item = page['Contents'][j]['Key']
                    try:
                        #  str(len(page['Contents']) - j) +
                        self.bucket.download_file(item, '{0}\\{1}'.format(self.Dir + 'sbd_', item))
                        
                    except botocore.exceptions.ClientError as e:

                        if e.response['Error']['Code'] == "404":

                            print("The object does not exist.")
                        else:
                            raise

    def create_dir(self, type, IMEI):
        try: 
            os.mkdir(self.Dir + "\\{}_".format(type) + str(IMEI))
            print("created \\{}_".format(type) + str(IMEI))
            
        except: 
            print("directory already exists")

    def jsontocsv(self):

        path = self.Dir + "\\csv"
        try: 
            os.mkdir(path)
            print('directory created')
        except:
            print("csv directory already exists")
        # Get messages 
        try: 
            messages = glob.glob(self.Dir + '\\sbd_\\*')
        except: 
            print('Failed to load directory')
            exit()
        # Save messages as CSV

        for message in messages:
            try:
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
            file_path = path + "\\" + file_name

            df = pd.DataFrame(arr)
            df.T
            #print(df.iloc[0])
            try: 
                x = pd.read_csv(self.Dir + '\\csv\\imei_' + IMEI  + '.csv')
                df = pd.concat([x,df], axis = 1)
                #
                df.to_csv(file_path, encoding='utf-8', index = False)
            except:
                df.to_csv(file_path, mode = 'a', encoding='utf-8', index=False)

        
    


