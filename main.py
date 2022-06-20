from msilib.schema import Directory
from MinionAws.TransferManager import TransferManager
import MinionAws.CsvtoTxt as ct
import os

#initilize a session
session = TransferManager('https://sqs.us-east-1.amazonaws.com/728808211862/ICCMO.fifo','minion-data')

clean = True

while True: 
    directory = input('input directory where you want files to be stored \n')
    session.Dir = directory + '\\'
    try: 
        print('creating {} . . .'.format(session.Dir))
        os.mkdir(session.Dir)
        break
    except FileExistsError:
        print('specified directory already exists, if minion data from a previous mission/cruise is in the directory it will be deleted')
        inp = input('Would you like to continue (y/n)? ')
        if inp.lower() == 'y':
            print('cleaning')
            session.clean_dir() 
            break
        elif inp.lower() == 'n':
            continue
        else: 
            continue
while True:

    
    inp = input('\nEnter command: \n[1] Refresh \n[2] Download \n[3] Exit\n')

    if inp == '1': 
        if clean == False: 
            session.clean_dir()
        print('transfering queue to S3 . . . ') 
        session.queue_to_s3() 
        continue
    elif inp == '2':
        print('creating folder for jsons')
        session.access_s3()
        print('sbd data downloaded.')

        print('Generating CSVs')
        session.jsontocsv()

        
        for i in session.devices: 
            ct.create_files(i, session.Dir)
        continue
    elif inp == '3': 
        print('Goodbye.')
        break
    else: 
        continue
    
    
   
    
   






    

        



