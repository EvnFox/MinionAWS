# This main file controls the program that interacts with sqs and s3 server as well as 
# handeling the download and creation of files
#
#####################################################
from msilib.schema import Directory
import simplekml
from numpy import empty
from MinionAws.TransferManager import TransferManager
import MinionAws.CsvtoTxt as ct
import os

#initilize a session
session = TransferManager('https://sqs.us-east-1.amazonaws.com/728808211862/ICCMO.fifo','minion-data')
kml = simplekml.Kml()
clean = True

while True: 
    #Get directory from user
    directory = input('input directory where you want files to be stored \n')
    #set the session dir
    if directory[-1] != '\\':
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
# This is the main loop of the program
while True:

    
    inp = input('\nEnter command: \n[1] Refresh \n[2] Download \n[3] Archive \n[4] Unarchive \n[5] Exit\n')

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

        try:
            # Try to generate KML files
            for i in session.devices: 

                location_data = ct.create_files(i, session.Dir)
                if location_data != []:
                    lin = kml.newlinestring(name='imei_{}'.format(i),
                                            coords=location_data)
                    kml.save('{}\\location.kml'.format(session.Dir))
        except: 
            for i in session.devices: 
                ct.create_files(i, session.Dir)
            #create new line
        

    elif inp == '3':
        if session.devices == []:
            print('No devices in session, to archive you first must download ([2])')
            continue

        print('Active IMEIs in session:\n')
        for i in range(len(session.devices)):
            print('[{}] imei_{}\n'.format(i, session.devices[i]))
        device = input('pleae input the devivce index ({} to {})\n'.format(0, len(session.devices)-1))

        device = int(device)
        if device < 0 or device > len(session.devices) - 1: 
            print('Invalid Entry')
            continue

        name = input('name of mission\n')

        try:
            os.rename(session.Dir + '\\txt_' + str(session.devices[device]), session.Dir + '\\' + name +'_txt_' + str(session.devices[device]))
        except: 
            print('mission name already in use' )
            continue
        
        session.archive(session.devices[device], name)
        #dev = input(session.devices)  < -- dev is the imei of the device we wish to archive
        #name = input('What would you like to call the archived file)
        #session.archive(dev, name)

        #imei_<imei>/<messageid> |--> archive_name_imei_<imei>/<messageid>
    
    elif inp == '4': 
        imei = input('Enter imei to unarchive')
        name = input ('Enter name of mission')

        try:
            os.rename(session.Dir + '\\' + name +'_txt_' + imei, session.Dir + '\\txt_' + imei )
        except: 
            print('mission name doesnt exist' )
            continue

        session.unarchive(imei, name)
    elif inp == '5': 
        print('Goodbye.')
        break
    else: 
        continue
    
    
   
    
   






    

        



