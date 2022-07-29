import pandas as pd 
import os 
from datetime import datetime


def convert(IMEI : str, dir : str):
    '''
    This function is called by create_files() 
    '''
    try: 
        os.mkdir(dir + "\\txt_" + str(IMEI))
        print('directory created')
    except:
        print("txt directory already exists")
        exit()
        
        

    #
    # Create the path to find the csv file. 
    try:
        path = dir + '\csv'
        file = 'imei_' + str(IMEI) + '.csv'
        file_path = path + '\\' + file
        df = pd.read_csv(file_path)
    except: 
        print("could not get imei_{}.csv, make sure that the file exists".format(IMEI))
        exit()

    # Generates a path for the txt file that will be created. 
    tpath = dir + "\\txt_" + str(IMEI)
    txt_file = 'imei_' + str(IMEI) + '.txt'
    txt_path = tpath + '\\' + txt_file

    # This will create the txt file and write to it. Note: if a txt file already exists with 
    # the same name it will be overwritten. 
    with open(txt_path, 'w') as T: 

        line_num = 0
        # loop through csv
        df = df.T
        df = df.sort_values(by=1)
        
        for i in df.iloc[0:df.shape[0], 5]: 

            if i == 'NaN':
                continue

            # Decode the hex format into ascii
            j = bytes.fromhex(i)
            ascii = j.decode("ASCII")
            
            # This loop creates a new line whenever a file break is detected, this makes our life easier in the create_files function.
            # We change the range in the first loop since we do not want to add a newline to the top of the file
            if line_num == 0: 
                l = len(ascii)
                for i in range(1,l): 
                    if ascii[i] == '$': 
                        ascii = ascii[:i] + '\n' + ascii[i:]
                        break
                line_num += 1
            else:
                l = len(ascii)
                for i in range(0,l): 
                    if ascii[i] == '$': 
                        ascii = ascii[:i] + '\n' + ascii[i:]
                        break

            # Write to file. 
            T.write(ascii)

            

    T.close()



def create_files(IMEI : str, dir : str) -> list: 
    '''
    Handles generation of minion data from CSV, this function works in two steps, 
    first it calls the convert() function which takes the csv and pastes all payloads 
    into a main txt file. Then the rest of this function loops through the txt file 
    and produces all other files from it. When it produces indiviual files for $0x, 
    it adds unix time and sample numbers. CSV must be of form outputed by 
    TransferManager.jsontoscsv()

    input: IMEI - string, The imei of the device whose csv needs to be compiled. 

            dir - string, directory where the csv folder is located. 

    output: Generates $01, $02, $03, $04 file types 
    returns: list containg gps data (eg. (lat, lon))
    
    '''

    # Generate a main text file containing all data that can be parsed through.
    convert(IMEI, dir)

    
    #Creates the path based on the given IMEI. 
 
    path = dir + '\\txt_' + str(IMEI)
    file = 'imei_' + str(IMEI) + '.txt'
    file_path = path + '\\' + file
   
    #sample number; resets after new file header
    k = 0

    #string that will hold unix time
    time = '1'

    # sampling rate
    rate = 0.25

    delims = []
    
    with open(file_path, 'r') as maintxt:
        current_file = ''
        location_data = []
        for line in maintxt: 
            ## NOTE: delims[0] = file code (eg $04), delims[1] = title (eg file name and date), delims[2] = sampling rate
            
            
            #if the begining of a line contains a filebreak then open a new file and begin writing to it. since 
            # we created a new line for every file break in convert() we know the begining of a header can only be at line[0]
            if line[0] == '$':

                delims = line.split(',')

                # When a file break is deteced we create a new filepath with the file code appened to the end of the file path
                current_file = path + "\\" + str(delims[0]) + ".txt" 

                # If the transmission was GPS
                if delims[0] == '$04': 
                    #$04,027,2022,07,27,12,59,06,41.491937,-71.422375
                    l= line.split(',')

                    # this if checks if msg was time stamped and generated unix time
                    try: 
                        if int(l[2]) > 0:
                        
                            dt = datetime(int(l[2]), int(l[3]), int(l[4]), int(l[5]), int(l[6]), int(l[7]))
                            time = str(dt.timestamp())
                        else:
                            time = 'Nan'
                    except: 
                        time = 'Nan'
                    
                    # This is a new header with unix time 
                    delims = [delims[0], delims[1], time, delims[-2], delims[-1]]
                    # append the data to be returned
                    location_data.append((l[-1], l[-2]))
                
                # If it was not GPS data then it is minion data
                else: 
                    # set sample rate
                    rate = float(delims[2])

                    # This handles generating unix time for the document. 
                    try: 
                        # delims[1] contains header 
                        # ex) delims = ['$02', '001-2022-05-20_15-00-54_TEMPPRES.txt', '0.25', 'Pressure(dbar*1000)', 'Temp(C*100)', 'TempTSYS01(C*100)']
                        # so delims[1] = '001-2022-05-20_15-00-54_TEMPPRES.txt'
                        t = delims[1].split('-')
                        # for example t = ['001', '2022', '05', '20_15', '00', '54_TEMPPRES.txt']
                        # so we need to split more
                        s = []
                        for i in range(len(t)): 
                            for j in t[i].split('_'):
                                s.append(j)
                        # example s = ['001', '2022', '05', '20', '15', '00', '54', 'TEMPPRES.txt'] as desired
                        try: 
                            if int(s[1]) > 0 :

                                dt = datetime(int(s[1]), int(s[2]), int(s[3]), int(s[4]), int(s[5]), int(s[6]))
                                time = str(dt.timestamp())
                            else: 
                                time = 'Nan'
                        except:
                            time = 'Nan'
                        
                        
                    except:
                        # This means the header does not match what the program expects. 
                        print('Error generating header. Make sure header matched what was expected\n')
                        raise
                    
                    # new header
                    delims[1] = s[0] + '_' + time + '_' + s[-1]

                # At this point we have looked at the header and changed the time stamp to unix time or Nan. 
                # So now we open the file and write the first line to it. 
                with open(current_file, 'a') as f:

                    line = ''
                    #print(delims)
                    for i in delims[0:-1]: 
                        j = i + ','
                        line += j
                    line = line + delims[-1]

                    f.write(line)

                    # Since we are doing a knew file we want to reset the the sample counter to 0.
                    k = 0
            else: 
                # If no file break was dected, we write to the previous file. Note that every maintext should start with a new file,
                # if it doesnt then the program will write to the non existing previous file 
                # # and there will be an exeption since current_file = '' by defult and this cannont be written to.
                try: 
                    with open(current_file, 'a') as f:
                        # if the doc was time stamped correctly we will add the time to each sample
                        if time != 'Nan':
                            # The first term on the left adds the sampling time. The second term 'k' is the sample number
                            line = str(float(time) + k*(1/rate)) + ',' + str(k) + ',' + line
                        else: 
                            line = 'Nan,' + str(k) + ',' + line
                        # increment the sample number
                        k += 1
                        f.write(line)
                except: 
                    # if current file could not be oppened then it is probably because the main txt file 
                    # did not start with a new file header. 
                    print("Main text file didn't start with file break '$' see 'minionaws\CsvtoTxt.py' line 223 ")
                    

    return location_data


def csv_kml(IMEI, dir):
    try:
        path = dir + '\csv'
        file = 'imei_' + str(IMEI) + '.csv'
        file_path = path + '\\' + file
        df = pd.read_csv(file_path)
    except: 
        print("could not get imei_{}.csv, make sure that the file exists".format(IMEI))
        exit()

     # Generates a path for the txt file that will be created. 
 

    df = df.T
    df = df.sort_values(by=1)

    location_data = []

    for i in range(df.shape[0]):
        location_data.append((df.iloc[i, 4], df.iloc[i, 3]))
        

    return location_data



