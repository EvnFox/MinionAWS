from distutils import text_file
import pandas as pd 
import numpy as np 
import os 

from datetime import datetime, timezone




# takes in imei of sbd device as string or int and will create a new maintxt file conatining the data, this data 
def convert(IMEI, dir):

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


# create_files(IMEI) -- takes IMEI as string or int and will turn the text file created by convert() 
# into multiple text files each containg one complete file. 
def create_files(IMEI, dir): 

    # Generate a master text file containing all data that can be parsed through.
    convert(IMEI, dir)

    
    #Creates the path based on the given IMEI. 
    try:
        path = dir + '\\txt_' + str(IMEI)
        file = 'imei_' + str(IMEI) + '.txt'
        file_path = path + '\\' + file
    except: 
        print("imei_{}.txt does not exist".format(IMEI))

    #sample number; resets after new file header
    k = 0

    #string that will hold unix time
    time = '1'

    #sampling rate
    rate = 0.25
    delims = []
    
    with open(file_path, 'r') as maintxt:
        current_file = ''
        location_data = []
        for line in maintxt: 
            #delims[0] = file code (ex 04), delims[1] = title (eg file name and date), delims[2] = sampling rate
            delims = find_delims(line)
            
            #if the begining of a line contains a filebreak then open a new file and begin writing to it. since 
            # we created a new line for every file break we know the begining of a header can only be at line[0]

            if line[0] == '$':

                # When a file break is deteced we create a new filepath with the file code appened to the end of the file path
                current_file = path + "\\imei_" + str(IMEI)+ "_" + str(line[1:3]) + ".txt" 
                if line[1:3] == '04': 
                    location = line.split(',')
                    
                    location_data.append((location[-2], location[-1]))
                    
                    
            
                # This handles generating unix time for the document. 
                try: 
                    dt = datetime(int(line[8:12]), int(line[13:15]), int(line[16:18]), int(line[19:21]), int(line[22:24]), int(line[25:27]))
                    time = str(dt.timestamp())
                    rate = float(delims[2])
                except:
                    # this means there was an error generating the unix time
                    time = '1'


                # Since we are only in this if statement if a file break was detected we open the new file and append unix time 
                # if it was generated correctly. Then the line from the maintext is saved to the file. 
                with open(current_file, 'a') as f:

                    if time != '1':
                        #if we have valid time data paste it
                        line = line[:8] + time + line[27:]

                    else:
                        #We are here if we have a new file with no valied time data. If this is a GPS file 
                        # then it is 04 and we want to get rid of indicies 8 to 27. If it is not a GPS file 
                        # then it is a data file with no time and we just want to include NaN without deleteing any
                        # thing. This is hardcoded based on known errors with the files, if a new error is introduced 
                        # this will likely produce unexpected behavior.
                        if line[1:3] =='04':
                            line = line[:8] + 'NaN' + line[27:]
                        else: 
                            line = line[:8] + 'NaN' + line[8:]

                    f.write(line)
                    k = 0
            else: 
                # If no file break was dected, we write to the previous file. Note that every maintext should start with a new file,
                # if it doesnt then there will be an exeption since current_file = '' by defult (see line 76) and this cannont be written to.
                try: 
                    with open(current_file, 'a') as f:
                        if time != '1':
                            # The first term on the left adds the sampling time.The second term is the sample number
                            line = str(float(time) + k*(1/rate)) + ',' + str(k) + ',' + line
                        else: 
                            line = 'NaN,' + str(k) + ',' + line
                        k += 1
                        f.write(line)
                except: 
                    print("Main text file didn't start with file break '$' ")
                    exit()

    return location_data
    # Delete the maintxt file since it is no longer useful. Once we get here all txt files 
    # have been created. if you are having issues comment the below out and see if the txt file is being generated 
    # correctly.
    #os.remove(file_path)

# This function will loop through the header of a file and find the file code, title, and sampling rate.

def find_delims(line):
    
    count = 0
    code = ''
    title = ''
    rate = ''

    for i in range(0, len(line)): 
        if line[i] == ',':
            count += 1
            continue
        if count == 0: 
            code += line[i] 
        elif count == 1: 
            title += line[i]
        elif count == 2: 
            rate += line[i]
    return [code, title, rate]

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



