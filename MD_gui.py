# This python script is a GUI that handles downloading Minion data from AWS
import simplekml
from MinionAws.TransferManager import TransferManager
import MinionAws.CsvtoTxt as ct
import os
from tkinter import *
from tkinter import filedialog

#initilize a session
session = TransferManager('https://sqs.us-east-1.amazonaws.com/728808211862/ICCMO.fifo','minion-data')
kml = simplekml.Kml()

# Create TK object (main window)
root = Tk()

# Create titile 
root.title('Minion Data')

# Set the directory where files will be saved. Also set this as root directory for the program
session.Dir = root.directory = filedialog.askdirectory() + "\\"
#root.geometry('1200x600')

#generated title of application and lists the CWD 
e = Label(root, text ='Minion Data\n{}'.format(session.Dir[0:-1]), width = 50, borderwidth=5)
e.grid(row = 0, column = 0, columnspan=4, padx=10, pady = 10)


# func, ..., func3 handle the four main operations of the program
def func():
    session.queue_to_s3() 	

def func1():
    # empty the cwd before downloading new files 
    session.clean_dir()
    session.access_s3()
    session.jsontocsv()
    
    for i in session.devices: 
        # ct.create files generates all files and returns location data to be formated 
        # into kml. 
        location_data = ct.create_files(i, session.Dir)
        if location_data != []:
            multipnt1 = kml.newmultigeometry(name='imei_{}'.format(i))

            #multipnt1.newlinestring(name='imei_{}'.format(i), 
            #                   coords=[location_data])

            
            for j in range(len(location_data)):
               pnt = multipnt1.newpoint(name=str(j),
                                   description =str(j),
                                   coords=[location_data[j]])
        kml.save('{}\\txt_{}\\location.kml'.format(session.Dir, i))

    
    # KML inaccurate is created using data from Iridium satilite not the GPS data transmitted by 
    # the minion.
    #for i in session.devices:
        #kpath = session.Dir + '\\txt_{}'.format(i)
        #kml_file = 'inaccurate.kml'
       # kml_path = kpath + '\\' + kml_file
       # multipnt = kml.newmultigeometry(name='imei_{}'.format(i))
       # data = ct.csv_kml(i, session.Dir)

       # multipnt.newlinestring(name='imei_{}'.format(i), coords=data)

      #  for j in range(len(data)): 
       #     multipnt.newpoint(name=str(j), coords=[data[j]])
    
    #kml.save(kml_path)
           
def func2():
    a = filedialog.askdirectory()
    print(a)
    window = Tk()
    window.title("Archive")
    inp = Entry(window, width = 50)
    inp.grid(row=0, column=0, columnspan=4)
    inp.insert(0,'name')
    
    

    if a[-19:-15] == 'txt_':
        imei = a[-15:]
    else: 
        window.destroy()
        return

    def save(): 
        try:
            os.rename(session.Dir + '\\txt_' + imei, session.Dir + '\\' + inp.get() +'_txt_' + imei)
        except: 
            print('mission name already in use' )
            window.destroy()
            return
        session.archive(imei, inp.get())
        window.destroy()
        return

    
    b = Button(window, text='archive', command=save)
    c = Button(window, text='cancel', command=window.destroy)

    b.grid(row=2, column=2, columnspan=1)
    c.grid(row=2, column=3, columnspan=1)
    window.mainloop()

def func3():
    
    window = Tk()
    window.title("Unarchive")

    imei_inp = Entry(window, width = 50)
    imei_inp.grid(row=0, column=0, columnspan=4)
    imei_inp.insert(0,'imei to unarchive')

    space = Label(window, text=' ')
    space.grid(row=1, column=0, columnspan=4)

    mission_inp = Entry(window, width = 50)
    mission_inp.grid(row=2, column=0, columnspan=4)
    mission_inp.insert(0,'mission name')

    message = Label(window, text='Make sure the imei you are unarchiving is not \nactive, failure to do so will result in dataloss', width = 50)
    message.grid(row =3, column=0, columnspan=4)
    

    def save(): 
        imei = imei_inp.get()

       
        if imei in session.devices: 
            message.configure(text='WARNING: the device you are trying to unarchive is currenly active\n please archive the current mission before unarchiving an old one.')
            print('WARNING: the device you are trying to unarchive is currenly active\n please archive the current mission before unarchiving an old one.')
            window.destroy()
            return
        try:
            if session.Dir  + mission_inp.get() +'_txt_' + imei in os.listdir():
                os.rename(session.Dir  + mission_inp.get() +'_txt_' + imei, session.Dir + '\\txt_' + imei)
            
        except: 
            # os.remame gives an execption if the file we are trying to name already exists. 
            print('Unarchived version of IMEI exists in the same directory\nTo prevent data loss make sure you archive the active session')
            window.destroy()
            return
        session.unarchive(imei, mission_inp.get())
        window.destroy()
        return

    
    b = Button(window, text='unarchive', command=save)
    c = Button(window, text='cancel', command=window.destroy)

    b.grid(row=1, column=4, columnspan=1)
    c.grid(row=2, column=4, columnspan=1)
    window.mainloop()

#Define the four main buttons for the application 
b1 = Button(root, text='Transfer', padx=80, pady=20, command=func)
b2 = Button(root, text='Compile', padx=80, pady=20, command=func1)
b3 = Button(root, text='Archive', padx=80, pady=20, command=func2)
b4 = Button(root, text='Unarchive', padx=80, pady=20, command=func3)

#create grid to place things into the window. 
b1.grid(row = 1, column = 1, columnspan=2)
b2.grid(row = 2, column = 1, columnspan=2)
b3.grid(row=3, column=1, columnspan=2)
b4.grid(row=4, column =1, columnspan=2)

#Run application
root.mainloop()

