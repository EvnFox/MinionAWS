import simplekml
from MinionAws.TransferManager import TransferManager
import MinionAws.CsvtoTxt as ct
import os
# import filedialog module
from tkinter import *
from tkinter import filedialog

#initilize a session
session = TransferManager('https://sqs.us-east-1.amazonaws.com/728808211862/ICCMO.fifo','minion-data')
kml = simplekml.Kml()

 

root = Tk()

root.title('Minion Data')
session.Dir = root.directory = filedialog.askdirectory() + "\\"
#root.geometry('1200x600')

e = Label(root, text ='Minion Data\n{}'.format(session.Dir[0:-1]), width = 50, borderwidth=5)
e.grid(row = 0, column = 0, columnspan=4, padx=10, pady = 10)


def func():
    session.queue_to_s3() 
	

def func1():
    session.clean_dir()
    session.access_s3()
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
    
    
    for i in session.devices:
        kpath = session.Dir
        kml_file = 'inaccurate_' + str(i) + '.kml'
        kml_path = kpath + '\\' + kml_file
    
        data = ct.csv_kml(i, session.Dir)
        line = kml.newlinestring(name='imei_{}'.format(i), coords=data)
        kml.save(kml_path)

           
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
            os.rename(session.Dir  + mission_inp.get() +'_txt_' + imei, session.Dir + '\\txt_' + imei)
        except: 
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

b1 = Button(root, text='Transfer', padx=80, pady=20, command=func)
b2 = Button(root, text='Compile', padx=80, pady=20, command=func1)
b3 = Button(root, text='Archive', padx=80, pady=20, command=func2)
b4 = Button(root, text='Unarchive', padx=80, pady=20, command=func3)


b1.grid(row = 1, column = 1, columnspan=2)
b2.grid(row = 2, column = 1, columnspan=2)
b3.grid(row=3, column=1, columnspan=2)
b4.grid(row=4, column =1, columnspan=2)

root.mainloop()