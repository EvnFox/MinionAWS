import simplekml
from MinionAws.TransferManager import TransferManager
import MinionAws.CsvtoTxt as ct

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

e = Label(root, text ='Minion Data', width = 35, borderwidth=5)
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
	return
def func3():
	return

b1 = Button(root, text='Transfer', padx=80, pady=20, command=func)
b2 = Button(root, text='Compile', padx=80, pady=20, command=func1)
b3 = Button(root, text='Archive', padx=80, pady=20, command=func2)
b4 = Button(root, text='Unarchive', padx=80, pady=20, command=func3)


b1.grid(row = 1, column = 1, columnspan=2)
b2.grid(row = 2, column = 1, columnspan=2)
b3.grid(row=3, column=1, columnspan=2)
b4.grid(row=4, column =1, columnspan=2)

root.mainloop()