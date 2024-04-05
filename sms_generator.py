import logging
import sys
from mysql.connector import connect
import pandas as pd
import smpplib.gsm
import smpplib.client
import smpplib.consts
import time
import configparser
from threading import Thread

class sms_generator:
    ip=""
    port=0
    username=""
    passwd=""
    files=[]
    repeat=False
    amount,amount_per_sec="",""
    
    def __init__(self, ip="", port="", user="", password="", files=[], repeat=False, amount=0, amount_per_sec=0):
        self.ip=ip
        self.port=port
        self.username=user
        self.passwd=password
        self.files=files
        self.repeat=repeat
        self.amount=amount
        self.amount_per_sec=amount_per_sec

    def Generator(self):
        connection = connect(host="localhost", user="root", password="", database="templates")
        cursor = connection.cursor()
    
        
        s = '''
        CREATE TABLE IF NOT EXISTS generator_''' + (self.files[0].split("/")[-1]).split(".")[0] + '''(
                       id INT PRIMARY KEY AUTO_INCREMENT,
                       ip   VARCHAR(255),
                       port VARCHAR(255),
                       user VARCHAR(255), 
                       password VARCHAR(255), 
                       file VARCHAR(255), 
                       repeaat VARCHAR(255), 
                       amount VARCHAR(255), 
                       amount_per_sec VARCHAR(255)
        );
        '''
        print(s)
        cursor.execute(s)
        
        # files  = [x, y, z]
        file = ",".join(self.files) # x,y,z
        print(file)
        s="INSERT INTO generator(file) values('{}')".format(file)
        print(s)
        cursor.execute(s)
        s="""INSERT INTO generator_"""+(self.files[0].split("/")[-1]).split(".")[0]+"""(ip, port, user, password, file, repeaat, amount,
        amount_per_sec) VALUES ('{}', '{}', '{}', '{}', '{}','{}', '{}', '{}')""".format(self.ip,
        self.port, self.username, self.passwd,file,self.repeat,  self.amount, self.amount_per_sec)
        print(s)
        cursor.execute(s)
        connection.commit()
        connection.close()


    def connection(self,query):
        #print("Executing query "+query+".....")
        connection = connect(host="localhost", user="root", password="", database="templates")
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        connection.close()

    def get_data(self,table):
        connection = connect(host="localhost", user="root", password="", database="templates")
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM {}".format(table))  # Fixed typo 'sekect' to 'SELECT'
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
        connection.close()
        return df  # Corrected return statement
    
    def get_file_by_id(self, id):
        connection = connect(host="localhost", user="root", password="", database="templates")
        cursor = connection.cursor()
        query = "SELECT file FROM generator WHERE id = %s"
        cursor.execute(query, (id,))
        result = cursor.fetchone()  # Fetch the row
        connection.close()  # Close the connection before further processing
        print("R:",result)
        if result is not None:
            file = result[0]  # Fetch the 'file' column from the first row
            return file
        else:
            print("No matching row found in the database.")
            return None

            

    def get_data_by_id(self,id):
        files = self.get_file_by_id(id)
        print("\nfiles:",files)
        connection = connect(host="localhost", user="root", password="", database="templates")
        cursor = connection.cursor()
        query = "SELECT ip, port, user, password, file, repeaat, amount, amount_per_sec FROM generator_{} WHERE id = %s".format(files.split(",")[0].split("/")[-1].split(".")[0])
        print("\nQuery:",query,"\n")
        cursor.execute(query, (id,))
        data = cursor.fetchone()
        connection.close()
        print("\nData:",data)
        if data:  # Check if data is not None
            ip, port, user, password, file, repeat, total, a = data
            repeat=bool(repeat)
            return ip, port, user, password, file, repeat, total, a
        else:
            return None  # Return None if no data is found for the provided ID

    def send_sms(self,client,src,dst,msg):
        #print("Client:",client,"\nsrc:",src,"\ndst:",dst,"msg:",msg)
        # Two parts, GSM default / UCS2, SMS with UDH
        parts, encoding_flag, msg_type_flag = smpplib.gsm.make_parts(u"{}".format(msg))
        #print("Parts:",parts,"\n Encoding Flag:",encoding_flag,"\n Msg Type:",msg_type_flag)
        for part in parts:
            pdu = client.send_message(
                source_addr_ton=smpplib.consts.SMPP_TON_ALNUM,
                source_addr_npi=smpplib.consts.SMPP_NPI_UNK,
                # Make sure it is a byte string, not unicode:
                source_addr=src,

                dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
                dest_addr_npi=smpplib.consts.SMPP_NPI_ISDN,
                # Make sure these two params are byte strings, not unicode:
                destination_addr=dst,
                short_message=part,

                data_coding=encoding_flag,
                esm_class=msg_type_flag,
                registered_delivery=True,
            )
            logging.info('submit_sm {}->{} seqno: {}'.format(pdu.source_addr,pdu.destination_addr,pdu.sequence))

    def sms_per_files(self,client,files):
        for file in files:
            self.sms_per_file(client,file)
    def sms_per_file(self,client,file):
        # Get data from MySQL DB with table named 'file'
        connection = connect(host="localhost", user="root", password="", database="templates")
        cursor = connection.cursor()
        print("Name:{}".format((file.split("/")[-1])))
        cursor.execute("SELECT * FROM {}".format((file.split("/")[-1]).split(".")[0]))
        data = cursor.fetchall()

        connection.close()
    
        # Assuming data structure is (src, dst, msg)
        for row in data:

            idi,src, dst, msg = row[0], row[1], row[2],row[3]
            self.send_sms(client, src, dst, msg)

    def send_per_amount(self,client,files,amount_per_sec,amount):
        monitor=0
        done=False
        while(True):
            for i in range(int(amount_per_sec)):
                if(monitor==int(amount)):
                    done=True
                    print("All sms has been sent!!!!!!!!!")
                    break            
                else:
                    self.sms_per_files(client,files)
                    monitor+=i
                    print("Sms ",monitor,"has been sent")
            if(done):
                break
            else:
                time.sleep(1)
    
    def copy_template(self,files):
        for file in files:
            self.insert_file_rows(file)
    def insert_file_rows(self, file):
        if "/" in file:
            name = (file.split("/")[-1]).split(".")[0]
        else:
            name = file.split(".")[0]
        
        df = pd.read_csv(file)
        cols = df.columns
        
        # Create table if not exists
        self.connection("""CREATE TABLE IF NOT EXISTS {} (
                   id INT AUTO_INCREMENT PRIMARY KEY,
                   SID VARCHAR(255),
                   Destination VARCHAR(255),
                   Content  VARCHAR(255)
        )""".format(str(name)))
        
        # Check if the number of columns match
        if len(cols) != 3:
            print("Number of columns in DataFrame doesn't match the expected count.")
            return
        
        src, dst, msg = str(cols[0]), str(cols[1]), str(cols[2])
        print("Df:", df)
        
        for i, row in df.iterrows():
            # Insert values into the table
            self.connection("INSERT INTO {}(SID, Destination, Content) VALUES ('{}', '{}', '{}')".format(name, str(row.iloc[0]), str(row.iloc[1]), str(row.iloc[2])))



    """
    def insert_file_rows(self , file):
        if "/" in file:
            name=(file.split("/")[-1]).split(".")[0]
        else:
            name=file.split(".")[0]
        df=pd.read_csv(file)
        cols=df.columns
        self.connection(""CREATE TABLE IF NOT EXISTS {} (
                   id INT AUTO_INCREMENT PRIMARY KEY,
                   SID VARCHAR(255),
                   Destination VARCHAR(255),
                   Content  VARCHAR(255)
        )"".format(str(name)))
        src,dst,msg=str(cols[0]),str(cols[1]),str(cols[2])
        print("Df:",df)
        for i, row in df.iterrows():
            self.connection("INSERT INTO "+name+"(SID,Destination,Content) values('"+str(row[0])+'","'+str(row[1])+"','"+str(row[2])+"')")
    """
    def main(self, ip, port, user, password, files, repeat=False, total=0, a=0):
        # Set up logging for debugging
        logging.basicConfig(level='INFO')

        # Two parts, GSM default / UCS2, SMS with UDH

        client = smpplib.client.Client(str(ip),int(port) )

        # Print when obtain message_id
        client.set_message_sent_handler(
            lambda pdu: logging.info('submit_sm_resp seqno: {} msgid: {}'.format(pdu.sequence, pdu.message_id)))

        # Handle delivery receipts (and any MO SMS)
        def handle_deliver_sm(pdu):
                logging.info('delivered msgid:{}'.format(pdu.receipted_message_id))
                return 0 # cmd status for deliver_sm_resp

        client.set_message_received_handler(lambda pdu: handle_deliver_sm(pdu))

        client.connect()
        client.bind_transceiver(system_id=str(user), password=str(password))
                    
            # If repeat is True, send messages repeatedly with specified total and interval
        if repeat:
                self.send_per_amount(client, files, total, a)
        else:
                # Otherwise, send SMS per file
                self.sms_per_files(client, files)
        
class frontend(sms_generator) :
    

    
    def check(self):
        connection = connect(host="localhost", user="root", password="", database="templates")
        self.connection("""CREATE TABLE IF NOT EXISTS generator(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,file VARCHAR(255)) """)
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM generator")
        count = cursor.fetchone()[0]
        connection.close()
        return count == 0
    def rows_gen(self):
            connection = connect(host="localhost", user="root", password="", database="templates")
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM generator")
            count = cursor.fetchone()[0]
            connection.close()
            return count
    def load_config(self,l):
        
        config= configparser.RawConfigParser()   
        configFilePath = r'{}'.format(str(l))
        config.read(configFilePath)
        
        # Get values from the config object
        ip = config["config"]["ip"]
        port = config["config"]["port"]
        user = config["config"]["username"]
        password = config["config"]["password"]
        files = config["config"]["files"].split(",")
        repeat = config["config"].getboolean("repeat")  # Assuming repeat is a boolean value in the config
        total = config["config"]["SMS/sec"]        # Assuming total is an integer value in the config
        a = config["config"]["Total"]                      # Assuming a is a string value in the config
        return ip, port, user, password, files, repeat, total, a
    
    def delete_generator(self,file,num):
        self.connection("drop table generator_{}".format(file.split("/")[-1]))
        self.connection("DELETE FROM generator WHERE id={}".format(int(num)))

    def dashboard(self):
        print("---------------------------------------------------ADMIN DASHBOARD---------------------------------------------------")
        if(self.check()):
            print("No Generator Inserted")
        else:
            for i in range(self.rows_gen()):
                print("----> Generator {}".format(str(i+1)))

        print("---------------------------------------------------------------------------------------------------------------------")
        print("1- Add From Config file")
        print("2- Add Generator ")
        print("3- Remove Generator ")
        print("4- Run Generator")
        print("5- Attach/Detach Template:")
        choice=input("Choise Option :")
        if choice == "1" or choice == "load" or choice =="Load" or choice == "LOAD":
            load=input("PLZ Enter Configuration File :")
            
            ip, port, user, password, files, repeat, total, a=self.load_config(load)
            print("Infos:\n",ip, port, user, password, files, repeat, total, a)
            print("Repeat:",repeat)
            if(repeat):
                gen_obj=sms_generator(ip, port, user, password, files, repeat, total, a)
                gen_obj.Generator()
                gen_obj.copy_template(files)
                th=Thread(target=gen_obj.main,args=(ip, port, user, password, files, repeat, total, a))
                th.start()
#                gen_obj.main(ip, port, user, password, files, repeat, total, a)
            else:
                gen_obj=sms_generator(ip, port, user, password, files)
                gen_obj.Generator()
                gen_obj.copy_template(files)
                th=Thread(target=gen_obj.main,args=(ip, port, user, password, files))
                th.start()
#                gen_obj.main(ip, port, user, password, files) 
        elif choice == "2" or choice == "Add" or choice =="add" or choice == "ADD":
            # port, user, password, file, repeat, amount, amount_per_sec
            ip=input("Enter IP Address>>>")
            try:
                port=int(input("Enter PORT Number >>>"))
            except Exception as err:
                print("Plz Enter a numerical value ...\n",err,"\n")
            user=input("Enter Username >>>")
            password=input("Enter password >>>")
            files=[]
            while True:
                file=input(" Add File to this generation(Press break to stop)>>> ")
                if file=="break":
                    break
                else:
                    files.append(file)
            
            repeat=input("repeat (y/n)>>>")
            if(repeat=="y" or repeat=="Y"):
                try:
                    total=int(input("Total Amount of SMSs:"))
                except Exception as err:
                    print("Plz Enter a numerical value ...\n",err,"\n")
                try:
                    a=int(input("Sms/sec:"))
                except Exception as err:
                    print("Plz Enter a numerical value ...\n",err,"\n")
                repeat=True

                gen_obj=sms_generator(ip, port, user, password, file, repeat, total, a)
                gen_obj.Generator()
                gen_obj.copy_template(file)
                th=Thread(target=gen_obj.main,args=(ip, port, user, password, files, repeat, total, a))
                th.start()
                #gen_obj.main(ip, port, user, password, files, repeat, total, a)
            else:
                gen_obj=sms_generator(ip, port, user, password, file)
                gen_obj.Generator()
                gen_obj.copy_template(file)
                th=Thread(target=gen_obj.main,args=(ip, port, user, password, files))
                th.start()
#                gen_obj.main(ip, port, user, password, files, repeat, total, a)     
            
        elif choice == "3" or choice == "Remove" or choice =="remove" or choice == "REMOVE": 
            num=input("The Number of Generator >>>")
            ip, port, user, password, files, repeat, total, a=self.get_data_by_id(num)
            name=files.split(",")[0].split("/")[-1].split(".")[0]
            self.delete_generator(name,num)
        elif choice == "4" or choice == "Run" or choice =="run" or choice == "RUN": 
            try:
                num=int(input("Plz Enter the number of generator >>>"))
                
            except Exception as err:
                print("plz Enter a numerical Value !!\n",err )
            ip, port, user, password, files, repeat, total, a=self.get_data_by_id(num)
            files=files.split(",")
                
            gen_obj=sms_generator(ip, port, user, password, file, repeat, total, a)
            th=Thread(target=gen_obj.main,args=(ip, port, user, password, files, repeat, total, a))
            th.start()
#            gen_obj.main(ip, port, user, password, file, repeat, total, a)
        elif choice == "5" :
            i=input("Choise option(Attach/Dettach):")
            if i=="Attach" or i=="attach" or i=="ATTACH":
                try:
                    num=int(input("Plz Enter the number of generator >>>"))
                
                except Exception as err:
                    print("plz Enter a numerical Value !!\n",err )
                new=input("New template:")
                ip, port, user, password, files, repeat, total, a=self.get_data_by_id(num)
                files=list(files.split(","))
                files.append(new)
                files=",".join(files)
                name=files.split(",")[0].split("/")[-1].split(".")[0]
                self.connection("UPDATE generator_{} SET file='{}'".format(name, files))
            elif i=="Dettach" or i=="dettach" or i=="DETTACH":
                new=input("Template Name:")
                ip, port, user, password, files, repeat, total, a=self.get_data_by_id(num)
                files=list(files.split(","))
                files.remove(new)
                files=",".join(files)
                name=files.split(",")[0].split("/")[-1].split(".")[0]
                self.connection("UPDATE generator_{} SET file='{}'".format(name, files))
        else:
            print("Enter a valid option")
            
            
if __name__ == "__main__":
    #print('all executing well')
    gen=frontend()
    gen.dashboard()
