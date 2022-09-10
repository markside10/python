import pymysql
import pandas as pd
import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path

counter = pd.datetime.now().strftime("%m-%d-%Y")

process_name = 'Name' #PUT PROCESS NAME
process_id = '123' #PUT PROCESS ID

conn=pymysql.connect(host='1.1.1.1',port=int(3306),user='username',passwd='password123',db='db_name')

data=pd.read_sql_query("SELECT * from table" ,conn)

row1 = data.append({} , 
                    ignore_index=True)
row2 = row1.append({} , 
                    ignore_index=True)

row3 = row2.append({'NAME' : '--- UPDATE DATE ---',
                    'CREATE_DATE' : "-->",
                    'UPDATE_DATE' : pd.datetime.now().strftime("%m/%d/%Y")} , 
                    ignore_index=True)

row3.to_csv(process_id+'-'+process_name+'.csv', index=False)
