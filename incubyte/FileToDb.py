import pandas as pd
import mysql.connector
from datetime import datetime

class FileToDb:
  def __init__(self,host,user,password,databaseName):
    self.host = host
    self.user = user
    self.password = password
    self.databaseName = databaseName
    self.mydb = None
    self.data=None

  def connectToDb(self):
    self.mydb = mysql.connector.connect(
      host=self.host,
      user=self.user,
      password=self.password,
      database=self.databaseName
    )

  def readFile(self, filepath):
    self.data=pd.read_csv(filepath, sep='|', parse_dates=['Open_Date','Last_Consulted_Date', "DOB"])

  def populateDb(self):
    dbcursor = self.mydb.cursor()
    for index, row in self.data.iterrows():
        country = row['Country']
        dob=datetime.strptime(row['DOB'], '%d%m%Y').date()

        dbcursor.execute("""CREATE TABLE IF NOT EXISTS Patients_{} (Customer_Name varchar(255) NOT NULL, 
        Customer_ID varchar(18) PRIMARY KEY, 
        Open_Dt date NOT NULL, 
        Consulted_Dt date, 
        Vac_type char(5), 
        Dr_Name varchar(255),
        State char(5), 
        Country char(5), 
        DOB date, 
        Flag char 
        )""".format(country))

        print("created Patients_{} table".format(country))
        
        sql = "INSERT INTO Patients_{} (Customer_Name,Customer_ID,Open_Dt,Consulted_Dt,Vac_type,Dr_Name,State,Country,DOB,Flag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(country)
        val = (row['Customer_Name'],row['Customer_Id'],row['Open_Date'].date(),row['Last_Consulted_Date'].date(),row['Vaccination_Id'],row['Dr_Name'],row['State'],row['Country'],dob,row['Is_Active'])
        dbcursor.execute(sql, val)
        self.mydb.commit()

        print("insterted customer with Id {} into Patients_{} table".format(row['Customer_Id'], country))


fileToDB = FileToDb("localhost", "root", "mysql", "incubyte")
fileToDB.connectToDb()
fileToDB.readFile("./resources/data.txt")
fileToDB.populateDb()