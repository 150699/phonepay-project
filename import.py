import pandas as pd
import json
import os
from database import database_connection

#This is to direct the path to get the data as states
mydb = database_connection()
cursor = mydb.cursor()
path=r"C:/Users/ASUS/Documents/DSC/pulse/data/aggregated/transaction/country/india/state"
Agg_state_list=os.listdir(path)
Agg_state_list
#Agg_state_list--> to get the list of states in India

#<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

#This is to extract the data's to create a dataframe

clm={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

for i in Agg_state_list:
    p_i=path+"/"+i+"/"
    #print(p_i)
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm['Transacion_type'].append(Name)
              clm['Transacion_count'].append(count)
              clm['Transacion_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quater'].append(int(k.strip('.json')))
        
#Succesfully created a dataframe
Agg_Trans=pd.DataFrame(clm)
print(clm)
# Example: your dataframe
# Agg_Trans = pd.DataFrame(clm)

for _, row in Agg_Trans.iterrows():
    sql = """INSERT INTO Agg_Transaction
    (State, Year, Quarter, Transaction_Type, Transaction_Count, Transaction_Amount)
    VALUES (%s, %s, %s, %s, %s, %s)"""
    
    val = (row['State'], row['Year'], row['Quater'], row['Transacion_type'],
           row['Transacion_count'], row['Transacion_amount'])
    
    cursor.execute(sql, val)

mydb.commit()
print("All records inserted successfully!")