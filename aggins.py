import pandas as pd
import json
import os
from database import database_connection

# Connect to MySQL
mydb = database_connection()
cursor = mydb.cursor()

# Path to the user data
path = r"C:/Users/ASUS/Documents/DSC/pulse/data/aggregated/user/country/india/state"
Agg_state_list = os.listdir(path)

# Dictionaries for DataFrames
clm_agg = {'State': [], 'Year': [], 'Quarter': [], 'RegisteredUsers': [], 'AppOpens': []}
clm_device = {'State': [], 'Year': [], 'Quarter': [], 'Brand': [], 'Count': [], 'Percentage': []}

# Loop through all states
for i in Agg_state_list:
    p_i = os.path.join(path, i)
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:  # Year folders
        p_j = os.path.join(p_i, j)
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:  # Quarter files
            p_k = os.path.join(p_j, k)
            with open(p_k, 'r') as Data:
                D = json.load(Data)

            # Check if "data" exists
            if D.get('data'):

                # 1. Aggregated Users
                if 'aggregated' in D['data']:
                    registeredUsers = D['data']['aggregated'].get('registeredUsers', 0)
                    appOpens = D['data']['aggregated'].get('appOpens', 0)

                    clm_agg['State'].append(i)
                    clm_agg['Year'].append(j)
                    clm_agg['Quarter'].append(int(k.strip('.json')))
                    clm_agg['RegisteredUsers'].append(registeredUsers)
                    clm_agg['AppOpens'].append(appOpens)

                # 2. Users by Device
                if 'usersByDevice' in D['data']:
                    for z in D['data']['usersByDevice']:
                        brand = z.get('brand', 'Unknown')
                        count = z.get('count', 0)
                        percentage = z.get('percentage', 0.0)

                        clm_device['State'].append(i)
                        clm_device['Year'].append(j)
                        clm_device['Quarter'].append(int(k.strip('.json')))
                        clm_device['Brand'].append(brand)
                        clm_device['Count'].append(count)
                        clm_device['Percentage'].append(percentage)

# Create DataFrames
Agg_User_Aggregated = pd.DataFrame(clm_agg)
Agg_User_ByDevice = pd.DataFrame(clm_device)

# Insert Aggregated Data
for _, row in Agg_User_Aggregated.iterrows():
    sql = """INSERT INTO Agg_User_Aggregated
    (State, Year, Quarter, RegisteredUsers, AppOpens)
    VALUES (%s, %s, %s, %s, %s)"""
    val = (row['State'], row['Year'], row['Quarter'], row['RegisteredUsers'], row['AppOpens'])
    cursor.execute(sql, val)

# Insert Device Data
for _, row in Agg_User_ByDevice.iterrows():
    sql = """INSERT INTO Agg_User_ByDevice
    (State, Year, Quarter, Brand, Count, Percentage)
    VALUES (%s, %s, %s, %s, %s, %s)"""
    val = (row['State'], row['Year'], row['Quarter'], row['Brand'], row['Count'], row['Percentage'])
    cursor.execute(sql, val)

mydb.commit()
print("All user records inserted successfully!")