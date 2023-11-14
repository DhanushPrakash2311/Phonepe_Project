
import git

repo_url = "https://github.com/PhonePe/pulse.git"

destination_dir = r"C:\Vs project\phone pe project\Data"

repo = git.Repo.clone_from(repo_url, destination_dir)

print(f"Cloned repository to {destination_dir}")
#Cloned repository to C:\VS PROJECTS\PHONEPE PROJ\DATA
#CONVERTING JSON FILES INTO DATAFRAME TO MIGRATE THE DATA INTO SQL

#----------------------------- Importing required packages for conversion -------------------------------#

import os
import json
import pandas as pd
import mysql.connector
from pprint import pprint
import pymysql
from sqlalchemy import create_engine

#----------------------------- Establishing connection with MySQL to migrate the data ----------------------#

mycon = mysql.connector.connect(host="127.0.0.1",user="root",password="dhanushd")
mycursor = mycon.cursor()
mycursor.execute(f"CREATE DATABASE IF NOT EXISTS Phonepe;")


root=f'/'#formatting to path conversion

#-------------------------------------- Data Conversion and Migration ----------------------------------------#

#creating a list of columns for specific dataframe
clm={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

#path of concerned file
agg_path=r"C:/Vs project/phone pe project/Data/data/aggregated/transaction/country/india/state/"
agg_state=os.listdir(agg_path)

#block of code to access the concerned file and converting into dataframe
for i in agg_state:
    p_i=agg_path+i+root
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+root
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              clm['Transaction_type'].append(Name)
              clm['Transaction_count'].append(count)
              clm['Transaction_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quarter'].append(int(k.strip('.json')))

Agg_Trans=pd.DataFrame(clm)

#lines of code to replace Nan values with respective suitable values
Agg_Trans= Agg_Trans.fillna({
        'Transaction_type': "Undefined",
        'Transaction_count': 0,
        'Transaction_amount': 0
    })

#creating engine to migrate the data into sql
engine = create_engine('mysql+mysqlconnector://root:dhanushd@localhost/Phonepe', echo=False)
Agg_Trans.to_sql('agg_trans', con=engine, if_exists='replace', index=False)
engine.dispose()

#path of concerned file
map_path=(r"C:/Vs project/phone pe project/Data/data/map/transaction/hover/country/india/state/")
map_state=os.listdir(map_path)

#block of code to access the concerned file and converting into dataframe
for i in map_state:
    p_i=map_path+i+root
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+root
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['hoverDataList']:
              Name=z['name']
              count=z['metric'][0]['count']
              amount=z['metric'][0]['amount']
              clm['Transaction_type'].append(Name)
              clm['Transaction_count'].append(count)
              clm['Transaction_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quarter'].append(int(k.strip('.json')))

Map_Trans=pd.DataFrame(clm)

#lines of code to replace Nan values with respective suitable values
Map_Trans= Map_Trans.fillna({
        'Transaction_type': "Undefined",
        'Transaction_count': 0,
        'Transaction_amount': 0
    })

#creating engine to migrate the data into sql
engine = create_engine('mysql+mysqlconnector://root:dhanushd@localhost/Phonepe', echo=False)
Map_Trans.to_sql('map_trans', con=engine, if_exists='replace', index=False)
engine.dispose()



#creating a list of columns for specific dataframe
clm={'State':[], 'Year':[],'Quarter':[],'District':[], 'Transaction_count':[], 'Transaction_amount':[]}

#path of concerned file
top_path=r"C:/Vs project/phone pe project/Data/data/top/transaction/country/india/state/"
top_state=os.listdir(top_path)

#block of code to access the concerned file and converting into dataframe
for i in top_state:
    p_i=top_path+i+root
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+root
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['districts']:
              Name=z['entityName']
              count=z['metric']['count']
              amount=z['metric']['amount']
              clm['District'].append(Name)
              clm['Transaction_count'].append(count)
              clm['Transaction_amount'].append(amount)
              clm['State'].append(i)
              clm['Year'].append(j)
              clm['Quarter'].append(int(k.strip('.json')))

Top_Trans_dist=pd.DataFrame(clm)

#lines of code to replace Nan values with respective suitable values
Top_Trans_dist= Top_Trans_dist.fillna({
        'District': "Undefined",
        'Transaction_count': 0,
        'Transaction_amount': 0
    })

#creating engine to migrate the data into sql
engine = create_engine('mysql+mysqlconnector://root:dhanushd@localhost/Phonepe', echo=False)
Top_Trans_dist.to_sql('top_trans_dist', con=engine, if_exists='replace', index=False)
engine.dispose()


#creating a list of columns for specific dataframe
clm={'State':[], 'Year':[],'Quarter':[],'Pincode':[], 'Transaction_count':[], 'Transaction_amount':[]}

#block of code to access the concerned file and converting into dataframe
for i in top_state:
    p_i=top_path+i+root
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+root
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for y in D['data']['pincodes']:
                Name=y['entityName']
                count=y['metric']['count']
                amount=y['metric']['amount']
                clm['Pincode'].append(Name)
                clm['Transaction_count'].append(count)
                clm['Transaction_amount'].append(amount)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))
Top_Trans_pin=pd.DataFrame(clm)

#lines of code to replace Nan values with respective suitable values
Top_Trans_pin= Top_Trans_pin.fillna({
        'Pincode': "Undefined",
        'Transaction_count': 0,
        'Transaction_amount': 0
    })

#creating engine to migrate the data into sql
engine = create_engine('mysql+mysqlconnector://root:dhanushd@localhost/Phonepe', echo=False)
Top_Trans_pin.to_sql('top_trans_pin', con=engine, if_exists='replace', index=False)
engine.dispose()




#creating a list of columns for specific dataframe
clm1={'State':[], 'Year':[],'Quarter':[],'Pincode':[], 'Registered_Users':[]}
clm2={'State':[], 'Year':[],'Quarter':[],'District':[], 'Registered_Users':[]}

#path of concerned file
top_path_user=r"C:/Vs project/phone pe project/Data/data/top/user/country/india/state/"
top_state_user=os.listdir(top_path_user)

#block of code to access the concerned file and converting into dataframe
for i in top_state_user:
    p_i=top_path_user+i+root
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+root
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for y in D['data']['pincodes']:
                Name=y['name']
                count=y['registeredUsers']
                clm1['Pincode'].append(Name)
                clm1['Registered_Users'].append(count)
                clm1['State'].append(i)
                clm1['Year'].append(j)
                clm1['Quarter'].append(int(k.strip('.json')))
            for y in D['data']['districts']:
                Name=y['name']
                count=y['registeredUsers']
                clm2['District'].append(Name)
                clm2['Registered_Users'].append(count)
                clm2['State'].append(i)
                clm2['Year'].append(j)
                clm2['Quarter'].append(int(k.strip('.json')))
Top_user_pin=pd.DataFrame(clm1)

#lines of code to replace Nan values with respective suitable values
Top_user_pin= Top_user_pin.fillna({
        'Pincode': "Undefined",
        'Registered_Users': 0
    })

#creating engine to migrate the data into sql
engine = create_engine('mysql+mysqlconnector://root:dhanushd@localhost/Phonepe', echo=False)
Top_user_pin.to_sql('top_user_pin', con=engine, if_exists='replace', index=False)
engine.dispose()

Top_user_dis=pd.DataFrame(clm2)

#lines of code to replace Nan values with respective suitable values
Top_user_dis= Top_user_dis.fillna({
        'District': "Undefined",
        'Registered_Users': 0
    })

#creating engine to migrate the data into sql
engine = create_engine('mysql+mysqlconnector://root:dhanushd@localhost/Phonepe', echo=False)
Top_user_dis.to_sql('top_user_dis', con=engine, if_exists='replace', index=False)
engine.dispose()



#creating a list of columns for specific dataframe
clm={'State':[], 'Year':[],'Quarter':[],'Brand':[], 'Users_Count':[],'User_Percentage':[],'Registered_Users':[],'AppOpen':[]}

#path of concerned file
agg_path_user=r"C:/Vs project/phone pe project/Data/data/aggregated/user/country/india/state/"
agg_state_user=os.listdir(agg_path_user)

#block of code to access the concerned file and converting into dataframe
for i in agg_state_user:
    p_i=agg_path_user+i+root
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+root
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            if D['data']['usersByDevice'] is not None:
                for y in D['data']['usersByDevice']:
                    Name=y['brand']
                    count=y['count']
                    per=y['percentage']
                    clm['Brand'].append(Name)
                    clm['Users_Count'].append(count)
                    clm['User_Percentage'].append(per)
                    clm['State'].append(i)
                    clm['Year'].append(j)
                    clm['Quarter'].append(int(k.strip('.json')))
                    clm['Registered_Users'].append(D['data']['aggregated']['registeredUsers'])
                    clm['AppOpen'].append(D['data']['aggregated']['appOpens'])
            else:
                Reg=D['data']['aggregated']['registeredUsers']
                count=D['data']['aggregated']['appOpens']
                clm['Registered_Users'].append(Reg)
                clm['AppOpen'].append(count)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))
                clm['Brand'].append(None)
                clm['Users_Count'].append(None)
                clm['User_Percentage'].append(None)
agg_user=pd.DataFrame(clm)

#lines of code to replace Nan values with respective suitable values
agg_user= agg_user.fillna({
        'Brand': "Undefined",
        'Registered_Users': 0,
        'Users_Count':0,
        'User_Percentage':0,
        'AppOpen':0
    })

#creating engine to migrate the data into sql
engine = create_engine('mysql+mysqlconnector://root:dhanushd@localhost/Phonepe', echo=False)
agg_user.to_sql('agg_user', con=engine, if_exists='replace', index=False)
engine.dispose()



#creating a list of columns for specific dataframe
clm={'State':[], 'Year':[],'Quarter':[],'Registered_Users':[],'AppOpen':[],'District':[]}

#path of concerned file
map_path_user=r"C:/Vs project/phone pe project/Data/data/map/user/hover/country/india/state/"
map_state_user=os.listdir(map_path_user)

#block of code to access the concerned file and converting into dataframe
for i in map_state_user:
    p_i=map_path_user+i+root
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+root
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for dist,data in D['data']['hoverData'].items():
                Name=data['registeredUsers']
                count=data['appOpens']
                clm['District'].append(dist)
                clm['Registered_Users'].append(Name)
                clm['AppOpen'].append(count)
                clm['State'].append(i)
                clm['Year'].append(j)
                clm['Quarter'].append(int(k.strip('.json')))
map_user=pd.DataFrame(clm)

#lines of code to replace Nan values with respective suitable values
map_user= map_user.fillna({
        'Registered_Users': 0,
        'AppOpen':0,
        'District':"Undefined"
    })

#creating engine to migrate the data into sql
engine = create_engine('mysql+mysqlconnector://root:dhanushd@localhost/Phonepe', echo=False)
map_user.to_sql('map_user', con=engine, if_exists='replace', index=False)
engine.dispose()


comb_df=pd.concat([Agg_Trans,Map_Trans,Top_Trans_dist,Top_Trans_pin,Top_user_pin,Top_user_dis,agg_user,map_user])
comb_df= comb_df.fillna({
        'Registered_Users': 0,
        'AppOpen':0,
        'District':"Undefined",
        'Transacion_type':"Undefined",
        'Transacion_count':0,
        'Transacion_amount':0,
        'Brand':"Undefined",
        'Pincode':"Undefined",
        'Users_Count':0,
        'User_Percentage':0
    })

#creating engine to migrate the data into sql
engine = create_engine('mysql+mysqlconnector://root:dhanushd@localhost/Phonepe', echo=False)
comb_df.to_sql('comb_df', con=engine, if_exists='replace', index=False)
engine.dispose()
#DATA VISUALIZATION IN STREAMLIT APPLICATION

# ----------------------------------------- Importing Required Packages ---------------------------------#
import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import json
import mysql.connector
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings("ignore", category=UserWarning)  # to neglect warnings thats not serious

# ----------------------------- Establishing connection to MySQL to fetch the stored data -------------#
mycon = mysql.connector.connect(host="127.0.0.1", user="root", password="dhanushd")
mycursor = mycon.cursor()

#------------------------------------Streamlit Application Setup -----------------------------------#
st.set_page_config(page_title="PhonePe Data API", page_icon=':money_with_wings:', layout='wide')
nav = st.sidebar.radio("Navigation panel",['HOME','About Page'])

if nav=='HOME':
    st.title(":chart: PhonePe Data Visualisation")
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Select year")
        year = st.selectbox("Select a year range", [2018, 2019, 2020, 2021, 2022, 2023])

    with col2:
        st.subheader("Select Quarter")
        abd = st.selectbox("Dropdown", [1, 2, 3, 4])

    year = int(year)
    quarter = int(abd)

    # Displaying Error when 2023-Q4
    if year == 2023 and quarter == 4:
        st.error("NO DATA AVAILABLE TO DISPLAY FOR 4th QUARTER OF 2023")
    else:
        # creating a list of state names
        stt = f"SELECT DISTINCT State FROM phonepe.map_trans"
        state_df = pd.read_sql(stt, con=mycon)
        state_list = state_df['State'].tolist()
        # creating tabs for visually appealing with whereas data visualization
        tab1, tab2, tab3, tab4, tab5 = st.tabs(
            ['StateWise Analysis', 'DistrictWise Analysis', 'PinCodeWise Analysis', 'CategoryWise Analysis',
             'NationWide Analysis'])

        with tab1:  # when switched for 1st Tab
            x = st.selectbox("Select STATE", state_list, key='sb1')  # let user to select state from list of states
            Trans_val_state = []  # creating a empty list to store the transaction values
            Recharge_query = f"SELECT DISTINCT * FROM phonepe.agg_trans WHERE State = '{x}' AND Year = '{year}' AND Quarter ='{quarter}' AND Transaction_type = 'Recharge & bill payments';"
            Rec_df = pd.read_sql(Recharge_query, con=mycon)
            Rec_list = Rec_df.values.tolist()
            Trans_val_state.append(
                Rec_list[0][5] if len(Rec_list) != 0 else 0)  # returns total amount accounted on recharge
            peer_query = f"SELECT DISTINCT * FROM phonepe.agg_trans WHERE State = '{x}' AND Year = '{year}' AND Quarter ='{quarter}' AND Transaction_type = 'Peer-to-peer payments';"
            peer_df = pd.read_sql(peer_query, con=mycon)
            peer_list = peer_df.values.tolist()
            Trans_val_state.append(
                peer_list[0][5] if len(peer_list) != 0 else 0)  # returns total amount accounted on peer transfer
            Merch_query = f"SELECT DISTINCT * FROM phonepe.agg_trans WHERE State = '{x}' AND Year = '{year}' AND Quarter ='{quarter}' AND Transaction_type ='Merchant payments';"
            Merch_df = pd.read_sql(Merch_query, con=mycon)
            Merch_list = Merch_df.values.tolist()
            Trans_val_state.append(
                Merch_list[0][5] if len(Merch_list) != 0 else 0)  # returns total amount accounted on merchant payments
            fin_query = f"SELECT DISTINCT * FROM phonepe.agg_trans WHERE State = '{x}' AND Year = '{year}' AND Quarter ='{quarter}' AND Transaction_type ='Financial Services';"
            fin_df = pd.read_sql(fin_query, con=mycon)
            fin_list = fin_df.values.tolist()
            Trans_val_state.append(fin_list[0][5] if len(fin_list) != 0 else 0)  # returns total amount accounted on finance
            oth_query = f"SELECT DISTINCT * FROM phonepe.agg_trans WHERE State = '{x}' AND Year = '{year}' AND Quarter ='{quarter}' AND Transaction_type ='Others';"
            oth_df = pd.read_sql(oth_query, con=mycon)
            oth_list = oth_df.values.tolist()
            Trans_val_state.append(
                oth_list[0][5] if len(oth_list) != 0 else 0)  # returns total amount accounted on other categories
            brand_query = f"SELECT DISTINCT Brand FROM phonepe.agg_user WHERE State='{x}' AND Year= '{year}' AND Quarter ='{quarter}'"
            brand_df = pd.read_sql(brand_query, con=mycon)
            brand_list = brand_df['Brand'].tolist()  # returns a list of brand name
            brand_count_list = []  # creating a empty list to store users of concerned brand
            for c in brand_list:
                brand_count_query = f"SELECT DISTINCT * FROM phonepe.agg_user WHERE State = '{x}' AND Year = '{year}' AND Quarter ='{quarter}' AND Brand = '{c}';"
                brand_count_df = pd.read_sql(brand_count_query, con=mycon)
                brand_count_lists = brand_count_df.values.tolist()
                brand_count_list.append(brand_count_lists[0][4] if len(
                    brand_count_lists) != 0 else 0)  # returns total users with respect to the brand

            col41, col42 = st.columns(2)  # diving the page into two equal sections
            with col41:  # with first half section
                category = ['Recharge & Bill Payments', 'Peer to Peer Payments', 'Merchant Payments', 'Financial Services',
                            'Others']  # list of title
                cols = ['#4C8BE2', '#00e061', '#fe073a', '#e1bc3a', '#4e8e71']  # list of color code for pie visualization
                exp = [1.5, 0.02, 0.9, 0.01, 1.2]  # list of spacing in pie
                # block of code to create a pie chart
                plt.pie(Trans_val_state, labels=category,
                        textprops=dict(size=20, color='black'),
                        radius=3,
                        colors=cols,
                        autopct='%2.2f%%',
                        explode=exp,
                        shadow=True,
                        startangle=45)
                plt.title('Transaction Type\n\n\n\n\n', color='#ed0000', size=50)
                st.pyplot(plt)
            with col42:  # with second half section
                if brand_list[0]!='Undefined':
                    # block of code to create a bar chart
                    plt.figure(figsize=(8, 6))
                    plt.bar(brand_list, brand_count_list, color="blue")
                    plt.xticks(rotation=90)
                    plt.xlabel('Brand')
                    plt.ylabel('UserCount')
                    plt.title('Bar Plot Example')
                    for i, j in zip(brand_list, brand_count_list):
                        plt.annotate(str(int(j)),
                                     xy=(i, j + 3),
                                     color='black',
                                     size='7')
                    st.pyplot(plt)
                else:
                    st.info("No User data found to Visualize")

        with tab2:  # when switched for 2nd Tab
            xx = st.selectbox("Select STATE", state_list, key='sb2')  # let user to select state from list of states
            district_query = f"SELECT DISTINCT District FROM phonepe.top_trans_dist WHERE State='{xx}' AND Year= '{year}' AND Quarter ='{quarter}';"
            district_df = pd.read_sql(district_query, con=mycon)
            district_list = district_df['District'].tolist()  # returns a list of district in selected state
            dist_trans_val = []  # creating a empty list to store transaction value of respective district
            for k in district_list:
                dist_trans_query = f"SELECT DISTINCT * FROM phonepe.top_trans_dist WHERE State = '{xx}' AND Year = '{year}' AND Quarter ='{quarter}' AND District = '{k}';"
                dist_trans_df = pd.read_sql(dist_trans_query, con=mycon)
                dist_trans_list = dist_trans_df.values.tolist()
                dist_trans_val.append(dist_trans_list[0][5] if len(
                    dist_trans_list) != 0 else 0)  # returns transaction value of respective district
            dist_user_val = []  # creating a empty list to store user count of respective district
            for l in district_list:
                dist_user_query = f"SELECT DISTINCT * FROM phonepe.top_user_dis WHERE State = '{xx}' AND Year = '{year}' AND Quarter ='{quarter}' AND District = '{l}';"
                dist_user_df = pd.read_sql(dist_user_query, con=mycon)
                dist_user_list = dist_user_df.values.tolist()
                dist_user_val.append(dist_user_list[0][4] if len(
                    dist_user_list) != 0 else 0)  # returns a list of user count of respective district

            col3, col4 = st.columns(2)  # diving the page into two equal sections
            with col3:  # with first half section
                # block of code to create a bar chart
                plt.figure(figsize=(8, 6))
                plt.bar(district_list, dist_trans_val, color="blue")
                plt.xticks(rotation=90)
                plt.xlabel('District')
                plt.ylabel('TransAmount')
                plt.title('DistrictWise Transaction Analysis')
                # ax = plt.axes()
                for i, j in zip(district_list, dist_trans_val):
                    plt.annotate(str(int(j)),
                                 xy=(i, j + 3),
                                 color='black',
                                 size='7')
                st.pyplot(plt)
            with col4:  # with second half section
                # block of code to create a bar chart
                plt.figure(figsize=(8, 6))
                plt.bar(district_list, dist_user_val, color="blue")
                plt.xticks(rotation=90)
                plt.xlabel('District')
                plt.ylabel('UserCount')
                plt.title('DistrictWise User Analysis')
                for i, j in zip(district_list, dist_user_val):
                    plt.annotate(str(int(j)),
                                 xy=(i, j + 3),
                                 color='black',
                                 size='7')
                st.pyplot(plt)

        with tab3:  # when switched for 3rd Tab
            x = st.selectbox("Select STATE", state_list, key='sb3')  # let user to select state from list of states
            pincode_trans_query = f"SELECT DISTINCT Pincode FROM phonepe.top_trans_pin WHERE State='{x}' AND Year= '{year}' AND Quarter ='{quarter}';"
            pincode_trans_df = pd.read_sql(pincode_trans_query, con=mycon)
            pincode_trans_list = pincode_trans_df[
                'Pincode'].tolist()  # returns a list of pincode where transaction data available
            pincode_user_query = f"SELECT DISTINCT Pincode FROM phonepe.top_user_pin WHERE State='{x}' AND Year= '{year}' AND Quarter ='{quarter}';"
            pincode_user_df = pd.read_sql(pincode_user_query, con=mycon)
            pincode_user_list = pincode_user_df['Pincode'].tolist()  # returns a list of pincode where user data available
            pincode_trans_val = []  # creating an empty list to store the pincodewise transaction data
            for k in pincode_trans_list:
                pin_trans_query = f"SELECT DISTINCT * FROM phonepe.top_trans_pin WHERE State = '{x}' AND Year = '{year}' AND Quarter ='{quarter}' AND Pincode = '{k}';"
                pin_trans_df = pd.read_sql(pin_trans_query, con=mycon)
                pin_trans_list = pin_trans_df.values.tolist()
                pincode_trans_val.append(pin_trans_list[0][5] if len(
                    pin_trans_list) != 0 else 0)  # returns a list of pincodewise transaction data
            pincode_user_val = []  # creating an empty list to store the pincodewise user data
            for l in pincode_user_list:
                pin_user_query = f"SELECT DISTINCT * FROM phonepe.top_user_pin WHERE State = '{x}' AND Year = '{year}' AND Quarter ='{quarter}' AND Pincode = '{l}';"
                pin_user_df = pd.read_sql(pin_user_query, con=mycon)
                pin_user_list = pin_user_df.values.tolist()
                pincode_user_val.append(
                    pin_user_list[0][4] if len(pin_user_list) != 0 else 0)  # returns a list of pincodewise user data

            col5, col6 = st.columns(2)  # diving the page into two equal sections
            with col5:  # with first half section
                # block of code to consrtuct the bar chart
                plt.figure(figsize=(8, 6))
                plt.bar(pincode_trans_list, pincode_trans_val, color="blue")
                plt.xticks(rotation=90)
                plt.xlabel('PINCODE')
                plt.ylabel('TransAmount')
                plt.title('PINCODEWise Transaction Analysis')
                # ax = plt.axes()
                for i, j in zip(pincode_trans_list, pincode_trans_val):
                    plt.annotate(str(int(j)),
                                 xy=(i, j + 3),
                                 color='black',
                                 size='7')
                st.pyplot(plt)
            with col6:  # with second half section
                # block of code to consrtuct the bar chart
                plt.figure(figsize=(8, 6))
                plt.bar(pincode_user_list, pincode_user_val, color="blue")
                plt.xticks(rotation=90)
                plt.xlabel('PINCODE')
                plt.ylabel('UserCount')
                plt.title('PINCODEWise User Analysis')
                for i, j in zip(pincode_user_list, pincode_user_val):
                    plt.annotate(str(int(j)),
                                 xy=(i, j + 3),
                                 color='black',
                                 size='7')
                st.pyplot(plt)

        with tab4:  # when switched for 4th Tab
            type_query = f"SELECT DISTINCT Transaction_type FROM phonepe.agg_trans"
            type_df = pd.read_sql(type_query, con=mycon)
            type_list = type_df['Transaction_type'].tolist()
            x = st.selectbox("Select Type of Transaction",
                             type_list)  # let user to select Type of transaction from list of transaction types
            type_trans_val = []  # creates an empty list to store typewise trans amount
            for ii in state_list:
                type_trans_query = f"SELECT DISTINCT * FROM phonepe.agg_trans WHERE State='{ii}' AND Year = '{year}' AND Quarter ='{quarter}' AND Transaction_type = '{x}';"
                type_trans_df = pd.read_sql(type_trans_query, con=mycon)
                type_trans_list = type_trans_df.values.tolist()
                type_trans_val.append(
                    type_trans_list[0][5] if len(type_trans_list) != 0 else 0)  # returns a list of typewise trans amount
            type_trans_count = []  # creates an empty list to store typewise trans count
            for ii in state_list:
                type_trans_query = f"SELECT DISTINCT * FROM phonepe.agg_trans WHERE State='{ii}' AND Year = '{year}' AND Quarter ='{quarter}' AND Transaction_type = '{x}';"
                type_trans_df = pd.read_sql(type_trans_query, con=mycon)
                type_trans_list = type_trans_df.values.tolist()
                type_trans_count.append(
                    type_trans_list[0][4] if len(type_trans_list) != 0 else 0)  # returns a list of typewise trans count
            col7, col8 = st.columns(2)  # diving the page into two equal sections
            with col7:  # with first half section
                # block of code to consrtuct the bar chart
                plt.figure(figsize=(20, 10))
                ax = plt.axes()
                ax.set_facecolor('black')
                ax.grid(linewidth=0.4, color='#8f8f8f')
                plt.xticks(rotation='vertical', size='20', color='green')
                plt.yticks(size='20', color='red')
                ax.set_xlabel('\nStates', size=25, color='brown')
                ax.set_ylabel('Transaction Count\n', size=25, color='brown')
                plt.tick_params(size=20, color='white')
                ax.set_title('Category wise breakdown of Transaction Count\n', size=50, color='violet')
                plt.bar(state_list, type_trans_count, label='re')
                for i, j in zip(state_list, type_trans_count):
                    ax.annotate(str(int(j)),
                                xy=(i, j + 3),
                                color='white',
                                size='15')
                st.pyplot(plt)
            with col8:  # with second half section
                # block of code to consrtuct the bar chart
                plt.figure(figsize=(20, 10))
                ax = plt.axes()
                ax.set_facecolor('black')
                ax.grid(linewidth=0.4, color='#8f8f8f')
                plt.xticks(rotation='vertical', size='20', color='green')
                plt.yticks(size='20', color='red')
                ax.set_xlabel('\nStates', size=25, color='brown')
                ax.set_ylabel('Transaction Value\n', size=25, color='brown')
                plt.tick_params(size=20, color='white')
                ax.set_title('Category wise breakdown of Transaction Value\n', size=50, color='violet')
                plt.bar(state_list, type_trans_val, label='re')
                for i, j in zip(state_list, type_trans_val):
                    ax.annotate(str(int(j)),
                                xy=(i, j + 3),
                                color='white',
                                size='15')
                st.pyplot(plt)

        with tab5:  # when switched for 5th Tab
            maps = st.selectbox("Select Option", ['NationWide Transaction Analysis',
                                                  'NationWide User Analysis'])  # let the user select from list
            if maps == 'NationWide Transaction Analysis':
                # loading json file that consits of india geological data
                india_states = json.load(open(r"C:\Users\DELL\Downloads\states_india.geojson","r"))
                state_id_map = {}
                # assigning key value to access
                for feature in india_states["features"]:
                    feature["id"] = feature["properties"]["state_code"]
                    state_id_map[feature["properties"]["st_nm"]] = feature["id"]
                # creating a dict file to store data required for visualization
                state_data = {'State': state_list,
                              "Total_Amount": []}
                for i in state_list:
                    total_trans_query = f"SELECT SUM(Transaction_amount) as NET_AMOUNT FROM phonepe.agg_trans WHERE State='{i}' AND Year = '{year}' AND Quarter ='{quarter}';"
                    total_trans_df = pd.read_sql(total_trans_query, con=mycon)
                    total_trans_list = total_trans_df.values.tolist()
                    state_data['Total_Amount'].append(total_trans_list[0][0] if len(
                        total_trans_list) != 0 else 0)  # returns cumulative Transaction Amount of respective state

                state_df = pd.DataFrame(state_data)  # converting dict to dataframe

                state_df["State"] = state_df["State"].str.capitalize()  # formatting state name to match as per GeoJson

                state_name_mapping = {'Arunachal-pradesh': 'Arunanchal-Pradesh',
                                      'Dadra-&-nagar-haveli-&-daman-&-diu': 'Dadara & Nagar Havelli',
                                      'Delhi': 'NCT of Delhi',
                                      'Andaman-&-nicobar-islands': 'Andaman & Nicobar Island',
                                      'Andhra-pradesh': 'Andhra Pradesh',
                                      'Himachal-pradesh': 'Himachal Pradesh',
                                      'Jammu-&-kashmir': "Jammu & Kashmir",
                                      'Madhya-pradesh': 'Madhya Pradesh',
                                      'Tamil-nadu': 'Tamil Nadu',
                                      'Uttar-pradesh': 'Uttar Pradesh',
                                      'West-bengal': 'West Bengal'
                                      }
                state_df['State'] = state_df['State'].replace(state_name_mapping,
                                                              regex=True)  # formatting state name to match as per GeoJson
                condition_to_delete = state_df['State'] == 'Ladakh'
                state_df = state_df[
                    ~condition_to_delete]  # deleting ladakh from dataframe as GeoJson doesn't contain ladakh to avoid error
                state_df["State"] = state_df["State"].apply(
                    lambda x: x.replace("-", " "))  # formatting state name to match as per GeoJson
                state_df["id"] = state_df["State"].apply(
                    lambda x: state_id_map[x])  # accessing with key value to access GeoJson

                state_df["TransactionScale"] = np.log10(state_df["Total_Amount"])  # defining a color scale
                # block of code to construct map
                fig = px.choropleth_mapbox(
                    state_df,
                    locations="id",
                    geojson=india_states,
                    color="TransactionScale",
                    hover_name="State",
                    hover_data=["Total_Amount"],
                    title="India Population Density",
                    mapbox_style="carto-positron",
                    center={"lat": 24, "lon": 78},
                    zoom=3,
                    opacity=0.5
                )

                st.plotly_chart(fig, use_container_width=True, help='Sample text')

            if maps == 'NationWide User Analysis':
                # loading json file that consits of india geological data
                india_states = json.load(open("D:\DOWNLOADS\states_india.geojson", "r"))
                state_id_map = {}
                # assigning key value to access
                for feature in india_states["features"]:
                    feature["id"] = feature["properties"]["state_code"]
                    state_id_map[feature["properties"]["st_nm"]] = feature["id"]
                # creating a dict file to store data required for visualization
                state_user_data = {'State': state_list,
                                   "Total_Users": []}
                for i in state_list:
                    total_user_query = f"SELECT Registered_Users FROM phonepe.agg_user WHERE State='{i}' AND Year = '{year}' AND Quarter ='{quarter}';"
                    total_user_df = pd.read_sql(total_user_query, con=mycon)
                    total_user_list = total_user_df.values.tolist()
                    state_user_data['Total_Users'].append(total_user_list[0][0] if len(
                        total_user_list) != 0 else 0)  # returns cumulative users of respective state

                state_user_df = pd.DataFrame(state_user_data)  # converting dict to dataframe

                state_user_df["State"] = state_user_df[
                    "State"].str.capitalize()  # formatting state name to match as per GeoJson

                state_name_mapping = {'Arunachal-pradesh': 'Arunanchal-Pradesh',
                                      'Dadra-&-nagar-haveli-&-daman-&-diu': 'Dadara & Nagar Havelli',
                                      'Delhi': 'NCT of Delhi',
                                      'Andaman-&-nicobar-islands': 'Andaman & Nicobar Island',
                                      'Andhra-pradesh': 'Andhra Pradesh',
                                      'Himachal-pradesh': 'Himachal Pradesh',
                                      'Jammu-&-kashmir': "Jammu & Kashmir",
                                      'Madhya-pradesh': 'Madhya Pradesh',
                                      'Tamil-nadu': 'Tamil Nadu',
                                      'Uttar-pradesh': 'Uttar Pradesh',
                                      'West-bengal': 'West Bengal'
                                      }
                state_user_df['State'] = state_user_df['State'].replace(state_name_mapping,
                                                                        regex=True)  # formatting state name to match as per GeoJson
                condition_to_delete = state_user_df['State'] == 'Ladakh'
                state_user_df = state_user_df[
                    ~condition_to_delete]  # deleting ladakh from dataframe as GeoJson doesn't contain ladakh to avoid error
                state_user_df["State"] = state_user_df["State"].apply(
                    lambda x: x.replace("-", " "))  # formatting state name to match as per GeoJson
                state_user_df["id"] = state_user_df["State"].apply(
                    lambda x: state_id_map[x])  # accessing with key value to access GeoJson

                state_user_df["TransactionScale"] = np.log10(state_user_df["Total_Users"])  # defining a color scale
                # block of code to construct map
                fig = px.choropleth_mapbox(
                    state_user_df,
                    locations="id",
                    geojson=india_states,
                    color="TransactionScale",
                    hover_name="State",
                    hover_data=["Total_Users"],
                    title="India Population Density",
                    mapbox_style="carto-positron",
                    center={"lat": 24, "lon": 78},
                    zoom=3,
                    opacity=0.5
                )

                st.plotly_chart(fig, use_container_width=True, help='Sample text')

elif nav=='About Page':#display the data of Author
    st.title(":dart: About Page")
    st.subheader("**Project by :**")
    st.markdown("#### DHANUSH RAJ")
    st.markdown("### GITHUB Profile")
    st.info("https://github.com/DhanushPrakash2311")
