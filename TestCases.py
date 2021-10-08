# -*- coding: utf-8 -*-
"""
Created on Sat Dec 14 14:05:36 2019

@author: Dvara
"""

import os
import pandas as pd
import pymssql
from sqlalchemy import create_engine
from pyexcel.cookbook import merge_all_to_a_book
import glob
import numpy as np

'''

cllist=list(df.columns.values)
print(cllist)
print(str(df.iloc[2:3,1:6]))
#for index,Data in df.iterrows():
'''

file='Data.csv'
def generatedf(file):    
    df=pd.read_csv(file)
    #REF https://stackoverflow.com/questions/42786804/concatenate-all-columns-in-a-pandas-dataframe
    #cols = list(['DD','MM','YY','Date','MaxTemp','MinTemp','IMDRF'])
    cols=list(df.columns.values)
    for cl in cols:
        df[cl]=df[cl].astype(str)
    selectfirst='SELECT '
    ite=len(cols)
    i=1
    for cl in cols:
        if i<ite:
            selectfirst+="'noobdossier' "+str(cl)+','
        elif i==ite:
            selectfirst+="'noobdossier' "+str(cl)
        i+=1
    
    df['concat'] = pd.Series(df[cols].fillna('').values.tolist()).str.join("','")
    return df,selectfirst,cols
    
    
#print(type(df['DD']))  
#print(df)
def create_conn():
    server = 'host'
    db = 'cam'
    user = 'user'
    pwd = 'password'
    conn = pymssql.connect(server=server, user=user, password=pwd, database=db)
    return conn

def getDbdata():
    query="SELECT * FROM [dbo].[tbl_perdix_enrollment]"
    conn=create_conn()
    df=pd.read_sql(query,conn)
    conn.close()
    cols=list(df.columns.values)
    for cl in cols:
        df[cl]=df[cl].astype(str)
    selectfirst='SELECT '
    ite=len(cols)
    i=1
    for cl in cols:
        if i<ite:
            selectfirst+="'noobdossier' "+str(cl)+','
        elif i==ite:
            selectfirst+="'noobdossier' "+str(cl)
        i+=1
    
    df['concat'] = pd.Series(df[cols].fillna('').values.tolist()).str.join("','")
    return df,selectfirst,cols
    

def directinsert(df):
    df['concat']="('"+df['concat']+"')"
    df['rownumber']=(np.arange(len(df)))+1 
    trows=len(df)
    lendf=True
    offset=0
    limit=10
    i=1
    while lendf:
        #Ref https://stackoverflow.com/questions/53934470/equivalent-of-limit-and-offset-of-sql-in-pandas
        dfs=df.sort_values(by='rownumber', ascending=True).reset_index(drop=True).loc[offset : offset+limit-1]
        offset+=10
        dfs['groupbyValue']=1
        print(dfs)
        dfselect=dfs.groupby('groupbyValue',as_index=False).apply(lambda x: ', '.join(x.concat))
        dfselect=dfselect.reset_index()
        if len(dfs) is None:        
            lendf=False
        elif len(dfs)==0:
            lendf=False
        else:
            insertQ=(dfselect.iat[0,1])
            print(insertQ)

def CREATETABLEQUERY(df):
    df.reset_index()    
    createtableQuery='CREATE TABLE #TEMPFROM ( '
    columnlist=list(df.columns.values)
    ite=len(columnlist)
    print(ite)
    i=1
    for cl in columnlist: 
        print('i'+str(i))
        print('len',str(ite))
        datatype=(df[cl].dtype)
        
        if i<ite:
            createtableQuery+=str(cl)+' '+str(datatype)+','
            print(createtableQuery)
        elif i==ite:
            createtableQuery+=str(cl)+' '+str(datatype)+')'
            print(createtableQuery)
            
        i+=1
    createtableQuery=str(datatype).replace('object','VARCHAR(NMAX)')
        
def Updateinsert(df,selectquery,cols,typeofops,tablename,tocolumns,setQuery,whereCondition):
    df['concat']="'"+df['concat']+"' UNION ALL "
    df['rownumber']=(np.arange(len(df)))+1 
    trows=len(df)
    lendf=True
    offset=0
    limit=10
    i=1
    while lendf:
        #Ref https://stackoverflow.com/questions/53934470/equivalent-of-limit-and-offset-of-sql-in-pandas
        dfs=df.sort_values(by='rownumber', ascending=True).reset_index(drop=True).loc[offset : offset+limit-1]
        offset+=10
        dfs['groupbyValue']=1
        dfselect=dfs.groupby('groupbyValue',as_index=False).apply(lambda x: 'SELECT  '.join(x.concat))
        dfselect=dfselect.reset_index()
        if len(dfs) is None:        
            lendf=False
        elif len(dfs)==0:
            lendf=False
        else:
            insertQ=(dfselect.iat[0,1])
            N = 10
            finalselect=selectquery+' UNION ALL SELECT  '+str(insertQ)
            finalselect=finalselect[:-N]
            addparenttable='SELECT af.* FROM ( '+finalselect+' )af '
        
            if typeofops=='updateif':
                print('updateif')
                updatequery='UPDATE targettable  SET '+setQuery+' FROM ' +tablename+' targettable JOIN ('+addparenttable+'WHERE '+cols[0]+"!='noobdossier' )sourcetable ON "+whereCondition
                wherenotequalto=whereCondition.replace('=','!=')
                upinsertquery='INSERT INTO '+tablename+'( '+tocolumns+') '+addparenttable +' sourcetable '+' JOIN  '+tablename+' targettable ON '+wherenotequalto +' WHERE '+cols[0]+"!='noobdossier'" 
                print(upinsertquery)
            elif typeofops=='insert':
                insertquery='INSERT INTO '+tablename+'( '+tocolumns+') '+addparenttable
                print(insertquery)
                
            
#df,selectquery,cols=generatedf(file)
df,selectquery,cols=getDbdata()
typeofops=''
#typeofops='insert'
tablename='totablename'
tocolumns='DD,MM,YY,Date,MaxTemp,MinTemp,IMDRF'
tocolumnslist=tocolumns.split(",")
setQuery=(','.join(['targettable.'+str(a)+'=sourcetable.' + b for a,b in zip(tocolumnslist,cols)]))
whereCondition='targettable.DD=sourcetable.DD'
Updateinsert(df,selectquery,cols,typeofops,tablename,tocolumns,setQuery,whereCondition)

    
    
    
