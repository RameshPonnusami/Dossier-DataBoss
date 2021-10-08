import pandas as pd
import sqlite3
import pymssql
import numpy as np


def dbconn():
    servername = 'host'
    dbName = 'db'
    username = 'user'
    pw = 'pwd'
    conn = pymssql.connect(server=servername, user=username, password=pw, database=dbName)
    return conn


def Query():
    conn = sqlite3.connect('test.db')
    print("Opened database successfully");
    QueryDist = '''
    CREATE TABLE tbl_MahaDistrict (
    	CatId nvarchar(50)   NOT NULL,
    	DistId nvarchar(50)   NOT NULL,
    	DistName nvarchar(200)   NULL,
    	CONSTRAINT tbl_MahaDistrict_pk PRIMARY KEY (CatId,DistId)
    );'''
    QueryTaluk = '''

    CREATE TABLE tbl_MahaTaluka (
    	CatId nvarchar(50)  NOT NULL,
    	DistId nvarchar(50)  NOT NULL,
    	TalukaId nvarchar(50)  NOT NULL,
    	TalukaName nvarchar(200)  NULL,
    	CONSTRAINT tbl_MahaTaluka_pk PRIMARY KEY (CatId,DistId,TalukaId)
    ) 
    '''
    QueryVillage = '''
    CREATE TABLE tbl_MahaVillage (
    	CatId nvarchar(50)   NOT NULL,
    	DistId nvarchar(50)  NOT NULL,
    	TalukaId nvarchar(50)   NOT NULL,
    	VillageId nvarchar(50)  NOT NULL,
    	VillageName nvarchar(200)   NULL,
    	CONSTRAINT tbl_MahaVillage_pk PRIMARY KEY (CatId,DistId,TalukaId,VillageId)
    ) 
    '''

    conn.execute(QueryVillage)
    print("Table created successfully");
    conn.close()


tablename = "tbl_MahaVillage"
Columns = "CatId,DistId,TalukaId,VillageId,VillageName"
insertquery = "INSERT INTO " + tablename + "(" + Columns + ") VALUES"
SourceQuery = 'select   * from tbl_MahaVillage'


def df_into_csv(SourceQuery):
    conn = dbconn()
    print("Opened database successfully");
    df = pd.read_sql(SourceQuery, conn)
    conn.close()
    return df


df = df_into_csv(SourceQuery)
print(df)


def df_details(df):
    dfcolumns = df.columns
    lendfC = len(dfcolumns)
    return dfcolumns, lendfC


dfcolumns, lendfC = df_details(df)

'''
file="cce_classification.csv"
df=dd.read_csv(file)
dfcolumns=df.columns
lendfC=len(dfcolumns)
tablename="Test"
Columns="ImageName,Stage,Health"
insertquery="INSERT INTO "+tablename+"("+Columns+") VALUES"
'''


def sqlf_into_target(insertquery):
    conn = sqlite3.connect('test.db')
    print("Opened database successfully");
    conn.execute(insertquery)
    conn.commit()
    print("Query successfully");
    conn.close()


def convert_df_into_sql(df, dfcolumns, insertquery):
    dfi = 1
    QueryValues = ''
    lendfdata = len(df)
    lendf = True
    offset = 0
    limit = 999
    df['rownumber'] = (np.arange(len(df))) + 1
    Querymsg = ''
    insertquerytemp = ''
    code = 1
    while lendf:
        insertquerytemp = insertquery
        # Ref https://stackoverflow.com/questions/53934470/equivalent-of-limit-and-offset-of-sql-in-pandas
        dfs = df.sort_values(by='rownumber', ascending=True).reset_index(drop=True).loc[offset: offset + limit - 1]
        offset += 999
        dfs['groupbyValue'] = 1
        print(len(dfs))
        lendfsdata = len(dfs)
        if len(dfs) is None or len(dfs) == 0:
            lendf = False
            break;
        dfsi = 1
        for index, data in dfs.iterrows():
            Ivalue = "("
            i = 1
            for dc in dfcolumns:
                st = "'"
                qvalue = data[dc]
                st += str(qvalue) + "'"
                if i == 1:
                    Ivalue += st
                elif i == lendfC:
                    Ivalue += "," + st + ")"
                else:
                    Ivalue += "," + st
                i += 1

            if dfsi == lendfsdata:

                QueryValues += Ivalue
                dfsi = 1

            else:

                QueryValues += Ivalue + ","
                dfsi += 1
            dfi += 1
            Ivalue = '('

        insertquerytemp += QueryValues

        sqlf_into_target(insertquerytemp)
        insertquerytemp = ''
        QueryValues = ''


convert_df_into_sql(df, dfcolumns, insertquery)