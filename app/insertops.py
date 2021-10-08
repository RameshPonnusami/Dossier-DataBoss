from . import dossCommon
from .audit import ReadGlobalVariables
from config import UPLOAD_FOLDER
import pandas as pd
from .CSVtoDB import convert_df_into_sql
rgv=ReadGlobalVariables()
def insertmastermethod(item,globalvariabledict):
    flagcout = True
    masteroffset = 0
    masterlimit = 50000
    code = 404
    Querymsg='Nothing Happened'
    while flagcout:
        connection1,engine1=dossCommon.db_conn1(item.fromserver_id)
        fromquery = rgv.global_variables_for_jobs_with_query(item, globalvariabledict)
        if engine1.url.drivername=='mysql':
            fquery = fromquery +' ORDER BY '+item.orderby+ " limit " + str(masteroffset) + " ," + str(masterlimit)
        if  engine1.url.drivername=='postgresql':#postgres has different systax for offset
            fquery = fromquery + ' ORDER BY ' + item.orderby + " limit " + str(masterlimit) + " OFFSET " + str(masteroffset)

        if engine1.url.drivername=='mssql+pymssql' or engine1.url.drivername=='mssql+pyodbc':
            fquery = fromquery +' ORDER BY '+item.orderby+ " OFFSET " + str(masteroffset) + " ROWS   FETCH   FIRST  " + str(masterlimit) +" ROWS   ONLY "
        masteroffset += 50000

        df, selectfirst, cols=dossCommon.get_select_query(connection1,fquery)
        print(fquery)
        totalrows = len(df)
        if totalrows == 0:
            flagcout = False
            masteroffset += 404
            break;
        wherelistnull,setQuery = dossCommon.whereconditioncolumn(item.tocolumns, str(item.toserverquery),cols)
        connection2, engine2 = dossCommon.db_conn2(item.toserver_id)
        Querymsg,code=dossCommon.Updateinsert(df, selectfirst, cols, item.Inserttype, item.toserverquery, item.tocolumns, setQuery,
                     item.whereforUpdateifexists, item.toserver_id, wherelistnull,connection2, engine2)
        if code==0:
            break;
    if flagcout == False and masteroffset==404:
        connection2.close()
    elif flagcout == False:
        connection2.close()
        connection1.close()

    return Querymsg,code


def singlemastermethod(item,globalvariabledict):
    connection1, engine1 = dossCommon.db_conn1(item.fromserver_id)
    query=rgv.global_variables_for_jobs_with_query(item,globalvariabledict)
    Querymsg, code = dossCommon.query_execution(connection1, query)
    connection1.close()
    return Querymsg,code

def loopmastermethod(item,globalvariabledict):
    print("loop")
    flagcout = True
    masteroffset = 0
    masterlimit = 50000
    code = 404
    Querymsg = 'Nothing Happened'
    toserverquery=item.toserverquery
    while flagcout:
        connection1, engine1 = dossCommon.db_conn1(item.fromserver_id)
        fromquery = rgv.global_variables_for_jobs_with_query(item, globalvariabledict)
        if engine1.url.drivername == 'mysql':
            fquery = fromquery + ' ORDER BY ' + item.orderby + " limit " + str(masteroffset) + " ," + str(masterlimit)
        if engine1.url.drivername == 'postgresql':  # postgres has different systax for offset
            fquery = fromquery + ' ORDER BY ' + item.orderby + " limit " + str(masterlimit) + " OFFSET " + str(
                masteroffset)

        if engine1.url.drivername == 'mssql+pymssql' or engine1.url.drivername == 'mssql+pyodbc':
            fquery = fromquery + ' ORDER BY ' + item.orderby + " OFFSET " + str(
                masteroffset) + " ROWS   FETCH   FIRST  " + str(masterlimit) + " ROWS   ONLY "

        masteroffset += 50000
        df, selectfirst, cols = dossCommon.get_select_query(connection1, fquery)
        totalrows = len(df)
        if totalrows == 0:
            flagcout = False
            break;
        for index,data in df.iterrows():
            toserverquery_temp = toserverquery
            for col in cols:
                val=data[col]
                col='$'+str(col)
               # val ="'"+ val+"'"
                val=val
                toserverquery_temp=toserverquery_temp.replace(col,val)
               # print(toserverquery_temp)
            Querymsg, code = dossCommon.query_execution(connection1, toserverquery_temp)
            # if code==0:
            #     flagcout = False
            #     break;
        connection1.close()
        return Querymsg, code

def migratemastermethod(item,globalvariabledict):
    flagcout = True
    masteroffset = 0
    masterlimit = 50000
    code = 404
    Querymsg='Nothing Happened'
    while flagcout:
        connection1,engine1=dossCommon.db_conn1(item.fromserver_id)
        fromquery = rgv.global_variables_for_jobs_with_query(item, globalvariabledict)
        if engine1.url.drivername=='mysql':
            fquery = fromquery +' ORDER BY '+item.orderby+ " limit " + str(masteroffset) + " ," + str(masterlimit)
        if engine1.url.drivername=='mssql+pymssql' or engine1.url.drivername=='mssql+pyodbc':
            fquery = fromquery +' ORDER BY '+item.orderby+ " OFFSET " + str(masteroffset) + " ROWS   FETCH   FIRST  " + str(masterlimit) +" ROWS   ONLY "
        masteroffset+=50000
        df, selectfirst, cols=dossCommon.get_select_query(connection1,fquery)
        totalrows = len(df)
        if totalrows == 0:
            flagcout = False
            masteroffset += 404
            break;
        wherelistnull,setQuery = dossCommon.whereconditioncolumn(item.tocolumns, str(item.toserverquery),cols)
        connection2, engine2 = dossCommon.db_conn2(item.toserver_id)
        dossCommon.migrate.delay(df, selectfirst, cols, item.Inserttype, item.toserverquery, item.tocolumns, setQuery,
                     item.whereforUpdateifexists, item.toserver_id, wherelistnull,connection2, engine2)

    if flagcout == False and masteroffset==404:
        connection2.close()
    connection1.close()

    return Querymsg,code


def csvtodbmastermethod(item):
    connection2, engine2 = dossCommon.db_conn2(item.toserver_id)
    filepath= UPLOAD_FOLDER+'/'+item.file
    df = pd.read_csv(filepath)
    tablename = item.toserverquery
    Columns = item.tocolumns
    insertquery = "INSERT INTO " + tablename + "(" + Columns + ") VALUES"
    Querymsg,code = convert_df_into_sql(df=df, dfcolumns = df.columns,insertquery= insertquery,lendfC=len(df),connection=connection2)
    return  Querymsg,code


