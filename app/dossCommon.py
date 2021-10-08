from . import   db
from .models import  Bucket,GlobalVariablesForJobs,Process,Job,GlobalVariables
import sqlalchemy
import pandas as pd
import numpy as np
from . import celery


def db_conn1(serverid):
    fromserverdetails = db.session.query(Bucket).filter(Bucket.id == serverid).first()
    engine1 = sqlalchemy.create_engine(str(fromserverdetails.conn), convert_unicode=True)
    connection1 = engine1.connect()
    return connection1,engine1

def get_db_values(serverid):
    serverdetails = db.session.query(Bucket).filter(Bucket.id == serverid).first()
    return serverdetails

def get_job_values(jobid):
    jobdetails=db.session.query(Job).filter(Job.id==jobid).first()
    return jobdetails

def db_conn2(serverid):
    fromserverdetails = db.session.query(Bucket).filter(Bucket.id == serverid).first()
    engine2 = sqlalchemy.create_engine(str(fromserverdetails.conn), convert_unicode=True)
    connection2 = engine2.connect()
    return connection2,engine2

def processlistperjob(jobid):
    processdetails=db.session.query(Process).filter(Process.job_id==jobid).all()
    return processdetails

def processlist(processidlist):
    processdetails=db.session.query(Process).filter(Process.id.in_((processidlist))).all()
    return processdetails


def get_select_query(connection,fromquery,initial=0):
    df = pd.read_sql_query(str(fromquery), connection)
    dictionary = {"'": "''"}
    if len(df)==0:
        return df,'emptyselect','empty'
    if initial==0:
        cols = list(df.columns.values)
        for cl in cols:
            df[cl] = df[cl].astype(str)
        df.replace(dictionary, regex=True, inplace=True)
        selectfirst = 'SELECT '
        ite = len(cols)
        i = 1
        for cl in cols:
            if i < ite:
                selectfirst += "'noobdossier' " + str(cl) + ','
            elif i == ite:
                selectfirst += "'noobdossier' " + str(cl)
            i += 1
        df['concat'] = pd.Series(df[cols].fillna('').values.tolist()).str.join("','")
        return df, selectfirst, cols


def whereconditioncolumn(tocolumns,toserverquery,cols):
    tocolumnslist = tocolumns.split(",")
    lcl = len(tocolumnslist)
    wherelistnull = ''
    ic = 1
    for tcl in tocolumnslist:
        if lcl > ic:
            wherelistnull += toserverquery + '.' + tcl + ' IS NULL AND  '
        elif lcl == ic:
            wherelistnull += toserverquery + '.' + tcl + ' IS NULL'
        ic += 1
    setQuery = (','.join([str(toserverquery) + '.' + str(a) + '=sourcetable.' + b for a, b in zip(tocolumnslist, cols)]))
    return wherelistnull,setQuery

def generate_update_query(setQuery,tablename,addparenttable,cols,whereCondition,tocolumns,enginetype):
    if enginetype == 'mssql':
        updatequery = 'UPDATE  targettable ' + '  SET ' + setQuery + ' FROM ' + tablename + ' targettable JOIN (' + addparenttable + 'WHERE ' + \
                      cols[0] + "!='noobdossier' )sourcetable ON " + whereCondition
    if enginetype == 'mysql':
        updatequery = 'UPDATE ' + tablename + ' JOIN (' + addparenttable + 'WHERE ' + cols[
            0] + "!='noobdossier' )sourcetable ON " + whereCondition + '  SET ' + setQuery
    return  updatequery

def generate_updateinsert_query(tablename,addparenttable,cols,whereCondition,tocolumns,wherelistnull,enginetype):
    if enginetype == 'mssql':
        wherenotequalto = whereCondition.replace('=', '!=')
        upinsertquery = 'INSERT INTO ' + tablename + '( ' + tocolumns + ') ' + addparenttable + ' sourcetable ' + ' JOIN  ' + tablename + ' targettable ON ' + wherenotequalto + ' WHERE ' + \
                    cols[0] + "!='noobdossier'"
    if enginetype == 'mysql':
        wherenotequaltoinsert = whereCondition.replace('=', '=')
        addparenttableinsert = addparenttable.replace('af', 'sourcetable')
        upinsertquery = 'INSERT INTO ' + tablename + '( ' + tocolumns + ') ' + addparenttableinsert + ' LEFT   JOIN  ' + tablename + ' ON ' + wherenotequaltoinsert + ' WHERE sourcetable.' + \
                        cols[0] + "!='noobdossier' AND " + wherelistnull
    return  upinsertquery


def Updateinsert(df, selectquery, cols, typeofops, tablename, tocolumns, setQuery, whereCondition,toserver,wherelistnull,connection2, engine2):
    if engine2.url.drivername=='postgresql':
        df['concat'] = "('" + df['concat'] + "')"
    else:
        df['concat'] = "'" + df['concat'] + "' UNION ALL "
    df['rownumber'] = (np.arange(len(df))) + 1
    trows = len(df)
    lendf = True
    offset = 0
    limit = 999
    i = 1
    Querymsg=''
    code=1
    while lendf:
        # Ref https://stackoverflow.com/questions/53934470/equivalent-of-limit-and-offset-of-sql-in-pandas
        dfs = df.sort_values(by='rownumber', ascending=True).reset_index(drop=True).loc[offset: offset + limit - 1]
        offset += 999
        dfs['groupbyValue'] = 1
        if engine2.url.drivername == 'postgresql':
            dfselect = dfs.groupby('groupbyValue', as_index=False).apply(lambda x: ',  '.join(x.concat))
        else:
            dfselect = dfs.groupby('groupbyValue', as_index=False).apply(lambda x: 'SELECT  '.join(x.concat))
        dfselect = dfselect.reset_index()
        if len(dfs) is None:
            lendf = False
        elif len(dfs) == 0:
            lendf = False
        else:
            insertQ = (dfselect.iat[0, 1])
            N = 10
            if engine2.url.drivername == 'postgresql':
                finalselect = ' VALUES  ' + str(insertQ)
                addparenttable=finalselect

            else:
                finalselect = selectquery + ' UNION ALL SELECT  ' + str(insertQ)
                finalselect = finalselect[:-N]
                addparenttable = 'SELECT af.* FROM ( ' + finalselect + ' )af '
            if typeofops == 'UpdateIfExists':
                updatequery=generate_update_query(setQuery,tablename,addparenttable,cols,whereCondition,tocolumns,enginetype=engine2.url.drivername)
                upinsertquery=generate_updateinsert_query(tablename,addparenttable,cols,whereCondition,tocolumns,wherelistnull,enginetype=engine2.url.drivername)
                upinsertquery=upinsertquery.replace("'None'",'NULL')
                upinsertquery = upinsertquery.replace("'NaT'", 'NULL')
                upinsertquery = upinsertquery.replace("'nan'", 'NULL')

                updatequery = updatequery.replace("'nan'", 'NULL')
                updatequery=updatequery.replace("'None'",'NULL')
                updatequery = updatequery.replace("'NaT'", 'NULL')

                Querymsg,code=query_execution(connection2,updatequery)
                if code==0:
                    break;
                Querymsg,code=query_execution(connection2,upinsertquery)
                if code==0:
                    break;
            elif typeofops == 'InsertDirectly':
                if engine2.url.drivername == 'postgresql':
                    insertquery = 'INSERT INTO ' + tablename + '( ' + tocolumns + ') ' + addparenttable
                else:
                    insertquery = 'INSERT INTO ' + tablename + '( ' + tocolumns + ') ' + addparenttable +'WHERE af.'+ cols[0] + "!='noobdossier'"
                insertquery=insertquery.replace("'None'",'NULL')
                insertquery = insertquery.replace("'nan'", 'NULL')
                insertquery = insertquery.replace("'NaT'", 'NULL')
                Querymsg,code=query_execution(connection2,insertquery)
                if code==0:
                    break;
    return Querymsg,code

def query_execution(connection,query):
    msg=''
    code=1
    try:
        #print(query)
        print('Executing query')
        connection.execute(query)
        msg = 'Query executed successfully'
        code = 1
    except Exception as e:
        print(e)
        print(query[:1000])
        msg = str(e)
        code = 0
    return msg,code





@celery.task()
def migrate(df, selectquery, cols, typeofops, tablename, tocolumns, setQuery, whereCondition,toserver,wherelistnull,connection2, engine2):
    df['concat'] = "'" + df['concat'] + "' UNION ALL "
    df['rownumber'] = (np.arange(len(df))) + 1
    trows = len(df)
    lendf = True
    offset = 0
    limit = 999
    i = 1
    Querymsg='Nothing Happened'
    code=404
    while lendf:
        # Ref https://stackoverflow.com/questions/53934470/equivalent-of-limit-and-offset-of-sql-in-pandas
        dfs = df.sort_values(by='rownumber', ascending=True).reset_index(drop=True).loc[offset: offset + limit - 1]
        offset += 999
        dfs['groupbyValue'] = 1
        dfselect = dfs.groupby('groupbyValue', as_index=False).apply(lambda x: 'SELECT  '.join(x.concat))
        dfselect = dfselect.reset_index()
        if len(dfs) is None:
            lendf = False
        elif len(dfs) == 0:
            lendf = False
        else:
            insertQ = (dfselect.iat[0, 1])
            N = 10
            finalselect = selectquery + ' UNION ALL SELECT  ' + str(insertQ)
            finalselect = finalselect[:-N]
            addparenttable = 'SELECT af.* FROM ( ' + finalselect + ' )af '
            insertquery = 'INSERT INTO ' + tablename + '( ' + tocolumns + ') ' + addparenttable +'WHERE af.'+ cols[0] + "!='noobdossier'"
            insertquery=insertquery.replace("'None'",'NULL')
            updatequery = updatequery.replace("'nan'", 'NULL')
            insertquery = insertquery.replace("'NaT'", 'NULL')
            Querymsg,code=query_execution(connection2,insertquery)
            if code==0:
                break;
    return Querymsg,code



