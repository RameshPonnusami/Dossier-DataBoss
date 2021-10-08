import numpy as np
from .dossCommon import query_execution

def convert_df_into_sql(df, dfcolumns, insertquery,lendfC,connection):
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
        insertquerytemp = insertquerytemp.replace("'none'", 'NULL')
        insertquerytemp = insertquerytemp.replace("'nan'", 'NULL')
        insertquerytemp = insertquerytemp.replace("'nat'", 'NULL')
        Querymsg,code=query_execution(connection,insertquerytemp)
        insertquerytemp = ''
        QueryValues = ''
        if code == 0:
            lendf=False
            break;
    return Querymsg,code