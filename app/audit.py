from .models import JobAudit,ProcessAuditBkup,GlobalVariablesForJobs,GlobalVariables
from . import dossCommon
from app import db
from .dossCommon import db_conn1

class JobAuditOps(object):
    def add_job_audit(self,item,jobstarttime):
        jobname = dossCommon.get_job_values(item)
        print(jobname)
        print('jobid is:',item)

        print('jobname is ',jobname.name)
        jobaudit = JobAudit(job_id=item, job=jobname.name, starttime=jobstarttime, status="Progress",
                            console="no error")
        db.session.add(jobaudit)
        db.session.commit()
        jobauditlastaddid = jobaudit.id
        return jobauditlastaddid
    def update_job_audit(self,jobauditlastaddid,jobstarttime,code,Querymsg,endtime):
        jobauditupdate = db.session.query(JobAudit).filter(JobAudit.id == jobauditlastaddid).first()
        jobauditupdate.endtime = endtime
        if code != 0:
            jobauditupdate.status = 'Success'
        elif code == 0:
            jobauditupdate.status = 'Failed'
            jobauditupdate.console = Querymsg
        jobauditupdate.totaltime = endtime-jobstarttime
        db.session.commit()

class ProcessAuditOps(object):
    def add_process_audit(self,item, processstarttime):
        '''Insert the process execution details when process is started'''
        # add audit deatils
        fromserverdetails = dossCommon.get_db_values(item.fromserver_id)
        toserverdetails = dossCommon.get_db_values(item.toserver_id)
        if toserverdetails is None:
            print(fromserverdetails)
            print(item)
            processaudit = ProcessAuditBkup(job_id=item.job_id, starttime=processstarttime, process_id=item.id,
                                        process=item.name,fromserver_id=item.fromserver_id,
                                        fromservername=fromserverdetails.name,fromserverconn=str(fromserverdetails.auditconn), fromquery=item.fromquery,
                                         status="Progress", console="no error")
        elif fromserverdetails is None:
            processaudit = ProcessAuditBkup(job_id=item.job_id, starttime=processstarttime, process_id=item.id,
                                        process=item.name, toserver_id=item.toserver_id,
                                        toserverconn=toserverdetails.auditconn, toservername=toserverdetails.name,
                                        toquery='Table: ' + str(item.toserverquery) + '; columns: ' + str(
                                            item.tocolumns), status="Progress", console="no error")
        else:
            processaudit = ProcessAuditBkup(job_id=item.job_id, starttime=processstarttime, process_id=item.id,
                                        process=item.name,
                                        fromserver_id=item.fromserver_id,
                                        fromservername=fromserverdetails.name,
                                        fromserverconn=fromserverdetails.auditconn, fromquery=item.fromquery,
                                        toserver_id=item.toserver_id,
                                        toserverconn=toserverdetails.auditconn, toservername=toserverdetails.name,
                                        toquery='Table: ' + str(item.toserverquery) + '; columns: ' + str(
                                            item.tocolumns), status="Progress", console="no error")
        db.session.add(processaudit)
        db.session.commit()
        processlastaddedid = processaudit.id
        return processlastaddedid

    def updateprocessaudit(self,processlastaddedid, code, Querymsg, processstarttime,endtime):
        ''' Update the process end time when process is completed'''
        # update endtime and status
        processauditupdate = db.session.query(ProcessAuditBkup).filter(ProcessAuditBkup.id == processlastaddedid).first()
        processauditupdate.endtime = endtime
        if code != 0:
            processauditupdate.status = "Success"
        else:
            processauditupdate.status = "Failed"
            processauditupdate.console = Querymsg
        processauditupdate.totaltime = endtime - processstarttime
        db.session.commit()


class ReadGlobalVariables(object):
    def global_variables_for_jobs_with_query(self,item,globalvariabledict):
        ''' Replace the global variable with their value for particular job'''
        globalvaribles = db.session.query(GlobalVariablesForJobs).filter(GlobalVariablesForJobs.job_id == item.job_id).all()
        query = item.fromquery
        for gvvariable, gvvalue in globalvariabledict.items():
            query = query.replace(gvvariable, str(gvvalue))
        for gv in globalvaribles:
            print(gv.name, gv.query, gv.server_id)
            connection1, engine1 = db_conn1(gv.server_id)
            results = connection1.execute(gv.query)
            replacevalue = results.first()[0]
            replacevariable = str(gv.name)
            # query = query.replace(replacevariable, "'" + str(replacevalue) + "'")
            query = query.replace(replacevariable,  str(replacevalue))
            connection1.close()
        return query



    def global_variables_for_all_with_query(self):
        ''' Replace the global variable with their value for all job and its unique for each job'''
        globalvaribles = db.session.query(GlobalVariables).all()
        globalvariabledict={}
        for gv in globalvaribles:
            print(gv.name, gv.query, gv.server_id)
            connection1, engine1 = db_conn1(gv.server_id)
            results = connection1.execute(gv.query)
            replacevalue = results.first()[0]
            replacevariable = str(gv.name)
            globalvariabledict[replacevariable]=replacevalue
            connection1.close()
        return globalvariabledict
