
from . import insertops, celery,dossCommon
from datetime import datetime
from .audit import JobAuditOps,ProcessAuditOps,ReadGlobalVariables

JA=JobAuditOps()
PA=ProcessAuditOps()
gv = ReadGlobalVariables()
@celery.task()
def mastermethod(processlist,fromprocess=1):
    '''
    It executes the individual list of processlist. its not comes under process.
    :param processlist:
    :param fromprocess:
    :return:
    '''
    globalvariabledict = gv.global_variables_for_all_with_query()
    processstarttime = datetime.now()
    if fromprocess==1:
        items=dossCommon.processlist(processlist)
    elif fromprocess==0:
        items=processlist
    for item in items:
        processlastaddedid=PA.add_process_audit(item,processstarttime)
        if item.type=='Insert':
            Querymsg,code=insertops.insertmastermethod(item,globalvariabledict)
            if code==0:
                break;
        if item.type=='Single':
            Querymsg, code = insertops.singlemastermethod(item,globalvariabledict)
        if item.type=='Loop':
            Querymsg, code = insertops.loopmastermethod(item,globalvariabledict)
        if item.type == 'Migrate':
            Querymsg, code = insertops.migratemastermethod(item,globalvariabledict)
        if item.type == 'CSVtoDB':
            Querymsg, code = insertops.csvtodbmastermethod(item)
            if code == 0:
                break;

        endtime = datetime.now()
        PA.updateprocessaudit(processlastaddedid, code, Querymsg, processstarttime,endtime)


@celery.task()
def jobmastermethod(items):
    ''' master function for job execution.
    GET the list of jobs and exectute it  and pass the all process of job to the  jobprocessmethod method'''
    for item in items:

        globalvariabledict=gv.global_variables_for_all_with_query()

        #Job Audit is adding
        jobstarttime = datetime.now()
        jobauditlastaddid=JA.add_job_audit(item,jobstarttime)

        processdetail=dossCommon.processlistperjob(item)
        Querymsg,code=jobprocessmethod(processdetail,globalvariabledict,fromprocess=0)

        #Update jobaudit details:
        endtime=datetime.now()
        JA.update_job_audit(jobauditlastaddid,jobstarttime,code,Querymsg,endtime)
        # Updated jobaudit details


def jobprocessmethod(processlist,globalvariabledict,fromprocess=1):
    ''' Child funtion of jobmastermethod.
    It get the process list from jobmastermethod function and execute it.'''
    processstarttime = datetime.now()
    if fromprocess==1:
        items=dossCommon.processlist(processlist)
    elif fromprocess==0:
        items=processlist
    for item in items:
        processlastaddedid = PA.add_process_audit(item, processstarttime)
        if item.type=='Insert':
            Querymsg,code=insertops.insertmastermethod(item,globalvariabledict)
            if code==0:
                break;
        if item.type=='Single':
            Querymsg, code = insertops.singlemastermethod(item,globalvariabledict)
            if code==0:
                break;
        if item.type=='Loop':
            Querymsg, code = insertops.loopmastermethod(item,globalvariabledict)
            if code==0:
                break;
        if item.type=='Migrate':
            Querymsg, code = insertops.migratemastermethod(item,globalvariabledict)
            if code==0:
                break;
        if item.type == 'CSVtoDB':
            Querymsg, code = insertops.csvtodbmastermethod(item)
            if code == 0:
                break;

        endtime=datetime.now()
        PA.updateprocessaudit(processlastaddedid, code, Querymsg, processstarttime,endtime)
    return Querymsg,code

