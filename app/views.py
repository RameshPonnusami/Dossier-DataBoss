from flask import render_template,flash,redirect
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder import ModelView, ModelRestApi,BaseView,expose,has_access

from flask_appbuilder.actions import action

from . import appbuilder, db, celery,app
from .models import Job,Process,Bucket,GlobalVariablesForJobs,Report,GlobalVariables,JobAudit,ProcessAuditBkup
from flask_appbuilder import MultipleView,MasterDetailView


'''
import job execution files
'''
from .jobexecution import mastermethod,jobmastermethod
from . import scheduler
import json
import pandas as pd

from celery.task.control import inspect,revoke
i = inspect()
class ProcessModelView(ModelView):

    datamodel=SQLAInterface(Process)
    list_title = "List Process "

    label_columns={'job':'Job','name':'Process Name','fromserver':'From Server','fromquery':'From Query','whereforUpdateifexists':'Where Clause'}
    list_columns=['job','name','fromserver','fromquery']

    @action("Excecute", "Execute", "Do you really want to Execute?", "fa-rocket")

    def myaction(self, items):
        """
            do something with the item record
        """
        if isinstance(items, list):
            list1=[]
            for item in items:
                list1.append(item.id)
            mastermethod.delay(list1)
            Querymsg ="Started"
            flash(Querymsg)

            self.update_redirect()

        else:
            print("list")
            print(items)
        return redirect(self.get_redirect())

    show_fieldsets = [
        ('Summary',
         {'fields': ['name','job','type']}
         ),
        ('From Query',
         {'fields': ['fromserver', 'fromquery', 'orderby'], 'expanded': True}
         ),
        ('To Query',
         {'fields': ['toserver', 'toserverquery', 'tocolumns'], 'expanded': True}
         ),
        ('Option',
         {'fields':['Inserttype'],'expanded': True}
        ),
        ('Where Clasue',
         {'fields': ['whereforUpdateifexists'],
          'expanded': True}
         ),
    ]
    add_fieldsets = [
        ('Summary',
         {'fields': ['name','job','type']}
         ),
        ('From Query',
         {'fields': ['fromserver', 'fromquery', 'orderby'], 'expanded': True}
         ),
        ('To Query',
         {'fields': ['toserver', 'toserverquery', 'tocolumns'], 'expanded': True}
         ),
        ('Option',
         {'fields': ['Inserttype'], 'expanded': True}
         ),
        ('Where Clasue',
         {'fields': ['whereforUpdateifexists'],
          'expanded': True}
         ),
        ('File',
         {'fields':['file']})

    ]
    edit_fieldsets = [
        ('Summary',
         {'fields': ['name','job','type']}
         ),
        ('From Query',
         {'fields': ['fromserver', 'fromquery', 'orderby'], 'expanded': True}
         ),
        ('To Query',
         {'fields': ['toserver', 'toserverquery', 'tocolumns'], 'expanded': True}
         ),
        ('Option',
         {'fields': ['Inserttype'], 'expanded': True}
         ),
        ('Where Clasue',
         {'fields': ['whereforUpdateifexists'],
          'expanded': True}
         ),
        ('File',
         {'fields': ['file']})
    ]



class JobView(ModelView):
    datamodel=SQLAInterface(Job)
    list_columns = ['name']
    @action("Excecute", "Execute", "Do you really want to Execute?", "fa-rocket")

    def myaction(self, items):
        """
            do something with the item record
        """
        if isinstance(items, list):
            list1 = []
            for item in items:
                list1.append(item.id)
                print(list1)
            jobmastermethod.delay(list1)
            Querymsg ="Started"
            flash(Querymsg)

            self.update_redirect()

        else:
            print("list")
            print(items)
        return redirect(self.get_redirect())

    edit_fieldsets = [
        ('Summary',
         {'fields': ['name']}
         ),

    ]


class BucketView(ModelView):
    datamodel=SQLAInterface(Bucket)

class GlobalVariablesForJobsView(ModelView):
    datamodel=SQLAInterface(GlobalVariablesForJobs)
    list_columns = ['name','query','job','server']
    edit_fieldsets = [('Summary',{'fields':['name','query','job','server']})]
    add_fieldsets = [('Summary',{'fields':['name','query','job','server']})]

class GlobalVariablesView(ModelView):
    datamodel=SQLAInterface(GlobalVariables)
    list_columns = ['name','query','server']
    edit_fieldsets = [('Summary',{'fields':['name','query','server']})]
    add_fieldsets = [('Summary',{'fields':['name','query','server']})]

class JobMasterView(MasterDetailView):
    datamodel=SQLAInterface(Job)
    related_views=[ProcessModelView,GlobalVariablesForJobsView]



class JobAuditView(ModelView):
    datamodel=SQLAInterface(JobAudit)
    list_columns = ['job_id', 'job', 'starttime', 'endtime','status','console','totaltime']




appbuilder.add_view(JobView,'List Jobs',icon='fa fa-university',category='Job Details',category_icon='fa fa-university')
appbuilder.add_view(BucketView,'List Bucket',icon='fa fa-bitbucket',category='Job Details')
appbuilder.add_view(GlobalVariablesForJobsView,'List Job Variables',icon='fa fa-file-text-o',category='Job Details')
appbuilder.add_view(ProcessModelView,'List Process',icon='fa fa-hourglass',category='Job Details')
appbuilder.add_view(JobMasterView,'Master Jobs',icon='fa fa-sitemap',category='Master Details',category_icon='fa fa-sitemap')
appbuilder.add_view(GlobalVariablesView,'List Global Variables',icon='fa fa-file',category='Job Details')


class ReportView(ModelView):
    datamodel = SQLAInterface(Report)

    @action("Activate", "Activate", "Do you really want to Activate?", "fa-rocket")
    def myaction(self, items):
        """
            do something with the item record
        """
        print("Report Activate")
        if isinstance(items, list):
            activate_reports(items)
            Querymsg = "Started"
            flash(Querymsg)

            self.update_redirect()

        else:
            print("list")
            print(items)
        return redirect(self.get_redirect())



class Taskops(object):
    print("Task ops is started")
    def addschedulebyid(self,k):
        self.k=k
        list1=[]
        list1.append(k)
        jobmastermethod.delay(list1)
        print("receivd from sceduler")

    def listscheduledjob(self):
        listscheduledjob = app.apscheduler.get_jobs()
        for job in listscheduledjob:
            print(job.name)
            print(job.id)
        return str(listscheduledjob)

    def get_elements_from_nested_array_dict(self,dictfile):

        column = ['jobid', 'jobname', 'taskid']
        rows = []

        #jobdetails = db.session.query(Report).all()
        for dictele in dictfile:
            for dictloop in dictfile[dictele]:
                jobid = str(dictloop['args'])
                taskid = dictloop['id']
                jobid = jobid.replace("(", "")
                jobid = jobid.replace(",)", '')
                jobname = db.session.query(Job).filter(Job.id==int(jobid)).first()
                row = [jobid, jobname.name, taskid]
                rows.append(row)

        df = pd.DataFrame(rows, columns=column)
        return df

    def listofrunningcelerytask(self):
        active = i.active()
        celerylistdf=self.get_elements_from_nested_array_dict(active)
        return  celerylistdf



def activate_reports(items):
    print("activate reports")
    for item in items:
        Reportupdate = db.session.query(Report).filter(Report.id == item.id).first()
        Reportupdate.status = 'active'
    db.session.commit()
    scheduled_job()



class ScheduleOps(BaseView):
    default_view = 'activate'
    @expose('/activate')
    @has_access
    def activate(self):
        scheduled_job()
        return "Record Added"
    @expose('/listcelerytasks')
    @has_access
    def listcelerytasks(self):
        to=Taskops()
        celerylistdf=to.listofrunningcelerytask()
        return  self.render_template('CeleryTasksList.html',data=celerylistdf)






appbuilder.add_view(ReportView,'Schedule',icon='fa fa-calendar-plus-o',category='Schedule', category_icon='fa fa-calendar')
appbuilder.add_view(ScheduleOps, "Start Schedule Server", icon='fa fa-bolt',category='Schedule')
appbuilder.add_link("List Celery Tasks",icon='fa fa-hourglass-half' ,href='/scheduleops/listcelerytasks',category='Schedule')

appbuilder.add_view(JobAuditView,'List of Job Audit',icon='fa fa-history',category='Audit Details',category_icon='fa fa-history')



@app.route('/killcelerytaskbyid/<string:taskid>' ,methods=['GET'])
def killcelerytaskbyid(taskid):
    print(taskid)
    celery.control.revoke(taskid,terminate=True)
    flash("task stoped")
    return redirect('/listcelerytasks')

@app.route('/test', methods=['GET'])
def test():
    print("check")
    return "check"
import time
@celery.task()
def celery_check(d):
    print("input started, sleeping now")
    time.sleep(10)
    print("slept")


def celery_checkmaster(d):
    print("input started, sleeping now")
    celery_check.delay(d)
    print("slept")



def scheduled_job():
    print("schedule")
    tops = Taskops()
    query = """
            select id,report_name,jobid,( CASE WHEN hour='12 AM' THEN 0
         WHEN hour='1 AM' THEN 1     WHEN hour='2 AM' THEN 2     WHEN hour='3 AM' THEN 3     WHEN hour='4 AM' THEN 4
         WHEN hour='6 AM' THEN 6     WHEN hour='7 AM' THEN 7   	 WHEN hour='8 AM' THEN 8     WHEN hour='9 AM' THEN 9
         WHEN hour='10 AM' THEN 10     WHEN hour='11 AM' THEN 11     WHEN hour='12 PM' THEN 12     WHEN hour='1 PM' THEN 13
         WHEN hour='2 PM' THEN 14     WHEN hour='3 PM' THEN 15     WHEN hour='4 PM' THEN 16     WHEN hour='5 PM' THEN 17
         WHEN hour='6 PM' THEN 18     WHEN hour='7 PM' THEN 19     WHEN hour='8 PM' THEN 20     WHEN hour='9 PM' THEN 21
         WHEN hour='10 PM' THEN 22     WHEN hour='11 PM' THEN 23 ELSE 0 END) hour,

        ( CASE WHEN day='Every Day' THEN 8     WHEN day='Every Sunday' THEN 1     WHEN day='Every Monday' THEN 2
         WHEN day='Every Tuesday' THEN 3     WHEN day='Every Wednesday' THEN 4     WHEN day='Every Thursday' THEN 5
         WHEN day='Every Friday' THEN 6     WHEN day='Every Saturday' THEN 7 ELSE 1 END) day,LOWER(LEFT(REPLACE(day,'Every ',''),3)) daykey,minute,status from report where status= 'active';"""
    reports = db.engine.execute(query)
    listscheduledjob = app.apscheduler.get_jobs()
    for r in reports:
        print("get into loop")
        if r.report_name not in listscheduledjob:
            print("condition ok",r.report_name)
            print('job id is :',r.jobid)
            strid=str(r.id)
            if r.day==8:
                app.apscheduler.add_job(func=tops.addschedulebyid, trigger='cron', name=r.report_name,hour=str(r.hour),
                                        minute=str(r.minute),args=[r.jobid], id=strid)
            elif r.day>=1 and r.day<=7:
                app.apscheduler.add_job(func=tops.addschedulebyid, trigger='cron', name=r.report_name,day_of_week=str(r.daykey),
                                        hour=str(r.hour), minute=str(r.minute), args=[r.jobid], id=strid)


            tsrid='s8987'
            app.apscheduler.add_job(func=celery_checkmaster, trigger='cron', name=r.report_name, second='*', args=[r.jobid],id=tsrid)


@appbuilder.app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "404.html", base_template=appbuilder.base_template, appbuilder=appbuilder
        ),
        404,
    )


db.create_all()

