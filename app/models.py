from flask_appbuilder import Model
from sqlalchemy import Column, Integer, String, ForeignKey, Enum, DateTime, Text, case
import enum
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
from flask_appbuilder.models.mixins import AuditMixin, FileColumn
from flask import Markup, url_for
from flask_appbuilder.filemanager import get_file_original_name
from flask_appbuilder.fields import SelectField
from . import appbuilder, db
# class BucketBkup(Model):
#     id=Column(Integer,primary_key=True)
#     name=Column(String(100),unique=True,nullable=False)
#     conn=Column(String(100),nullable=False)
#     def __repr__(self):
#         return self.name
class Bucket(Model):

    id = Column(Integer, primary_key=True)
    name = Column(String(64), unique=True)
    username = Column(String(64))
    # TODO encrypt password
    password = Column(String(128))
    hostname =Column(String(64))
    database = Column(String(64))
    port = Column(String(10))
    dialect = Column(Enum("mysql", "mssql+pymssql","postgresql"))

    @hybrid_property
    def conn(self)  -> str:
        conn=self.dialect+'://'+self.username+':'+self.password+'@'+self.hostname+':'+self.port+'/'+self.database
        return str(conn)

    @hybrid_property
    def auditconn(self)  -> str:
        auditconn = self.dialect + '://' + self.username + ':' + '****'+ '@' + self.hostname + ':' + self.port + '/' + self.database
        return str(auditconn)
    def __repr__(self):
        return self.name



class Job(Model):
    id=Column(Integer,primary_key=True)
    name=Column(String(50),unique=True,nullable=False)
    def __repr__(self):
        return self.name

class Process(Model):
    id=Column(Integer,primary_key=True)
    name=Column(String(50),nullable=False)
    job_id=Column(Integer,ForeignKey('job.id'))
    job=relationship("Job")
    #Ref https://stackoverflow.com/questions/45526761/making-flask-appbuilder-play-nicely-with-views-of-tables-with-multiple-foreign-k
    #Refer single foreign keys many times in single model
    fromserver_id=Column(Integer,ForeignKey('bucket.id'))
    fromserver=relationship("Bucket",foreign_keys=[fromserver_id])
    fromquery=Column(String(6000),nullable=True)
    orderby=Column(String(1000))
    toserver_id=Column(Integer,ForeignKey('bucket.id'))
    toserver=relationship("Bucket",foreign_keys=[toserver_id])
    toserverquery=Column(String(6000))
    tocolumns=Column(String(6000))
    # typeofoperation_id=Column(Integer,ForeignKey('type.id'))
    # typeofoperation=relationship("Type",foreign_keys=[typeofoperation_id])
    #type = Column(String(100))
    type = Column(Enum("Insert", "Single","Loop","CSVtoDB"))
    Inserttype = Column(Enum( "InsertDirectly","UpdateIfExists"))
    whereforUpdateifexists = Column(String(600))
    file = Column(FileColumn)

    def file_name(self):
        return get_file_original_name(str(self.file))

    # def view_based_on_type(self):

    def __repr__(self):
        return self.name




class GlobalVariablesForJobs(Model):
    id=Column(Integer,primary_key=True)
    name=Column(String(300))
    query=Column(String(5000))
    job_id = Column(Integer, ForeignKey('job.id'))
    job = relationship("Job")
    server_id = Column(Integer, ForeignKey('bucket.id'))
    server = relationship("Bucket", foreign_keys=[server_id])

    def __repr__(self):
        return self.name

class GlobalVariables(Model):
    id=Column(Integer,primary_key=True)
    name=Column(String(300))
    query=Column(String(5000))
    server_id = Column(Integer, ForeignKey('bucket.id'))
    server = relationship("Bucket", foreign_keys=[server_id])

    def __repr__(self):
        return self.name


class JobAudit(Model):
    id = Column(Integer,primary_key=True)
    job_id=Column(Integer)
    job=Column(String(500))
    starttime=Column(DateTime)
    endtime=Column(DateTime)
    status=Column(Enum("Success","Failed","Progress"))
    console=Column(Text(4294000000))
    totaltime=Column(String(500))

class ProcessAuditBkup(Model):
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    process_id=Column(Integer)
    process = Column(String(500))
    starttime = Column(DateTime)
    fromserver_id = Column(Integer)
    fromservername=Column(String(500))
    fromserverconn= Column(String(500))
    fromquery=Column(Text())
    toserver_id = Column(Integer)
    toservername=Column(String(500))
    toserverconn = Column(String(500))
    toquery=Column(Text())
    endtime = Column(DateTime)
    status = Column(Enum("Success", "Failed","Progress"))
    console = Column(Text())
    totaltime = Column(String(500))




class ProcessExecutionAuditBkup(Model):
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    process_id=Column(Integer)
    starttime = Column(DateTime)

    fromserver_id = Column(Integer)
    fromservername = Column(String(500))
    fromservername = Column(String(500))
    fromquery = Column(Text(4294000000))
    offset= Column(String(500))
    totalrows= Column(String(500))

    toserver_id = Column(Integer)
    toservername = Column(String(500))
    toservername = Column(String(500))
    toquery = Column(Text(4294000000))
    rowsaffected = Column(String(1000))

    endtime = Column(DateTime)
    status = Column(Enum("Success", "Failed"))
    console = Column(Text(4294000000))
    totaltime = Column(String(500))

class ProcessExecutionAudit(Model):
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    process_id=Column(Integer)
    starttime = Column(DateTime)

    fromserver_id = Column(Integer)
    fromservername = Column(String(500))
    fromservername = Column(String(500))
    fromquery = Column(Text(4294000000))
    offset= Column(String(500))
    totalrows= Column(String(500))

    toserver_id = Column(Integer)
    toservername = Column(String(500))
    toservername = Column(String(500))
    toquery = Column(Text(4294000000))
    rowsaffected = Column(String(1000))

    endtime = Column(DateTime)
    status = Column(Enum("Success", "Failed"))
    console = Column(Text(4294000000))
    totaltime = Column(String(500))
HOURS = [(0, '12 AM'), (1, '1 AM'), (2, '2 AM'), (3, '3 AM'),
         (4, '4 AM'),(5, '5 AM'), (6, '6 AM'), (7, '7 AM'), (8, '8 AM'),
         (9, '9 AM'), (10, '10 AM'), (11, '11 AM'), (12, '12 AM'),
         (13, '1 PM'), (14, '2 PM'), (15, '3 PM'), (16, '4 PM'),
         (17, '5 PM'), (18, '6 PM'), (19, '7 PM'), (20, '8 PM'),
         (21, '9 PM'), (22, '10 PM'), (23, '11 PM')]

MINUTES = [(0, 'On the hour'), (15, 'At XX:15'), (30, 'At XX:30'),
           (45, 'At XX:45')]

DAYS = [(0, 'Every Sunday'), (1, 'Every Monday'),
        (2, 'Every Tuesday'), (3, 'Every Wednesday'),
        (4, 'Every Thursday'), (5, 'Every Friday'),
        (6, 'Every Saturday')]


class Report(Model):
    __tablename__ = 'report'
    id = Column(Integer, primary_key=True)
    report_name =  Column( String(64), unique=True)
    jobid =  Column(Integer,ForeignKey('job.id'),nullable=False)
    job = relationship("Job", foreign_keys=[jobid])
    hour = Column( Enum('12 AM','1 AM','2 AM','3 AM','4 AM','5 AM','6 AM','7 AM','8 AM','9 AM','10 AM','11 AM','1 PM','2 PM','3 PM','4 PM','5 PM','6 PM','7 PM','8 PM','9 PM','10 PM','11 PM','12 PM'))
    minute =  Column(Enum('1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','43','44','45','46','47','48','49','50','51','52','53','54','55','56','57','58','59','60'))
    status= Column(String(15))
    day =  Column(Enum('Every Day','Every Sunday','Every Monday','Every Tuesday','Every Wednesday','Every Thursday','Every Friday','Every Saturday'))
    # query_text =  Column( Text())
    dayid = case([(day == 'Every Day','8'),(day == 'Every Sunday','1'), (day == 'Every Monday','2'),(day == 'Every Tuesday','3'),(day == 'Every Wednesday','4'), (day == 'Every Thursday','5') ,(day == 'Every Friday','6'), (day == 'Every Saturday','7')], else_=day)
    hourid = case([(hour == '12 AM', '0'), (hour == '1 AM', '1'), (hour == '2 AM', '2'), (hour == '3 AM', '3'),
                   (hour == '4 AM', '4'), (hour == '5 AM', '5'), (hour == '6 AM', '6'), (hour == '7 AM', '7'),
                   (hour == '8 AM', '8'), (hour == '9 AM', '9'), (hour == '10 AM', '10'), (hour == '11 AM', '11'),
                   (hour == '12 PM', '12'),
                   (hour == '1 PM', '13'), (hour == '2 PM', '14'), (hour == '3 PM', '15'), (hour == '4 PM', '16'),
                   (hour == '5 PM', '17'), (hour == '6 PM', '18'), (hour == '7 PM', '19'),
                   (hour == '8 PM', '20'), (hour == '9 PM', '21'), (hour == '10 PM', '22'), (hour == '11 PM', '23')],
                  else_=hour)




"""

You can use the extra Flask-AppBuilder fields and Mixin's

AuditMixin will add automatic timestamp of created and modified by who


"""
