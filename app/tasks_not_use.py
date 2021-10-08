import random
import time

from celery.signals import task_postrun
from celery.utils.log import get_task_logger

from app import celery
from app import db
from app.models import Report
from celery.schedules import crontab


logger = get_task_logger(__name__)


@celery.task(bind=True)
def long_task(self):
    """Background task that runs a long function with progress reports."""
    verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
    adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
    noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']
    message = ''
    total = random.randint(10, 50)
    for i in range(total):
        if not message or random.random() < 0.25:
            message = '{0} {1} {2}...'.format(random.choice(verb),
                                              random.choice(adjective),
                                              random.choice(noun))
        self.update_state(state='PROGRESS',
                          meta={'current': i, 'total': total,
                                'status': message})
        time.sleep(1)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 42}


@celery.task
def log(message):
    """Print some log messages"""
    logger.debug(message)
    logger.info(message)
    logger.warning(message)
    logger.error(message)
    logger.critical(message)


@celery.task
def reverse_messages():
    """Reverse all messages in DB"""
    print("reverse images")

@celery.task(name="find_new_reports")
def find_new_reports():
    print("items are")
    print("Find new reports")
    # reports = Report.query.all()
    # reports = db.session.query(Report).filter(Report.status == 'active').all()
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
     WHEN day='Every Friday' THEN 6     WHEN day='Every Saturday' THEN 7 ELSE 1 END) day,minute,status from report;"""
    reports = db.engine.execute(query)
    print("find new reports check")
    for r in reports:
        print(r.day)
        dd=r.id
        print(r.report_name)

        if r.report_name not in celery.conf.beat_schedule:
            print(r.report_name)
            celery.conf.beat_schedule[r.report_name] = {
                r.report_name:{
                "task": "celery_worker.task_check",
                "schedule": crontab(minute="*"),
                "args":(dd),
                },
            }
            # celery.conf.update(beat_schedule=celery.conf.beat_schedule, )
        print("completed")

@celery.task()
def task_check(id):
    print(id)
    print("This is check")

'''
@task_postrun.connect
def close_session(*args, **kwargs):
    # Flask SQLAlchemy will automatically create new sessions for you from
    # a scoped session factory, given that we are maintaining the same app
    # context, this ensures tasks have a fresh session (e.g. session errors
    # won't propagate across tasks)
    celery.db.session.remove()
'''