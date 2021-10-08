import logging
from celery import Celery
from flask import Flask
from flask_appbuilder import AppBuilder, SQLA
from app.index import MyIndexView
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from config import config
import os
from flask_apscheduler import APScheduler

''' To start the celery server: celery -A app.celery worker -l info  '''
''' Global variables start with $ example $MaxId 
     and it should be a single row ex: SELECT MAX(id) id from table  OR SELECT id from table where id=10 limit 1 '''

logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logging.getLogger().setLevel(logging.DEBUG)

celery=Celery()

def create_app(config_name=None):
    if config_name is None:
        config_name = os.environ.get('CONFIG', 'development')
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    celery.config_from_object(app.config)
    return app

def create_celery(app):
    celery = Celery(app.import_name,backend=app.config['CELERY_RESULT_BACKEND'],broker=app.config['BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


app=create_app()
db = SQLA(app)
celery = create_celery(app)

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

logger = get_task_logger(__name__)


appbuilder = AppBuilder(app, db.session,base_template='my_index.html')



from . import views
