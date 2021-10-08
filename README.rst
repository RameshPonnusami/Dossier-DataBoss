Base Skeleton to start your application using Flask-AppBuilder
--------------------------------------------------------------

- Install it::

	pip install flask-appbuilder
	git clone https://github.com/dpgaspar/Flask-AppBuilder-Skeleton.git

- Run it::

    $ export FLASK_APP=app
    # Create an admin user
    $ flask fab create-admin
    # Run dev server
    $ flask run
    #Run celery server
    celery -A app.celery worker -l info
    #run flask server
    python run.py




Note:"Here Im using apscheduler beacuse it allow us to do
action on each tasks, and we need to start the scheduler once at initial time."
-In Views.py scheduler is defined
That's it!!

Redis server has beed used here for celery queue's

-MysqlDB error:(ubuntu)::

         https://github.com/openai/gym/issues/757
         apt-get install python3.6-dev libmysqlclient-dev
         apt-get install libssl-dev
         pip install mysqlclient
         pip install pyYAML


-Ops type called loops:
    it act like for loop


![Alt text](https://bitbucket.org/RameshPonnusamy/dossier/raw/dossier_1/app/static/uploads/loop_process.JPG)


