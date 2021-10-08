"""
from app import app
from .forms import ProcessForm
from .tables import Results
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from config import db_session,init_db
from flask import flash, render_template, request, redirect

def populate_form_choices(form):
    Servers = db.session.query(Bucket).all()
    server_names = []
    for server in Servers:
        server_names.append((str(server.id),server.name))

    Jobs = db.session.query(Job).all()
    job_names = []
    for job in Jobs:
        job_names.append((str(job.id),job.name))

    form.FromServer.choices = server_names
    form.ToServer.choices = server_names
    form.Job.choices=job_names

class MyView(BaseView):
    default_view = 'new_album'
    @expose('/new_album/')
    @has_access
    def new_album(self):

        form = ProcessForm(request.form)
        populate_form_choices(form)

        results = []


        qry = db_session.query(Process)
        results = qry.all()
        table = Results(results)
        table.border = True

        if request.method == 'POST'  and form.validate():
            # save the album
            process = Process()
            print(process)
            save_changes(process, form, new=True)
            flash('Album created successfully!')
            return "msg"
        return render_template('new_album.html', base_template=appbuilder.base_template, appbuilder=appbuilder,  form=form,table=table)

    @expose('/method2/<string:param1>')
    @has_access
    def method2(self, param1):
        # do something with param1
        # and render template with param
        param1 = 'Goodbye %s' % (param1)
        return param1

appbuilder.add_view(MyView, "Method1", category='My View')
appbuilder.add_link("Method2", href='/myview/method2/john', category='My View')

@app.route('/new_album/', methods=['GET', 'POST'])
def new_album():

    form = ProcessForm(request.form)
    print(form)
    print(request.method)
    if request.method == 'POST':
        # save the album
        process = Process()
        save_changes(process, form, new=True)
        print("updates")
        flash('Album created successfully!')
        return redirect('/')

   # return render_template('new_album.html', base_template=appbuilder.base_template, appbuilder=appbuilder, form=form)
def save_changes(process, form, new=False,id=False):
    print("save edit")

    process.name = form.name.data
    process.job_id = form.Job.data
    process.fromserver_id = form.FromServer.data
    process.fromquery = form.FromQuery.data
    process.orderby = form.OrderBy.data
    process.toserver_id = form.ToServer.data
    process.toserverquery = form.ToTable.data
    process.tocolumns = form.ToTableColumns.data
    process.type=form.Ops_type.data
    if new:
        # Add the new album to the database
            db_session.add(process)
    else:

        # form.populate_obj(Process)
        db_session.commit()
    # commit the data to the database
    db_session.commit()


@app.route('/item/<int:id>', methods=['GET', 'POST'])
def edit(id):
    qry = db_session.query(Process).filter(Process.id==id)
    print(qry)
    process = qry.first()
    #Ref https://github.com/wtforms/wtforms/issues/106
    if process:
        form = ProcessForm(formdata=request.form, obj=process)
        populate_form_choices(form)
        form.FromServer.default = process.fromserver_id
        form.ToServer.default = process.toserver_id
        form.name.default = process.name
        form.Job.default = process.job_id
        form.FromQuery.default = process.fromquery
        form.OrderBy.default = process.orderby
        form.ToTableColumns.default = process.tocolumns
        form.ToTable.default = process.toserverquery
        form.Ops_type.default = process.type

        form.process()
        if request.method == 'POST' :
            FromServer = request.form.get("FromServer")
            ToServer = request.form.get("ToServer")
            name=request.form.get("name")
            Job = request.form.get("Job")
            FromQuery = request.form.get("FromQuery")
            OrderBy = request.form.get("OrderBy")
            ToTableColumns=request.form.get("ToTableColumns")
            ToTable = request.form.get("ToTable")
            OpsType = request.form.get("Ops_type")
            processd = db_session.query(Process).filter(Process.id==id)
            processd.fromserver_id=FromServer
            processd.fromquery=FromQuery
            processd.toserver_id=ToServer
            processd.toserverquery=ToTable
            processd.orderby=OrderBy
            processd.tocolumns=ToTableColumns
            print(ToTableColumns)
            processd.type=OpsType
            processd.job_id=Job
            processd.name=name
            print(process.id)

            db_session.commit()
            # save edits
           #save_changes(process, form,id)
            flash('Album updated successfully!')
            return redirect('/')
        return render_template('edit_album.html', base_template=appbuilder.base_template, appbuilder=appbuilder, form=form)
    else:
        return 'Error loading #{id}'.format(id=id)
"""