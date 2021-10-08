from wtforms import Form,StringField
from wtforms.validators import DataRequired
from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
from flask_appbuilder.forms import DynamicForm
    from wtforms import Form, StringField, SelectField, validators

from wtforms_alchemy import ModelForm
from .models import Job,Process,Bucket,Type
import wtforms
import wtforms.validators as validators



class ProcessForm(Form):
    Type = [('Insert', 'Insert'),
                   ('Single', 'Single'),
                   ('CSVtoTable', 'CSVtoTable')
                   ]
    name = wtforms.StringField('name')
    Job = wtforms.SelectField(u'Job', choices=())
    FromServer =wtforms.SelectField(u' From Server',choices=())
    FromQuery=wtforms.StringField('FromQuery')
    OrderBy = wtforms.StringField('OrderBy')
    ToServer=wtforms.SelectField(u'To Server',choices=())
    ToTable = wtforms.StringField('ToTable')
    ToTableColumns = wtforms.StringField('ToTableColumns')
    Ops_type = wtforms.SelectField('Type', choices=Type)


