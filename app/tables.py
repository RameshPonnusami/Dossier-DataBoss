from flask_table import Table, Col, LinkCol

class Results(Table):
    id = Col('Id', show=False)
    name = Col('name')
    job = Col('job')
    fromserver = Col('fromserver')
    fromquery = Col('fromquery')
    orderby = Col('orderby')
    toserver = Col('toserver')
    toserverquery = Col('toserverquery')
    tocolumns = Col('tocolumns')
    edit = LinkCol('Edit', 'edit', url_kwargs=dict(id='id'))