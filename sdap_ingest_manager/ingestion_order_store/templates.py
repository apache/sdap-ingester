from flask_restplus import fields

class Templates:
    def __init__(self, app):
        self._app = app
        self.time_range = app.model('time_range', {
            'from': fields.DateTime(dt_format='iso8601'),
            'to': fields.DateTime(dt_format='iso8601')
        })

        self.event = app.model('event', {
            'schedule': fields.String
        })

        self.order_template = {
            'id': fields.String,
            'dataset_id': fields.String,
            'path': fields.String,
            'variable': fields.String,
            'update_time': fields.Nested(self.time_range),
            'observation_time': fields.Nested(self.time_range),
            'forward-processing': fields.String,
            'priority': fields.String,
            'on': fields.Nested(self.event),
            'active': fields.String
        }

        self.order = app.model('order', self.order_template)

