import os
import sys
from flask import Flask
from flask import request
from flask_restplus import Api, Resource, fields, marshal, reqparse
import argparse
import logging
from git import Repo
import yaml
from sdap_ingest_manager.collections_ingester import LocalConfiguration, full_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

flask_app = Flask(__name__)
app = Api(app=flask_app)

name_space = app.namespace('ingestion-orders', description='SDAP Nexus ingestion order configuration')


time_range = app.model('time_range', {
        'from': fields.DateTime(dt_format='iso8601'),
        'to': fields.DateTime(dt_format='iso8601')
})

event = app.model('event', {
    'schedule': fields.String
})

order_template = {
    'id': fields.String,
    'dataset_id': fields.String,
    'path': fields.String,
    'variable': fields.String,
    'update_time' : fields.Nested(time_range),
    'observation_time': fields.Nested(time_range),
    'forward-processing': fields.String,
    'priority': fields.String,
    'on': fields.Nested(event),
    'active': fields.String
}

order = app.model('order', order_template)


@name_space.route("/")
class OrdersClass(Resource):
    @app.marshal_with(order, as_list=True)
    @app.doc(description="list the ingestion orders",
             responses={200: 'OK', 500: 'Internal error'},
             params={'format': 'either json or yaml'}
             )
    def get(self):
        return {
            "status": "To Be Implemented"
        }

    @app.marshal_with(order)
    @app.doc(description="submit an ingestion order which will be saved in the configuration and processed",
             responses={200: 'Success', 201: "Success", 400: 'Invalid Argument', 500: 'Internal error'},
             params={})
    def post(self):
        return {
            "status": "To be implemented"
        }


#params = {'git_url': 'url of the reference git repository',
#          'git_branch': 'branch of the reference git repository'}

class UnexpectedParameterException(Exception):
    pass


@name_space.route("/synchronize")
class MainClass(Resource):
    @app.doc(description="Pull configuration from reference repository",
             responses={200: 'OK', 400: 'Bad request', 500: 'Internal error'},
             params={'git_url': 'url of the reference git repository',
                     'git_branch': 'branch of the reference git repository'}
             )
    def get(self):
        try:
            config = local_config.get()
            if len(request.args) > 0:
                expected_parameters = {'git_url', 'git_branch'}
                for k in request.args.keys():
                    if k not in expected_parameters:
                        raise UnexpectedParameterException(f'Expected parameters are {expected_parameters}')

                if not config.has_section('GIT_CONFIG'):
                    config.add_section('GIT_CONFIG')

                for key, value in request.args.items():
                    config.set('GIT_CONFIG', key, value)

                repo = init_local_config_repo(config)

            repo.git.push()


            local_config.save()

            return {
                "status": "To Be Implemented"
            }
        except UnexpectedParameterException as e:
            logger.error(e)
            return {'message': f'{e}'}, 400



@name_space.route('/<order_id>', doc={'params':{'order_id': 'The id of the order'}})
class OrderClass(Resource):

    @app.marshal_with(order, envelope='resource')
    @app.doc(description="get an order",
             responses={200: 'OK', 404: 'Not existing', 500: 'Internal error'})
    def get(self):
        return {
            "status": "To be implemented"
        }

    @app.marshal_with(order, envelope='resource')
    @app.doc(description="update an ingestion order. ",
             responses={200: 'OK', 400: 'Invalid Argument', 404: 'Not existing', 500: 'Internal error'},
             params={}
             )
    def put(self):
        return {
            "status": "To be implemented"
        }

    @app.doc(description="delete an ingestion order. ",
             responses={200: 'OK', 400: 'Invalid Argument', 404: 'Not existing', 500: 'Internal error'},
             params={}
             )
    def delete(self):
        return {
            "status": "To be implemented"
        }

def init_local_config_repo(config):
    git_url = config.get('GIT_CONFIG', 'git_url')
    git_url = git_url if git_url.endswith(".git") else git_url + '.git'
    git_branch = config.get('GIT_CONFIG', 'git_branch')
    git_token = config.get('GIT_CONFIG', 'git_patoken')
    git_local = full_path(config.get('GIT_CONFIG', 'git_local'))
    repo = Repo.init(git_local)
    if len(repo.remotes)==0 or 'origin' not in [r.name for r in repo.remotes]:
        repo.create_remote('origin', git_url)
    repo.git.fetch()
    repo.git.checkout(git_branch)
    return repo


def read_ingestion_orders_from_config(file_name):

    try:
        with open(file_name, 'r') as f:
            orders_yml = yaml.load(f, Loader=yaml.FullLoader)

        ingestion_orders = {}
        for (c_id, ingestion_order) in orders_yml.items():
            ingestion_order['id'] = c_id
            ingestion_orders[c_id] = marshal(ingestion_order, order_template)

        logger.info(f"collections are {ingestion_orders}")
        return ingestion_orders

    except FileNotFoundError:
        logger.error(f"no collection configuration found at {file_name}")


def main():
    global local_config
    global orders
    global repo


    parser = argparse.ArgumentParser(description="Run ingestion for a list of collection ingestion streams")

    parser.add_argument("-c", "--config", help="configuration directory which contains the sdap_ingest_manager.ini file"
                                               "and other configuration files (list of ingestion streams)",
                        default=os.path.join(sys.prefix, ".sdap_ingest_manager"))
    options = parser.parse_args()
    local_config = LocalConfiguration(config_path=options.config)

    if local_config.get().has_section('GIT_CONFIG'):
        repo = init_local_config_repo(local_config.get())
        collections_yml = os.path.join(repo.working_dir, 'ingestion_orders.yml')
        orders = read_ingestion_orders_from_config(collections_yml)

    else:
        logger.info("configuration is empty use default resources")
        logger.info("if you want to elaborate and save your own configuration"
                    "set a git repository as /sdap-ingestion?git_url=<git url>&git_branch=<git_branch>&token=<personal access token>")
        orders = read_ingestion_orders_from_config(full_path('collections.yml.example'))

    flask_app.run()


if __name__ == "__main__":
    main()