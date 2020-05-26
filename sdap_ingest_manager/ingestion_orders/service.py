import os
import sys
from flask import Flask
from flask_restplus import Api, Resource, fields
import argparse
import logging
from sdap_ingest_manager.ingestion_orders.ingestion_orders import IngestionOrders
from sdap_ingest_manager.ingestion_orders.templates import Templates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

flask_app = Flask(__name__)
app = Api(app=flask_app)

name_space = app.namespace('ingestion-orders', description='SDAP Nexus ingestion order configuration')

templates = Templates(app)


@name_space.route("/")
class OrdersClass(Resource):
    @app.marshal_with(templates.order, as_list=True)
    @app.doc(description="list the ingestion orders",
             responses={200: 'OK', 500: 'Internal error'},
             params={'format': 'either json or yaml'}
             )
    def get(self):
        return {
            "status": "To Be Implemented"
        }

    @app.marshal_with(templates.order)
    @app.doc(description="submit an ingestion order which will be saved in the configuration and processed",
             responses={200: 'Success', 201: "Success", 400: 'Invalid Argument', 500: 'Internal error'},
             params={})
    def post(self):
        return {
            "status": "To be implemented"
        }



@name_space.route("/pull")
class MainClass(Resource):
    @app.doc(description="Pull configuration from the reference repository",
             responses={200: 'OK', 400: 'Bad request', 500: 'Internal error'},
             params={}
             )
    def get(self):

        orders.pull()

        return {
            "message": "ingestion orders succesfully synchonized",
            "git_url" : orders.get_git_url(),
            "git_branch": orders.get_git_branch(),
            "orders": orders.dump()
        }

@name_space.route("/push")
class MainClass(Resource):
    @app.doc(description="Push configuration from the reference repository",
             responses={200: 'OK', 400: 'Bad request', 500: 'Internal error'},
             params={}
             )
    def get(self):

        orders.pull()

        return {
            "message": "ingestion orders succesfully synchonized",
            "git_url" : orders.get_git_url(),
            "git_branch": orders.get_git_branch(),
            "orders": orders.dump()
        }


@name_space.route('/<order_id>', doc={'params': {'order_id': 'The id of the order'}})
class OrderClass(Resource):

    @app.marshal_with(templates.order, envelope='resource')
    @app.doc(description="get an order",
             responses={200: 'OK', 404: 'Not existing', 500: 'Internal error'})
    def get(self):
        return {
            "status": "To be implemented"
        }

    @app.marshal_with(templates.order, envelope='resource')
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


def main():
    global orders

    parser = argparse.ArgumentParser(description="Run ingestion for a list of collection ingestion streams")

    default_configuration = os.path.join(sys.prefix, '.sdap_ingest_manager')
    parser.add_argument("-gu", "--git-url", help="git repository from which the ingestion order list is pulled/saved",
                        default="https://github.com/tloubrieu-jpl/sdap-ingester-config")
    parser.add_argument("-gb", "--git-branch", help="git branch from which the ingestion order list is pulled/saved",
                        default="master")
    parser.add_argument("-gt", "--git-token", help="git personnal access token used to access the repository",
                        default="master")
    options = parser.parse_args()

    logger.info("")

    orders = IngestionOrders(options.git_url,
                             git_branch=options.git_branch,
                             git_token=options.git_token,
                             order_template=templates.order_template)

    flask_app.run()


if __name__ == "__main__":
    main()
