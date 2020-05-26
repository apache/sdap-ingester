import argparse
import logging
import os
import sys
from typing import List

from flask import Flask
from flask_restplus import Api, Resource

from sdap_ingest_manager.ingestion_order_executor import IngestionOrderExecutor
from sdap_ingest_manager.history_manager import DatasetIngestionHistoryFile, DatasetIngestionHistorySolr
from sdap_ingest_manager.ingestion_order_store import GitIngestionOrderStore, FileIngestionOrderStore
from sdap_ingest_manager.ingestion_order_store.templates import Templates

logging.basicConfig(level=logging.INFO)
logging.getLogger("pika").setLevel(logging.DEBUG)
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


@name_space.route("/synchronize")
class MainClass(Resource):
    @app.doc(description="Pull configuration from the reference repository",
             responses={200: 'OK', 400: 'Bad request', 500: 'Internal error'},
             params={}
             )
    def get(self):
        global order_store

        order_store.load()

        return {
            "message": "ingestion orders succesfully synchonized",
            "git_url": order_store.get_git_url(),
            "git_branch": order_store.get_git_branch(),
            "orders": order_store.orders()
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
    global order_store

    parser = argparse.ArgumentParser(description="Run ingestion for a list of collection ingestion streams")
    parser.add_argument("-gu", "--git-url",
                        help="git repository from which the ingestion order list is pulled/saved")
    parser.add_argument("-gb", "--git-branch", help="git branch from which the ingestion order list is pulled/saved",
                        default="master")
    parser.add_argument("-gt", "--git-token", help="git personal access token used to access the repository")
    parser.add_argument("--local-ingestion-orders", help="path to local ingestion orders file", required=True)
    history_group = parser.add_mutually_exclusive_group(required=True)
    history_group.add_argument("--history-path", help="path to ingestion history local directory")
    history_group.add_argument("--history-url", help="url to ingestion history solr database")

    options = parser.parse_args()

    if options.local_ingestion_orders:
        order_store = FileIngestionOrderStore(path=options.local_ingestion_orders,
                                              order_template=templates.order_template)
    else:
        order_store = GitIngestionOrderStore(options.git_url,
                                             git_branch=options.git_branch,
                                             git_token=options.git_token,
                                             order_template=templates.order_template)

    message_schema = os.path.join(os.path.dirname(__file__),
                                  '../ingestion_order_executor/resources/dataset_config_template.yml')
    ingestion_launcher = IngestionOrderExecutor()
    for ingestion_order in list(order_store.orders().values()):
        if options.history_path:
            history_manager = DatasetIngestionHistoryFile(options.history_path, ingestion_order['id'])
        else:
            history_manager = DatasetIngestionHistorySolr(options.history_url, ingestion_order['id'])
        ingestion_launcher.execute_ingestion_order(collection=ingestion_order,
                                                   collection_config_template=message_schema,
                                                   history_manager=history_manager)

    flask_app.run()


if __name__ == "__main__":
    main()
