import argparse
import logging
import os
from functools import partial

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from flask_restplus import Api, Resource

from sdap_ingest_manager.history_manager import IngestionHistoryBuilder, SolrIngestionHistoryBuilder, \
    FileIngestionHistoryBuilder
from sdap_ingest_manager.ingestion_order_executor import IngestionOrderExecutor
from sdap_ingest_manager.ingestion_order_store import GitIngestionOrderStore, FileIngestionOrderStore
from sdap_ingest_manager.ingestion_order_store import IngestionOrderStore
from sdap_ingest_manager.ingestion_order_store.templates import Templates
from sdap_ingest_manager.publisher import MessagePublisher

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


@name_space.route("/pull")
class MainClass(Resource):
    @app.doc(description="Pull configuration from the reference repository",
             responses={200: 'OK', 400: 'Bad request', 500: 'Internal error'},
             params={}
             )
    def get(self):
        order_store.pull()

        return {
            "message": "ingestion orders succesfully synchonized",
            "git_url": order_store.get_git_url(),
            "git_branch": order_store.get_git_branch(),
            "orders": order_store.dump()
        }


@name_space.route("/push")
class MainClass(Resource):
    @app.doc(description="Push configuration from the reference repository",
             responses={200: 'OK', 400: 'Bad request', 500: 'Internal error'},
             params={}
             )
    def get(self):
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


def generate_ingestion_orders(order_executor: IngestionOrderExecutor,
                              store: IngestionOrderStore,
                              ingestion_history_builder: IngestionHistoryBuilder):
    store.load()
    orders = store.orders()

    message_schema = os.path.join(os.path.dirname(__file__), 'resources/dataset_config_template.yml')
    for ingestion_order in orders:
        history_manager = ingestion_history_builder.build(dataset_id=ingestion_order['id'])
        order_executor.execute_ingestion_order(collection=ingestion_order,
                                               collection_config_template=message_schema,
                                               history_manager=history_manager)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ingestion for a list of collection ingestion streams")
    parser.add_argument("--refresh",
                        help="refresh interval in seconds to check for new or updated granules",
                        default=300)
    parser.add_argument("--git-url",
                        help="git repository from which the ingestion order list is pulled/saved")
    parser.add_argument("--git-branch",
                        help="git branch from which the ingestion order list is pulled/saved",
                        default="master")
    parser.add_argument("--git-token",
                        help="git personal access token used to access the repository")
    parser.add_argument("--local-ingestion-orders",
                        help="path to local ingestion orders file",
                        required=True)
    parser.add_argument('--rabbitmq_host',
                        default='localhost',
                        metavar='HOST',
                        help='RabbitMQ hostname to connect to. (Default: "localhost")')
    parser.add_argument('--rabbitmq_username',
                        default='guest',
                        metavar='USERNAME',
                        help='RabbitMQ username. (Default: "guest")')
    parser.add_argument('--rabbitmq_password',
                        default='guest',
                        metavar='PASSWORD',
                        help='RabbitMQ password. (Default: "guest")')
    parser.add_argument('--rabbitmq_queue',
                        default="nexus",
                        metavar="QUEUE",
                        help='Name of the RabbitMQ queue to consume from. (Default: "nexus")')
    history_group = parser.add_mutually_exclusive_group(required=True)
    history_group.add_argument("--history-path",
                               help="path to ingestion history local directory")
    history_group.add_argument("--history-url",
                               help="url to ingestion history solr database")
    return parser.parse_args()


def main():
    global order_store

    options = get_args()

    if options.local_ingestion_orders:
        order_store = FileIngestionOrderStore(path=options.local_ingestion_orders,
                                              order_template=templates.order_template)
    else:
        order_store = GitIngestionOrderStore(options.git_url,
                                             git_branch=options.git_branch,
                                             git_token=options.git_token,
                                             order_template=templates.order_template)
    if options.history_path:
        history_manager_builder = FileIngestionHistoryBuilder(history_path=options.history_path)
    else:
        history_manager_builder = SolrIngestionHistoryBuilder(solr_url=options.history_url)

    publisher = MessagePublisher(host=options.rabbitmq_host,
                                 username=options.rabbitmq_username,
                                 password=options.rabbitmq_password,
                                 queue=options.rabbitmq_queue)
    publisher.connect()
    order_executor = IngestionOrderExecutor(message_publisher=publisher)

    scheduler = BackgroundScheduler()
    scheduler.add_job(partial(generate_ingestion_orders, order_executor, order_store, history_manager_builder),
                      'interval',
                      seconds=int(options.refresh))

    scheduler.start()
    flask_app.run()


if __name__ == "__main__":
    main()

