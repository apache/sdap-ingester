import argparse
import logging
import os
import time

from collection_manager.services import CollectionProcessor, CollectionWatcher, MessagePublisher
from collection_manager.services.history_manager import SolrIngestionHistoryBuilder, FileIngestionHistoryBuilder

logging.basicConfig(level=logging.INFO)
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger("collection_manager")


def check_path(path) -> str:
    if not os.path.isabs(path):
        raise argparse.ArgumentError("Paths must be absolute.")
    return path


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ingestion for a list of collection ingestion streams")
    parser.add_argument("--refresh",
                        help="refresh interval in seconds to check for new or updated granules",
                        default=300)
    parser.add_argument("--collections",
                        help="Absolute path to collections configuration file",
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
                               help="Absolute path to ingestion history local directory")
    history_group.add_argument("--history-url",
                               help="URL to ingestion history solr database")

    return parser.parse_args()


def main():
    try:
        options = get_args()

        if options.history_path:
            history_manager_builder = FileIngestionHistoryBuilder(history_path=options.history_path)
        else:
            history_manager_builder = SolrIngestionHistoryBuilder(solr_url=options.history_url)
        publisher = MessagePublisher(host=options.rabbitmq_host,
                                     username=options.rabbitmq_username,
                                     password=options.rabbitmq_password,
                                     queue=options.rabbitmq_queue)
        publisher.connect()
        collection_processor = CollectionProcessor(message_publisher=publisher,
                                                   history_manager_builder=history_manager_builder)
        collection_watcher = CollectionWatcher(collections_path=options.collections,
                                               collection_updated_callback=collection_processor.process_collection,
                                               granule_updated_callback=collection_processor.process_granule)

        collection_watcher.start_watching()
        while True:
            time.sleep(1)

    except Exception as e:
        logger.error(e)
        return


if __name__ == "__main__":
    main()
