import argparse
import asyncio
import logging
import os

from collection_manager.services import CollectionProcessor, CollectionWatcher, MessagePublisher
from collection_manager.services.history_manager import SolrIngestionHistoryBuilder, FileIngestionHistoryBuilder

logging.basicConfig(level=logging.INFO)
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def check_path(path) -> str:
    if not os.path.isabs(path):
        raise argparse.ArgumentError("Paths must be absolute.")
    return path


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch the filesystem for new granules, and publish messages to "
                                                 "RabbitMQ whenever they become available.")
    parser.add_argument("--collections-path",
                        help="Absolute path to collections configuration file",
                        metavar="PATH",
                        required=True)
    history_group = parser.add_mutually_exclusive_group(required=True)
    history_group.add_argument("--history-path",
                               metavar="PATH",
                               help="Absolute path to ingestion history local directory")
    history_group.add_argument("--history-url",
                               metavar="URL",
                               help="URL to ingestion history solr database")
    parser.add_argument('--rabbitmq-host',
                        default='localhost',
                        metavar='HOST',
                        help='RabbitMQ hostname to connect to. (Default: "localhost")')
    parser.add_argument('--rabbitmq-username',
                        default='guest',
                        metavar='USERNAME',
                        help='RabbitMQ username. (Default: "guest")')
    parser.add_argument('--rabbitmq-password',
                        default='guest',
                        metavar='PASSWORD',
                        help='RabbitMQ password. (Default: "guest")')
    parser.add_argument('--rabbitmq-queue',
                        default="nexus",
                        metavar="QUEUE",
                        help='Name of the RabbitMQ queue to consume from. (Default: "nexus")')
    parser.add_argument('--refresh',
                        default='30',
                        metavar="INTERVAL",
                        help='Number of seconds after which to reload the collections config file. (Default: 30)')

    return parser.parse_args()


async def main():
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
        collection_watcher = CollectionWatcher(collections_path=options.collections_path,
                                               collection_updated_callback=collection_processor.process_collection,
                                               granule_updated_callback=collection_processor.process_granule,
                                               collections_refresh_interval=int(options.refresh))

        collection_watcher.start_watching()

        while True:
            try:
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                return

    except Exception as e:
        logger.error(e)
        return


if __name__ == "__main__":
    asyncio.run(main())
