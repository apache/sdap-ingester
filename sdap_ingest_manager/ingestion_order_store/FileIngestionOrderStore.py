from sdap_ingest_manager.ingestion_order_store import IngestionOrderStore


class FileIngestionOrderStore(IngestionOrderStore):
    def __init__(self, path: str, order_template):
        self._file_name = path
        super().__init__(order_template)

        self._read_from_file()
