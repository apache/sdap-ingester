import logging
import asyncio
import functools

from common.async_utils.AsyncUtils import run_in_executor
from granule_ingester.writers.MetadataStore import MetadataStore
from elasticsearch import Elasticsearch
from granule_ingester.exceptions import (ElasticsearchFailedHealthCheckError, ElasticsearchLostConnectionError)
from nexusproto.DataTile_pb2 import NexusTile, TileSummary
from datetime import datetime
from pathlib import Path
from typing import Dict


class ElasticsearchStore(MetadataStore):
    def __init__(self, elastic_url: str, username: str, password: str, index: str):
        super().__init__()
        self.TABLE_NAME = 'sea_surface_temp'
        self.iso = '%Y-%m-%dT%H:%M:%SZ'
        self.elastic_url = elastic_url
        self.username = username
        self.password = password
        self.index = index
        self.geo_precision = 3
        # self.collection = 'nexustiles'
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG)
        self.elastic = None

    def get_connection(self) -> Elasticsearch:
        if self.elastic_url:
            if not self.username or not self.password:
                return Elasticsearch([self.elastic_url])
            else:
                return Elasticsearch([self.elastic_url], http_auth=(self.username, self.password))
        else:
            raise RuntimeError('No Elasticsearch URL')
    
    def connect(self):
        self.elastic = self.get_connection()

    async def health_check(self):
        connection = self.get_connection()

        if not connection.ping():
            raise ElasticsearchFailedHealthCheckError

    async def save_metadata(self, nexus_tile: NexusTile) -> None:
        es_doc = self.build_es_doc(nexus_tile)
        await self.save_document(es_doc)
    
    @run_in_executor
    def save_document(self, doc: dict):
        try:
            self.elastic.index(self.index, doc)
        except:
            raise ElasticsearchLostConnectionError

    def build_es_doc(self, tile: NexusTile) -> Dict:
        summary: TileSummary = tile.summary
        bbox: TileSummary.BBox = summary.bbox
        stats: TileSummary.DataStats = summary.stats

        min_time = datetime.strftime(datetime.utcfromtimestamp(stats.min_time), self.iso)
        max_time = datetime.strftime(datetime.utcfromtimestamp(stats.max_time), self.iso)
        day_of_year = datetime.utcfromtimestamp(stats.min_time).timetuple().tm_yday
        geo = self.determine_geo(bbox)

        granule_file_name: str = Path(summary.granule).name  # get base filename

        tile_type = tile.tile.WhichOneof("tile_type")
        tile_data = getattr(tile.tile, tile_type)

        var_name = summary.standard_name if summary.standard_name else summary.data_var_name

        input_document = {
            'table_s': self.TABLE_NAME,
            'geo': geo,
            'id': summary.tile_id,
            'solr_id_s': '{ds_name}!{tile_id}'.format(ds_name=summary.dataset_name, tile_id=summary.tile_id),
            'sectionSpec_s': summary.section_spec,
            'dataset_s': summary.dataset_name,
            'granule_s': granule_file_name,
            'tile_var_name_s': var_name,
            'day_of_year_i': day_of_year,
            'tile_min_lon': round(bbox.lon_min, 3),
            'tile_max_lon': round(bbox.lon_max, 3),
            'tile_min_lat': round(bbox.lat_min, 3),
            'tile_max_lat': round(bbox.lat_max, 3),
            'tile_depth': tile_data.depth,
            'tile_min_time_dt': min_time,
            'tile_max_time_dt': max_time,
            'tile_min_val_d': stats.min,
            'tile_max_val_d': stats.max,
            'tile_avg_val_d': stats.mean,
            'tile_count_i': int(stats.count)
        }

        ecco_tile_id = getattr(tile_data, 'tile', None)
        if ecco_tile_id:
            input_document['ecco_tile'] = ecco_tile_id

        for attribute in summary.global_attributes:
            input_document[attribute.getName()] = attribute.getValues(
                0) if attribute.getValuesCount() == 1 else attribute.getValuesList()

        return input_document
    
    @staticmethod
    def _format_latlon_string(value):
        rounded_value = round(value, 3)
        return '{:.3f}'.format(rounded_value)
    
    
    @classmethod
    def determine_geo(cls, bbox: TileSummary.BBox) -> str:
        lat_min = cls._format_latlon_string(bbox.lat_min)
        lat_max = cls._format_latlon_string(bbox.lat_max)
        lon_min = cls._format_latlon_string(bbox.lon_min)
        lon_max = cls._format_latlon_string(bbox.lon_max)
        
        # If lat min = lat max and lon min = lon max, index the 'geo' bounding box as a POINT instead of a POLYGON
        if lat_min == lat_max and lon_min == lon_max:
            geo = 'POINT({} {})'.format(lon_min, lat_min)
        
        # If lat min = lat max but lon min != lon max, or lon min = lon max but lat min != lat max, then we essentially have a line.
        elif lat_min == lat_max or lon_min == lon_max:
            geo = 'LINESTRING({} {}, {} {})'.format(lon_min, lat_min, lon_max, lat_min)
        
	    # All other cases should use POLYGON
        else:
            geo = 'POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))'.format(lon_min, lat_min,
                                                                        lon_max, lat_min,
                                                                        lon_max, lat_max,
                                                                        lon_min, lat_max,
                                                                        lon_min, lat_min)

        return geo
