import datetime
import logging
import uuid
import elasticsearch
import elasticsearch.helpers


class ElasticAPI:
    def __init__(self, index=None, host='localhost', port=9200):
        self.es = elasticsearch.Elasticsearch(hosts=[{"host": host, "port": port}], scheme='http')
        self.es_index = index
        self.log = logging.getLogger('root')

    def push(self, data_dict):
        ts = datetime.datetime.utcnow()
        data_dict.update({'timestamp': ts})
        res = self.es.index(index=self.es_index, id=uuid.uuid1(), body=data_dict, request_timeout=10)
        return res["_shards"]["successful"] == 1

    def index_exists(self):
        exists = self.es.indices.exists(index=self.es_index)
        if not exists:
            raise ValueError(f"{self.es_index} does not exist!")
        return exists

    def trip_exists(self, trip_id):
        query = {"query": {"term": {"trip_id": trip_id}}}
        try:
            query_gen = elasticsearch.helpers.scan(self.es, index=self.es_index, query=query, request_timeout=5)
            return len(list(query_gen)) > 0

        except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout) as ex:
            self.log.exception(ex)
            self.log.error("Connection to ES server failed!")
            return None
