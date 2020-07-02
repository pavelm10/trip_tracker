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

    def bulk_push(self, iterator):
        return elasticsearch.helpers.bulk(self.es, actions=self.process_iterator(iterator))

    def process_iterator(self, iterator):
        ts = datetime.datetime.utcnow()
        for data_dict in iterator:
            data_dict.update({'timestamp': ts, '_id': uuid.uuid1(), '_index': self.es_index})
            yield data_dict

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

    def delete_trip(self, trip_id):
        query = {"query": {"term": {"trip_id": trip_id}}}
        try:
            self.es.delete_by_query(index=self.es_index, body=query)

        except (elasticsearch.ConnectionError, elasticsearch.ConnectionTimeout) as ex:
            self.log.exception(ex)
            self.log.error("Connection to ES server failed!")


if __name__ == "__main__":
    import argparse

    argp = argparse.ArgumentParser()
    argp.add_argument('--index',
                      default=None)
    argp.add_argument('--trip-id',
                      dest='trip_id',
                      default=None)
    argp.add_argument('-ct',
                      dest='check_trip',
                      action='store_true',
                      help='check trip existence')
    argp.add_argument('-dt',
                      dest='delete_trip',
                      action='store_true',
                      help='delete trip')

    args = argp.parse_args()

    elastic = ElasticAPI(index=args.index)
    elastic.index_exists()

    if args.check_trip:
        trip_exists = elastic.trip_exists(args.trip_id)
        print(f"{args.trip_id} exists in index {args.index}: {trip_exists}")

    if args.delete_trip:
        elastic.delete_trip(args.trip_id)
        print(f"{args.trip_id} deleted from index {args.index}")
