# coding=utf-8

from elasticsearch import Elasticsearch, ElasticsearchException
import json
import argparse
import logging
import os


FB_SEARCH_URL = "ec2-54-217-5-98.eu-west-1.compute.amazonaws.com"
FB_SEARCH_PORT = 9200
FB_SEARCH_INDEX = "intprof"
FB_SEARCH_TYPE = "profile"
MAX_RETRIES = 3
TIMEOUT_CUTOFF = 1
ES_LOG_PATH = "tmp/es_trace.log"
CHUNK_SIZE = 1000

SOC_DEMO_PARAMS = [u"gender", u"city", u"country"]


def init_loggers():
    # soc-dem client logger
    logging.basicConfig(filename="fb_stats.log", format='%(asctime)s %(message)s', level=logging.INFO)
    last_slash_pos = ES_LOG_PATH.rfind("/")
    if not os.path.exists(ES_LOG_PATH):
        os.makedirs(ES_LOG_PATH[:last_slash_pos])
    # es logger
    tracer = logging.getLogger('elasticsearch.trace')
    tracer.setLevel(logging.DEBUG)
    tracer.addHandler(logging.FileHandler(ES_LOG_PATH))


def parse_args():
    parser = argparse.ArgumentParser(description="Demographic frequency distribution"
                                                 " for the FB users with given ids")
    parser.add_argument('source', metavar='source', type=str,
                        help='A source file containing the ids of FB users')
    return parser.parse_args()


class FBSocDemoStatClient():
    def __init__(self, options):
        self.source = options.source
        self.fb_ids = self.__read_ids()
        self.es = Elasticsearch([{"host": FB_SEARCH_URL, "port": FB_SEARCH_PORT}], max_retries=MAX_RETRIES,
                                timeout_cutoff=TIMEOUT_CUTOFF)
        self.freq_distribution = self.__init_dist_dict()
        self.counter = 0

    def __init_dist_dict(self):
        res = dict()
        for key in SOC_DEMO_PARAMS:
            res[key] = dict()
        return res

    def __read_ids(self):
        ids_list = list()
        with open(self.source) as f:
            f.readline() # skipping the first line (header)
            for id_str in f:
                try:
                    ids_list.append(int(id_str))
                except ValueError:
                    logging.info("Invalid id detected in input file: %s" % id_str)
        return ids_list

    def __yield_ids(self):
        ids = self.fb_ids
        while True:
            chunk = ids[:CHUNK_SIZE]
            yield chunk
            ids = ids[CHUNK_SIZE:]
            if not ids:
                break

    def __yield_search_res(self):
        for chunk in self.__yield_ids():
            logging.info("Querying next chunk:")
            self.counter += len(chunk)
            logging.info("%d out of %d ids are read" % (self.counter, len(self.fb_ids)))
            yield self.es.search(index=FB_SEARCH_INDEX, doc_type=FB_SEARCH_TYPE,
                                 body={
                                     "query": {
                                         "filtered": {
                                             "query": {
                                                 "match_all": {}
                                             },
                                             "filter": {
                                                 "terms": {
                                                     "snid.fb.id": chunk
                                                 }
                                             }
                                         }
                                     }}, size=CHUNK_SIZE)

    def __build_freq_dict(self, profiles):
        for profile in profiles:
            for key in SOC_DEMO_PARAMS:
                profile_value = profile["_source"]["snid"]["fb"][0].get(key)
                dist_bin = self.freq_distribution[key]
                if profile_value:
                    if profile_value not in dist_bin:
                        dist_bin[profile_value] = 1
                    else:
                        dist_bin[profile_value] += 1

    def id_list_empty(self):
        return len(self.fb_ids) == 0

    def get_demographic_stats(self):
        for res in self.__yield_search_res():
            self.__build_freq_dict(res['hits']['hits'])
        return self.freq_distribution

    @staticmethod
    def print_stats(stats):
        print "Soc-demo statistics:\n\n"
        print json.dumps(stats, ensure_ascii=False, sort_keys=True, indent=4).encode('utf-8')


options = parse_args()
init_loggers()
try:
    client = FBSocDemoStatClient(options)
    if client.id_list_empty():
        logging.info("The input id list is empty")
        print "The input id list is empty"
    else:
        logging.info("Starting search...")
        stats = client.get_demographic_stats()
        client.print_stats(stats)
except ElasticsearchException as ex:
    logging.error("Error while communicating with ES server: %s" % ex.error)
    print "Error while retrieving data from ES"
