import ES_Query_Config as config_file
from elasticsearch import Elasticsearch

import csv


def preProcessFile():
    csvfile = open(config_file.output_file_path,"w")
    writer = csv.DictWriter(csvfile, fieldnames=config_file.output_csv_header)
    writer.writeheader()
    return writer


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': '127.0.0.1', 'port': 9200, 'timeout': 1200}])
    if _es.ping():
        print('Connected')
    else:
        print('Awww, it could not connect!')
    return _es

def prepareQuery(es):
    page = es.search(
        index=config_file.es_index,
        doc_type=config_file.es_type,
        scroll='2m',
        _source=config_file.required_fields,
        size=9000,
        request_timeout=1200,
        body=config_file.es_query
    )
    sid = page.get('_scroll_id')
    total_hits = page['hits']['total']
    if sid is None:
        print('No records found')
        exit(0)
    print ("Total Hits = " + str(total_hits))
    scroll_size = total_hits
    return page, sid, scroll_size

def startScrolling(page,sid,scroll_size,es,writer):
    first_run = True
    cnt = 0
    # Start scrolling
    while (scroll_size > 0):
        cnt = cnt + 1
        if first_run:
            first_run = False
        else:
            page = es.scroll(scroll_id=sid, scroll='2m', request_timeout=1200)
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        print("Scroll ID = " + sid + " ^&^ Scroll No = " + str(cnt) + " ^&^ Scroll Size = " + str(scroll_size))
        # print "scroll size: " + str(scroll_size)
        record = {}
        for field in config_file.output_csv_header:
            record[field] = ''
        for h in page['hits']['hits']:
            for field in config_file.output_csv_header:
                try:
                    record[field] = h['_source'][field]
                except:
                    record[field] = ""
            writer.writerow(record)



def main():
    writer = preProcessFile()
    es = connect_elasticsearch()
    page, sid, scroll_size = prepareQuery(es)
    startScrolling(page,sid,scroll_size,es,writer)

if __name__ == "__main__":
    main()

