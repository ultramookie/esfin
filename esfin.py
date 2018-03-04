#!/usr/bin/env python
#
# Use Excel to track expenses in CSV file.
# Use this script to insert transactions into Elasticsearch for analysis/graphing.
# Don't forget to create a template for this guy.
# The template is set to have all indices start with "esfin"
#

import argparse
import csv
import json
from elasticsearch import Elasticsearch
from pprint import pprint

def go():
    previous = ''
    postfix = 0
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', action='store', dest='file', help='File to import', required=True)
    parser.add_argument('-i', '--index', action='store', dest='index', help='Elasticsearch index name', required=True)
    parser.add_argument('-e', '--eshost', action='store', dest='eshost', help='Elasticsearch host', required=True)
    parser.add_argument('-p', '--esport', action='store', dest='esport', help='Elasticsearch port', required=True)
    parser.add_argument('-d', '--debug', action='store_true', dest='debug', required=False)
    parsed = parser.parse_args()

    # Get Elasticsearch Client
    es = Elasticsearch(hosts=[{"host":parsed.eshost, "port":parsed.esport}])
    es.indices.create(index=parsed.index, ignore=400)

    with open(parsed.file) as csvfile:
        reader = csv.DictReader(csvfile,dialect='excel')
        for row in reader:
            empties = 0
            for value in row.values():
                if value == '':
                    empties = empties+1

            if empties == 0:
                # Create id's based on date
                if row['date'] == previous:
                    postfix = postfix+1
                    row['id'] = row['date'] + str(postfix)
                else:
                    postfix = 0
                    previous = row['date']
                    row['id'] = row['date'] + str(postfix)

                row['merchant'] = unicode(row['merchant'], errors='replace')
                row['category'] = unicode(row['category'], errors='replace')

                # Debug stuff
                if parsed.debug:
                    pprint(row)
                    print(row['date'], row['merchant'], row['amount'], row['category'],row['id'])
                    print json.dumps(row)
                else:
                    # Dump into Elasticsearch
                    es.index(index=parsed.index, doc_type='transactions', id=row['id'], body=json.dumps(row))

go()
