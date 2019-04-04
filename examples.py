from pprint import pprint
import yaml
from time import sleep
import json, pprint, requests, textwrap
import os
from os import listdir
from os.path import isfile, join
import glob
import importlib

from parser.parse import getSql, getCypher

RUN_LIVY = True

def getJoin(q,olap="spark-sql"):

	try:
		qtype = q['query']['queryType']
		left = q['query']['left']
		right = q['query']['right']
		on = q['query']['on'][0]
		join_type = q['query']['type']
		name = q['result']['name']
		code_block = "val {} = {}.join({},Seq(\"{}\"),\"{}\")".format(name,left,right,on,join_type)
		return code_block
	except:
		return ""


PAPA_HOME = os.environ["PAPA_HOME"]
query_path = PAPA_HOME + "/refactored/queries/examples"


query_files = [join(query_path, f) for f in listdir(query_path) if isfile(join(query_path, f))]
query_files.sort()



for query_file in query_files:
	print("\nprocessing: " + query_file)
	with open(query_file) as stream: query = json.load(stream)
	qtype = query['query']['queryType']
	olap = query['olap']
	result = query['result']
	if qtype != "join":
		code_block = getSql(query)
		print("** sql ** :")
		print(code_block)
		code_block = getCypher(query)
		print("** cypher **:")
		print(code_block)
		
	else:
		# stitch the query
		print("join:")
		code_block = getJoin(query)
		print(code_block)




query = {
  "version": 0.1,
  "templateName":"ContentModel",
  "olap":"spark-sql",
  "result": {"name":"x1","type":"table"},
  "query": {
    "queryType": "groupBy",
    "dataSource": "ContentTable",
    "granularity": "day",
    "dimensions":["subject","identifier"],
    "filter": {
      "type": "and",
      "fields": [
      { "type": "selector", "dimension": "contentType", "value": "'Resource'" },
      { "type": "or", 
        "fields": [
          { "type": "selector", "dimension": "board", "value": "'CBSE'" },
          { "type": "selector", "dimension": "subject", "value": "'English'" }
        ]
      }
    ]
  }
}
}
print(getSql(query))