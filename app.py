import requests
import os
import json
import configparser
import yaml
import time
import subprocess
import pandas as pd
import numpy as np
import pandas as pd
from flask import Flask
from flask import request, jsonify, Response
from parser.parse import getSql, getCypher, getJoin

app = Flask(__name__)

@app.route('/translate', methods = ['POST'])
def translate():
    
    if (request.is_json):
        try:
            query = request.get_json()
            print(query)
            #query = query["query"]
            ln = query["ln"]
    

        except: 
            raise InvalidRequest()
            status = Response(status=400) 
        
        try:
            # translate the query
            qtype = query['query']['queryType']
            olap = query['olap']
            result = query['result']
            if qtype != "join":
                if ln =="sql":
                    code_block = getSql(query)
                elif  ln =="cypher":
                    code_block = getCypher(query)
                else:
                    code_bloc = None
            else:
                # stitch the query
                code_block = getJoin(query)

        except:
            raise ValueError('Can not process')
            status = Response(status=400)
        
        
        time_format = time.strftime("%Y-%m-%d %H:%M:%S:%s")
        date = time.strftime("%Y-%m-%d")
        api_response= {
                "id": "sunbird.org.nlq",
                "ver": "v1",
                "ts": time_format,
                "responseCode": "OK",
                "result": {
                    "status": 200,
                    "query" : code_block
                }
            }
        print('*****')    
        print("code",api_response)
        return jsonify(api_response)
app.run(host='0.0.0.0', port= 3580)