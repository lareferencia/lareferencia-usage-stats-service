
import re
import sys
from textwrap import indent
from unittest import result
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import awswrangler as wr
from config import read_ini
from opensearchpy import OpenSearch, exceptions



app = FastAPI(
    title="LA Referencia Usage Statistics API",
    description="API for usage statistics of the LA Referencia service",
    version="2.0.0",
    terms_of_service="",
    contact={
        "name": "Lautaro Matas",
        "url": "http://www.lareferencia.info",
        "email": "lautaro.matas@lareferencia.redclara.net",
    },
    license_info={
        "name": "AGPL-3.0",
        "url": "https://www.gnu.org/licenses/agpl-3.0.html",
    }, 
    #docs_url="/api/usage_stats/v1/docs",
    #root_path="/api/usage_stats/v1",
)

origins = ["*"]

try: 

    config_file_path = "config.ini"

    # read config file
    config = read_ini(config_file_path);

    # get config values
    cors_filename = config["CORS"]["FILENAME"]

    # read cors file
    with open(cors_filename, "r") as f:
        origins = f.read().splitlines()
        
except Exception as e:
    print("Error reading config file: %s" % e)
    sys.exit(1)


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def build_index_name(source, country = '*', year = '*'):
    source = source.replace("::", "_")
    index_name = 'usage_stats_%s_%s_%s' % (country, source, year)
    index_name = index_name.lower() 
    return index_name


@app.get("/report/repositoryWidget")
async def repositoryWidget(identifier: str = None, source: str = '*', start_date: 'str' = 'now-1y', end_date: 'str' = 'now', time_unit : str = 'year'):

    host = config["OPENSEARCH"]["HOST"]
    port = int(config["OPENSEARCH"]["PORT"])
    auth = (config["OPENSEARCH"]["USER"], config["OPENSEARCH"]["PASSWORD"]) 

    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        use_ssl = False
    )

    query = {
       "aggs": {
            "views": { "sum": { "field": "views" }},
            "downloads": { "sum": { "field": "downloads" }},
            "conversions": { "sum": {"field": "conversions" }},
            "outlinks": { "sum": { "field": "outlinks" }},
            
             "level": {
                        "terms": {"field": "level",
                        "order": { "_key": "desc"},
                        "size": 5 },
                    
                        "aggs": {
                              "views": { "sum": { "field": "views" }},
                              "downloads": { "sum": { "field": "downloads" }},
                              "conversions": { "sum": {"field": "conversions" }},
                              "outlinks": { "sum": { "field": "outlinks" }}
                        }
            },
        
            "time": {
                "date_histogram": {
                    "field": "date",
                    "calendar_interval": "1m",
                    "min_doc_count": 1
                },
                "aggs": {
                    "level": {
                        "terms": {"field": "level",
                        "order": { "_key": "desc"},
                        "size": 5 },
                    
                        "aggs": {
                              "views": { "sum": { "field": "views" }},
                              "downloads": { "sum": { "field": "downloads" }},
                              "conversions": { "sum": {"field": "conversions" }},
                              "outlinks": { "sum": { "field": "outlinks" }}
                        }
                    }
                }
            }
        },
        "size": 0,
        
        "query": {
          "bool": {
            "should": [
              {
                  "match_phrase": {
                        "identifier": "oai:sedici.unlp.edu.ar:*"
                  }
              }
            ],
            "minimum_should_match": 1,
            "must": [],
            "filter": [
              {
                "range": {
                "date": {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "strict_date_optional_time"
                  }
                }
              }
            ]
          }
        },
        
        "track_total_hits": "false"
    }

    # if identifier is None then remove the identifier filter
    #if identifier is None:
    #    del query['query']['bool']['should'][0]
    #    del query['query']['bool']['minimum_should_match']

    # indices = set()

    # indices.add(build_index_name(regional_source, '00', '*'))
    # indices.add(build_index_name(national_source, country, '*'))
    # indices.add(build_index_name(repository_source, country, '*'))
    
    # indices = list(indices)


    indices = ["test-processor-*"]

    try:
        response = client.search(
            body = query,
            index = ','.join(indices),
        )
    except exceptions.NotFoundError:
        print("Index not found: %s" % ','.join(indices))
        return {}

    return response.get("aggregations", {})


