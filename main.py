
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
    version="0.0.1",
    terms_of_service="",
    contact={
        "name": "Lautaro Matas",
        "url": "http://www.lareferencia.info",
        "email": "lautaro.matas@redclara.net",
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
async def repositoryWidget(identifier: str = None, country : 'str' = '*',  regional_source:'str' = '*', national_source: 'str' = '*', repository_source: str = '*', start_date: 'str' = 'now-1y', end_date: 'str' = 'now', time_unit : str = 'year'):

    host = config["OPENSEARCH"]["HOST"]
    port = int(config["OPENSEARCH"]["PORT"])
    auth = (config["OPENSEARCH"]["USER"], config["OPENSEARCH"]["PASSWORD"]) 

    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        # http_auth = auth,
        # client_cert = client_cert_path,
        # client_key = client_key_path,
        use_ssl = False
    )

    query = {
        "aggs": {
        "level": {
            
            "terms": {
            "field": "container.level",
            "order": {
            "_count": "desc"
            },
            "size": 5
            },
            
            "aggs": {
            
            "action": {
                "terms": {
                "field": "event.action",
                "order": {
                    "_count": "desc"
                },
                "size": 3
                },
                "aggs": {
                "time": {
                    "date_histogram": {
                    "field": "event.created",
                    "calendar_interval": time_unit,
                    }
                }
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
                "event.target.oai_identifier": identifier #"oai:repositorio.concytec.gob.pe:20.500.12390/2238"
                }
            }
            ],
            "minimum_should_match": 1,
            "must": [],
            
            "filter": [
            {
                "range": {
                "event.created": {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "strict_date_optional_time"
                }
                }
            }
            ]
            
            
        }
        }
    }

    # if identifier is None then remove the identifier filter
    if identifier is None:
        del query['query']['bool']['should'][0]
        del query['query']['bool']['minimum_should_match']

    indices = set()

    indices.add(build_index_name(regional_source, '00', '*'))
    indices.add(build_index_name(national_source, country, '*'))
    indices.add(build_index_name(repository_source, country, '*'))
    
    indices = list(indices)


    try:
        response = client.search(
            body = query,
            index = ','.join(indices),
        )
    except exceptions.NotFoundError:
        print("Index not found: %s" % ','.join(indices))
        return {}

    return response.get("aggregations", {})

@app.get("/report/byLevelActionTime")
async def byLevelActionTime(identifier: str = None, country: 'str' = None, source : str = None, year: int = None, start_date: 'str' = 'now-1y', end_date: 'str' = 'now', time_unit : str = 'year'):

    host = config["OPENSEARCH"]["HOST"]
    port = int(config["OPENSEARCH"]["PORT"])
    auth = (config["OPENSEARCH"]["USER"], config["OPENSEARCH"]["PASSWORD"]) 

    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        # http_auth = auth,
        # client_cert = client_cert_path,
        # client_key = client_key_path,
        use_ssl = False
    )

    query = {
        "aggs": {
        "level": {
            
            "terms": {
            "field": "container.level",
            "order": {
            "_count": "desc"
            },
            "size": 5
            },
            
            "aggs": {
            
            "action": {
                "terms": {
                "field": "event.action",
                "order": {
                    "_count": "desc"
                },
                "size": 3
                },
                "aggs": {
                "time": {
                    "date_histogram": {
                    "field": "event.created",
                    "calendar_interval": time_unit,
                    }
                }
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
                "event.target.oai_identifier": identifier #"oai:repositorio.concytec.gob.pe:20.500.12390/2238"
                }
            }
            ],
            "minimum_should_match": 1,
            "must": [],
            
            "filter": [
            {
                "range": {
                "event.created": {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "strict_date_optional_time"
                }
                }
            }
            ]
            
            
        }
        }
    }

    # if identifier is None then remove the identifier filter
    if identifier is None:
        del query['query']['bool']['should'][0]
        del query['query']['bool']['minimum_should_match']

    
    # if source is None then use wildcard
    if source is None:
        source = "*"
    else:
        source = source.replace('::', '_')

    # if country is None then use wildcard
    if country is None:
        country = "*"
   
    # if year is None then use wildcard
    if year is None:
        year = "*"

    # build index name from parameters
    index_name = 'usage_stats_%s_%s_%s' % (country, source, year)
    index_name = index_name.lower() 

    try:
        response = client.search(
            body = query,
            index = index_name
        )
    except exceptions.NotFoundError:
        print("Index not found: %s" % index_name)
        return {}

    return response.get("aggregations", {})

@app.get("/report/byRepositoryActionTime")
async def byRepositoryActionTime(identifier: str = None, repository_id: str = None, country: 'str' = None, source : str = None, year: int = None, start_date: 'str' = 'now-1y', end_date: 'str' = 'now', time_unit : str = 'year'):

    host = config["OPENSEARCH"]["HOST"]
    port = int(config["OPENSEARCH"]["PORT"])
    auth = (config["OPENSEARCH"]["USER"], config["OPENSEARCH"]["PASSWORD"]) 

    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        # http_auth = auth,
        # client_cert = client_cert_path,
        # client_key = client_key_path,
        use_ssl = False
    )

    query = {
        "aggs": {
        "repository": {
            
            "terms": {
            "field": "event.target.repository_id",
            "order": {
            "_count": "desc"
            },
            "size": 5
            },
            
            "aggs": {
            
            "action": {
                "terms": {
                "field": "event.action",
                "order": {
                    "_count": "desc"
                },
                "size": 3
                },
                "aggs": {
                "time": {
                    "date_histogram": {
                    "field": "event.created",
                    "calendar_interval": time_unit,
                    }
                }
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
                "event.target.oai_identifier": identifier,
                "event.target.repository_id": repository_id
                }
            }
            ],
            "minimum_should_match": 1,
            "must": [],
            
            "filter": [
            {
                "range": {
                "event.created": {
                    "gte": start_date,
                    "lte": end_date,
                    "format": "strict_date_optional_time"
                }
                }
            }
            ]
            
            
        }
        }
    }

    # if identifier is None then remove the identifier filter
    if identifier is None:
        del query['query']['bool']['should'][0]['match_phrase']['event.target.oai_identifier']

    # if repository_id is None then remove the repository_id filter        
    if repository_id is None:
        del query['query']['bool']['should'][0]['match_phrase']['event.target.repository_id']

    # if repository_id is None then remove the should filter
    if repository_id is None and identifier is None:
        del query['query']['bool']['should'][0]
        del query['query']['bool']['minimum_should_match']

    # if source is None then use wildcard
    if source is None:
        source = "*"
    else:
        source = source.replace('::', '_')

    # if country is None then use wildcard
    if country is None:
        country = "*"
   
    # if year is None then use wildcard
    if year is None:
        year = "*"

    # build index name from parameters
    index_name = 'usage_stats_%s_%s_%s' % (country, source, year)
    index_name = index_name.lower() 

    try:
        response = client.search(
            body = query,
            index = index_name
        )
    except exceptions.NotFoundError:
        print("Index not found: %s" % index_name)
        return {}

    return response.get("aggregations", {})

