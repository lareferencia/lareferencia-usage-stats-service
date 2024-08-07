
import sys
from textwrap import indent
from unittest import result
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException

from config import read_ini
from opensearchpy import OpenSearch, exceptions

from lareferenciastatsdb import UsageStatsDatabaseHelper, IdentifierPrefixNotFoundException

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

    # get index prefix
    index_prefix = config["USAGE_STATS_INDEX"]["INDEX_PREFIX"]

    # read cors file
    with open(cors_filename, "r") as f:
        origins = f.read().splitlines()

    ## dbhelper
    dbhelper = UsageStatsDatabaseHelper(config)    

    # opensearch
    host = config["OPENSEARCH"]["HOST"]
    port = int(config["OPENSEARCH"]["PORT"])
    is_ssl = str(config["OPENSEARCH"]["SSL"]).lower() == "true"
    auth = (config["OPENSEARCH"]["USER"], config["OPENSEARCH"]["PASSWORD"]) 

    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        use_ssl = is_ssl,
    )

    print("Connected to OpenSearch")
    print ( client.info() )
        

except Exception as e:
    print("Error: %s" % e)
    sys.exit(1)


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)




def parametrize_query(identifier, start_date, end_date, time_unit):

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
                        "identifier": identifier
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

    # if identifier is None, then remove the identifier from the query
    if identifier is None:
        del query["query"]["bool"]["should"]
        del query['query']['bool']['minimum_should_match']

    return query
  

def parametrize_bycountry_query(identifier, start_date, end_date, limit=10):

    query = {
        "aggs": {
            "views": {
            "sum": {
                "field": "views"
            }
            },
            "downloads": {
            "sum": {
                "field": "downloads"
            }
            },
            "conversions": {
            "sum": {
                "field": "conversions"
            }
            },
            "outlinks": {
            "sum": {
                "field": "outlinks"
            }
            },
            "country": {
            "nested": {
            "path": "stats_by_country"
            
            },
            "aggs": {
                "views": {
                "terms": {
                    "field": "stats_by_country.country",
                    "size": limit
                }, 
                "aggs": {
                    "count": {
                    "sum": {
                        "field": "stats_by_country.views"
                    }
                    }
                } 
                },
                "downloads": {
                "terms": {
                    "field": "stats_by_country.country",
                    "size": limit
                }, 
                "aggs": {
                    "count": {
                    "sum": {
                        "field": "stats_by_country.downloads"
                    }
                    }
                } 
                },
                "outlinks": {
                "terms": {
                    "field": "stats_by_country.country",
                    "size": limit
                }, 
                "aggs": {
                    "count": {
                    "sum": {
                        "field": "stats_by_country.outlinks"
                    }
                    }
                } 
                },
                "conversions": {
                "terms": {
                    "field": "stats_by_country.country",
                    "size": limit
                }, 
                "aggs": {
                    "count": {
                    "sum": {
                        "field": "stats_by_country.conversions"
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
            "must": [
                {
                "match_phrase": {
                    "identifier": identifier
                }
                }
            ],
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

    # if identifier is None, then remove the identifier from the query
    if identifier is None:
        del query["query"]["bool"]["should"]
        del query['query']['bool']['minimum_should_match']

    return query






## repositoryWidget endpoint
@app.get("/report/itemWidget")
async def itemWidget(identifier: str = None, source: str = '*', start_date: 'str' = 'now-1y', end_date: 'str' = 'now', time_unit : str = 'year'):

    # parametrize the query based on the parameters
    query = parametrize_query(identifier, start_date, end_date, time_unit)
    
    try:

        try: 
            ## first try to get the indices from the identifier (this works if the repository is registered in the database)
            indices = dbhelper.get_indices_from_identifier(index_prefix, identifier)
            print ("indices from identifier: %s" % indices)
        except IdentifierPrefixNotFoundException as e:
            ## if the identifier is not found in the database, then try to get the indices from the source (this will get national and regional statistics only)
            indices = dbhelper.get_indices_from_source(index_prefix, source)

        if len(indices) == 0:
            raise HTTPException(status_code=404, detail="The source %s and identifier %s are not present in the database" % (source, identifier))
        
        print ("indices: %s" % indices)

        response = client.search(
            body = query,
            index = ','.join(indices),
            #index = "usage-stats-48-*",
        )
    except Exception as e:
        #print ("Error: %s" % e)
        # stacktrace
        # import traceback
        # traceback.print_exc()
        raise HTTPException(status_code=404, detail=str(e))
    
    if response is None or response.get("aggregations") is None:
        raise HTTPException(status_code=404, detail="Not found")   
 
    return response.get("aggregations", {})


## repositoryWidgetByCountry endpoint
@app.get("/report/itemWidgetByCountry")
async def itemWidgetByCountry(identifier: str = None, source: str = '*', start_date: 'str' = 'now-1y', end_date: 'str' = 'now', limit: int = 10):

    # parametrize the query based on the parameters
    query = parametrize_bycountry_query(identifier, start_date, end_date, limit)
    
    try:

        try: 
            ## first try to get the indices from the identifier (this works if the repository is registered in the database)
            indices = dbhelper.get_indices_from_identifier(index_prefix, identifier)
            print ("indices from identifier: %s" % indices)
        except IdentifierPrefixNotFoundException as e:
            ## if the identifier is not found in the database, then try to get the indices from the source (this will get national and regional statistics only)
            indices = dbhelper.get_indices_from_source(index_prefix, source)

        if len(indices) == 0:
            raise HTTPException(status_code=404, detail="The source %s and identifier %s are not present in the database" % (source, identifier))
        
        print ("indices: %s" % indices)

        response = client.search(
            body = query,
            index = ','.join(indices),
            #index = "usage-stats-48-*",
        )
    except Exception as e:
        #print ("Error: %s" % e)
        # stacktrace
        # import traceback
        # traceback.print_exc()
        raise HTTPException(status_code=404, detail=str(e))
    
    if response is None or response.get("aggregations") is None:
        raise HTTPException(status_code=404, detail="Not found")   
 
    return response.get("aggregations", {})


## repositoryWidget endpoint
@app.get("/report/repositoryWidget")
async def repositoryWidget(source_id: str = '*', start_date: 'str' = 'now-1y', end_date: 'str' = 'now', time_unit : str = 'year'):

    source = dbhelper.get_source_by_id(source_id)

    if source is None:
        raise HTTPException(status_code=404, detail="The source %s is not present in the database" % (source_id))
    
    if source.type != "R":
        raise HTTPException(status_code=404, detail="The source %s is not a repository" % (source))

    #try:

    identifier_prefix = dbhelper.get_identifier_prefix_from_source(source_id)
    print("identifier_prefix: %s" % identifier_prefix)

    identifier_pattern = identifier_prefix + "*"
    query = parametrize_query(identifier_pattern, start_date, end_date, time_unit)

    indices = dbhelper.get_indices_from_identifier(index_prefix,identifier_prefix)

    print("indices: %s" % indices)

    if len(indices) == 0:
        raise HTTPException(status_code=404, detail="The source %s is not present in the database" % (source))
    

    try:
        response = client.search(
            body = query,
            index = ','.join(indices),
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    if response is None or response.get("aggregations") is None:
        raise HTTPException(status_code=404, detail="Not found")   
 
    return response.get("aggregations", {})

## repositoryWidgetBycountry endpoint
@app.get("/report/repositoryWidgetByCountry")
async def repositoryWidgetByCountry(source_id: str = '*', start_date: 'str' = 'now-1y', end_date: 'str' = 'now', limit: int = 10):
    source = dbhelper.get_source_by_id(source_id)

    if source is None:
        raise HTTPException(status_code=404, detail="The source %s is not present in the database" % (source_id))
    
    if source.type != "R":
        raise HTTPException(status_code=404, detail="The source %s is not a repository" % (source))

    #try:

    identifier_prefix = dbhelper.get_identifier_prefix_from_source(source_id)
    print("identifier_prefix: %s" % identifier_prefix)

    identifier_pattern = identifier_prefix + "*"
    query = parametrize_bycountry_query(identifier_pattern, start_date, end_date, limit)

    #print("query: %s" % query)

    indices = dbhelper.get_indices_from_identifier(index_prefix,identifier_prefix)

    print("indices: %s" % indices)

    if len(indices) == 0:
        raise HTTPException(status_code=404, detail="The source %s is not present in the database" % (source))
    
    try:
        response = client.search(
            body = query,
            index = ','.join(indices),
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    if response is None or response.get("aggregations") is None:
        raise HTTPException(status_code=404, detail="Not found")   

    return response.get("aggregations", {})


## repositoryWidget endpoint
@app.get("/report/nationalWidget")
async def repositoryWidget(source_id: str = '*', start_date: 'str' = 'now-1y', end_date: 'str' = 'now', time_unit : str = 'year'):

    source = dbhelper.get_source_by_id(source_id)

    if source is None:
        raise HTTPException(status_code=404, detail="The source %s is not present in the database" % (source_id))
    
    if source.type != "N":
        raise HTTPException(status_code=404, detail="The source %s is not a national aggregator" % (source))


    indices = dbhelper.get_indices_from_national(index_prefix,source_id)

    print("indices: %s" % indices)

    if len(indices) == 0:
        raise HTTPException(status_code=404, detail="The source %s is not present in the database" % (source))
    
    query = parametrize_query(None, start_date, end_date, time_unit)

    #print("query: %s" % query)

    try:
        response = client.search(
            body = query,
            index = ','.join(indices),
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

    if response is None or response.get("aggregations") is None:
        raise HTTPException(status_code=404, detail="Not found")   
 
    return response.get("aggregations", {})


## repositoryWidget endpoint
@app.get("/report/itemWidgetByCountry")
async def itemWidgetByCountry(identifier: str = None, source: str = '*', start_date: 'str' = 'now-1y', end_date: 'str' = 'now', time_unit : str = 'year'):

    # parametrize the query based on the parameters
    query = parametrize_query(identifier, start_date, end_date, time_unit)

    #print("query: %s" % query)
    
    try: 
        ## first try to get the indices from the identifier (this works if the repository is registered in the database)
        indices = dbhelper.get_indices_from_identifier(index_prefix, identifier)
        print ("indices from identifier: %s" % indices)
    except IdentifierPrefixNotFoundException as e:
        ## if the identifier is not found in the database, then try to get the indices from the source (this will get national and regional statistics only)
        indices = dbhelper.get_indices_from_source(index_prefix, source)

    if len(indices) == 0:
        raise HTTPException(status_code=404, detail="The source %s and identifier %s are not present in the database" % (source, identifier))
    
    print ("indices: %s" % indices)

    try:
        response = client.search(
            body = query,
            index = ','.join(indices),
            #index = "usage-stats-48-*",
        )
    except Exception as e:
        #print ("Error: %s" % e)
        # stacktrace
        # import traceback
        # traceback.print_exc()
        raise HTTPException(status_code=404, detail=str(e))
    
    if response is None or response.get("aggregations") is None:
        raise HTTPException(status_code=404, detail="Not found")   
 
    return response.get("aggregations", {})