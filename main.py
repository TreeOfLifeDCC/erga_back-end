from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI

from constants import DATA_PORTAL_AGGREGATIONS

app = FastAPI()
es = AsyncElasticsearch(["localhost:9200"])


@app.get("/{index}")
async def root(index: str, offset: int = 0, limit: int = 15,
               sort: str = "rank:desc", filter: str | None = None,
               search: str | None = None):
    # data structure for ES query
    body = dict()
    # building aggregations for every request
    body["aggs"] = dict()
    for aggregation_field in DATA_PORTAL_AGGREGATIONS:
        body["aggs"][aggregation_field] = {
            "terms": {"field": aggregation_field}
        }
    # adding filters, format: filter_name1:filter_value1, etc...
    if filter:
        filters = filter.split(",")
        body["query"] = {"bool": {"filter": []}}
        for filter_item in filters:
            filter_name, filter_value = filter_item.split(":")
            body["query"]["bool"]["filter"].append(
                {"term": {filter_name: filter_value}}
            )

    # adding search string
    if search:
        # body already has filter parameters
        if "query" in body:
            # body["query"]["bool"].update({"should": []})
            body["query"]["bool"].update({"must": {}})
        else:
            # body["query"] = {"bool": {"should": []}}
            body["query"] = {"bool": {"must": {}}}
        body["query"]["bool"]["must"] = {"bool": {"should": []}}
        body["query"]["bool"]["must"]["bool"]["should"].append(
            {"wildcard": {"organism": {"value": f"*{search}*",
                                       "case_insensitive": True}}}
        )
        body["query"]["bool"]["must"]["bool"]["should"].append(
            {"wildcard": {"commonName": {"value": f"*{search}*",
                                         "case_insensitive": True}}}
        )

    response = await es.search(
        index=index, sort=sort, from_=offset, size=limit, body=body
    )
    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    data['aggregations'] = response['aggregations']
    return data


@app.get("/{index}/{record_id}")
async def details(index: str, record_id: str):
    response = await es.search(index=index, q=f'_id:{record_id}')
    data = dict()
    data['count'] = response['hits']['total']['value']
    data['results'] = response['hits']['hits']
    return data
