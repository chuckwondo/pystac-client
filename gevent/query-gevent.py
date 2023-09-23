import gevent
from gevent import monkey

monkey.patch_socket()
monkey.patch_ssl()

from typing import Any, Mapping, Sequence

import pandas as pd
import requests

import pystac_client


def sync_query(intersects: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
    search_start = "2018-01-01"
    search_end = "2019-12-31"
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1"
    )

    # The time frame in which we search for non-cloudy imagery
    search = catalog.search(
        collections=["sentinel-2-l2a"],
        intersects=intersects,
        datetime=[search_start, search_end],
        query={"eo:cloud_cover": {"lt": 10}},
        limit=500,
    )
    results = []
    timeout = (20, 120)

    with requests.Session() as session:
        r = session.post(search.url, json=search.get_parameters(), timeout=timeout)
        resp = r.json()
        results.extend(resp["features"])

        while next_link := next((x for x in resp["links"] if x["rel"] == "next"), None):
            r = session.post(next_link["href"], json=next_link["body"], timeout=timeout)
            resp = r.json()
            results.extend(resp["features"])

    return results


def main(hulls: pd.Series):
    return gevent.wait([gevent.spawn(sync_query, hull) for hull in hulls])


if __name__ == "__main__":
    hulls = pd.read_json("hulls.json", orient="records", typ="series")[:20]
    results = main(hulls)
    print(len(results))
    print(sum(len(x.value) for x in results))
