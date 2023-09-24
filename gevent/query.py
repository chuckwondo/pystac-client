from typing import Any, Iterable, Mapping, Sequence, TypeAlias, TypedDict

import pandas as pd
import requests

import pystac_client
from pystac_client.item_search import IntersectsLike

Feature: TypeAlias = Mapping[str, Any]


class Link(TypedDict, total=True):
    href: str
    body: Mapping[str, Any]


def fetch_features(
    link: Link,
    session: requests.Session,
    **post_kwargs: Any,
) -> Iterable[Feature]:
    next_link: Link | None = link

    while next_link:
        r = session.post(next_link["href"], json=next_link["body"], **post_kwargs)
        payload = r.json()
        yield from payload["features"]
        links = payload["links"]
        next_link = next((link for link in links if link["rel"] == "next"), None)


def query(
    intersects: IntersectsLike,
) -> Sequence[Feature]:
    catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1"
    )
    search = catalog.search(
        collections=["sentinel-2-l2a"],
        intersects=intersects,
        datetime=["2018-01-01", "2019-12-31"],
        query={"eo:cloud_cover": {"lt": 10}},
        limit=500,
    )

    with requests.Session() as session:
        link: Link = {"href": search.url, "body": search.get_parameters()}
        return tuple(fetch_features(link, session, timeout=(20, 120)))


def main(n: int) -> None:
    hulls = pd.read_json("hulls.json", orient="records", typ="series")[:n]
    results = [query(hull) for hull in hulls]
    features = [feature for features in results for feature in features]
    print(len(features))


if __name__ == "__main__":
    import sys

    n = 10 if len(sys.argv) == 1 else int(sys.argv[1])
    main(n)
