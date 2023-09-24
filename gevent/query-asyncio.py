import asyncio
from typing import Any, Mapping, Sequence, TypeAlias, TypedDict

import httpx
import pandas as pd

import pystac_client
from pystac_client.item_search import IntersectsLike

Feature: TypeAlias = Mapping[str, Any]


class Link(TypedDict, total=True):
    href: str
    body: Mapping[str, Any]


async def fetch_features(
    link: Link,
    client: httpx.AsyncClient,
    concurrency: asyncio.BoundedSemaphore,
) -> Sequence[Feature]:
    next_link: Link | None = link
    features = []

    while next_link:
        async with concurrency:
            r = await client.post(next_link["href"], json=next_link["body"])
        payload = r.json()
        features.extend(payload["features"])
        links = payload["links"]
        next_link = next((link for link in links if link["rel"] == "next"), None)

    return features


async def query(
    intersects: IntersectsLike,
    concurrency: asyncio.BoundedSemaphore,
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

    timeout = httpx.Timeout(None, connect=20, read=120)

    async with httpx.AsyncClient(timeout=timeout) as client:
        link: Link = {"href": search.url, "body": search.get_parameters()}
        return await fetch_features(link, client, concurrency)


async def main(n: int) -> None:
    hulls = pd.read_json("hulls.json", orient="records", typ="series")[:n]
    concurrency = asyncio.BoundedSemaphore(20)
    results = await asyncio.gather(*[query(hull, concurrency) for hull in hulls])
    features = [feature for features in results for feature in features]
    print(len(features))


if __name__ == "__main__":
    import sys

    n = 10 if len(sys.argv) == 1 else int(sys.argv[1])
    asyncio.run(main(n))
