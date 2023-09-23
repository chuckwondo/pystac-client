import asyncio
from typing import Any, Mapping, Sequence

import httpx
import pandas as pd

import pystac_client


async def query(
    intersects: Mapping[str, Any],
    max_connections: int | asyncio.Semaphore = 20,
) -> Sequence[Mapping[str, Any]]:
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

    if isinstance(max_connections, int):
        max_connections = asyncio.Semaphore(max_connections)

    timeout = httpx.Timeout(None, connect=20, read=120)
    results = []

    async with httpx.AsyncClient(timeout=timeout) as client:
        async with max_connections:
            r = await client.post(search.url, json=search.get_parameters())
        resp = r.json()
        results.extend(resp["features"])

        while next_link := next((x for x in resp["links"] if x["rel"] == "next"), None):
            async with max_connections:
                r = await client.post(next_link["href"], json=next_link["body"])
            resp = r.json()
            results.extend(resp["features"])

    return results


async def query_sequentially(hulls: pd.Series):
    return [await query(hull) for hull in hulls]


async def query_concurrently(hulls: pd.Series):
    return await asyncio.gather(*[query(hull) for hull in hulls])


async def main(query_fn, hulls: pd.Series):
    return await query_fn(hulls)


def parse_args(args: Sequence[str]):
    speeds = {"slow": query_sequentially, "fast": query_concurrently}
    if len(args) != 2 or (arg := args[1]) not in speeds:
        raise SystemExit(f"Usage: {args[0]} {{{' | '.join(speeds)}}}'")
    return speeds[arg]


if __name__ == "__main__":
    import sys

    query_fn = parse_args(sys.argv)
    hulls = pd.read_json("hulls.json", orient="records", typ="series")[:20]
    results = asyncio.run(main(query_fn, hulls))
    print(len(results))
    print(sum(len(x) for x in results))
