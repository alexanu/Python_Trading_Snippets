# Process FINRA Short Daily Data feeds
    # Source: https://github.com/amor71/FINRAShortData
    # https://developer.finra.org/docs#query_api-equity-equity_short_interest_standardized
    # FINRA Developer Credentials are required: https://developer.finra.org/create-account?Forward_URL=https://gateway.finra.org/app/dfo-console?rcpRedirNum=1
    # API  key: https://gateway.finra.org/app/api-console/add-credential

import asyncio
import concurrent.futures
import time
from typing import Optional, Tuple

import pandas as pd
import requests

url: str = "https://api.finra.org/data/group/OTCMarket/name/regShoDaily"


def _requests_get(token: str, chunk_size: int, offset: int) -> pd.DataFrame:
    r = requests.get(
        url=url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        params={"limit": chunk_size, "offset": offset},
    )
    r.raise_for_status()

    if r.status_code in (429, 502):
        print(f"{url} return {r.status_code}, waiting and re-trying")
        time.sleep(10)
        return _requests_get(token, chunk_size, offset)

    x = r.json()
    df = pd.DataFrame(x)
    df.rename(
        columns={
            "securitiesInformationProcessorSymbolIdentifier": "symbol",
            "totalParQuantity": "volume",
            "shortParQuantity": "shorts",
            "shortExemptParQuantity": "exempt",
        },
        inplace=True,
    )
    df.drop(["reportingFacilityCode", "marketCode"], axis=1, inplace=True)
    return df


def get_chunk_and_size(token: str) -> Tuple[int, int]:
    """Return the optimal chunk size and total number of data-points,
    Chunk size is used internally, by the process() function
    to reduce the number of calls to the FINRA end-point,
    it is also used as the 'offset' step when calling process() directly with restrictions.
    Input Arguments: token obtained from the auth() function.
    Returns: tuple with chunk size followed by number of data-points to be loaded from FINRA end-point.
    """
    r = requests.get(
        url=url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        params={"limit": 1},
    )
    r.raise_for_status()
    return int(r.headers["Record-Max-Limit"]), int(r.headers["Record-Total"])


async def process(
    token: str, offset: int = 0, limit: Optional[int] = None
) -> pd.DataFrame:

    chunk_size, max_records = get_chunk_and_size(token)

    if limit:
        max_records = min(max_records, limit)

    print(f"loading data (chunk_size={chunk_size}, offset={offset}, max_records={max_records-offset})...")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(executor, _requests_get, token, chunk_size, offset)
            for offset in range(offset, max_records, chunk_size)
        ]
        df = (pd.concat(await asyncio.gather(*futures)).groupby(["tradeReportDate", "symbol"]).sum())

    df["short_percent"] = round(100.0 * df.shorts / df.volume, 1)

    return df