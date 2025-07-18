import requests
import pandas as pd
import asyncio
import aiohttp
from curl_cffi.requests import Session
from crawler.bhx.token_interceptor import BHXTokenInterceptor
from crawler.bhx.token_interceptor import get_headers

FULL_API_URL = "https://apibhx.tgdd.vn/Location/V2/GetFull"

async def fetch_full_location_data(token: str, deviceid: str) -> dict:
    headers = get_headers(token, deviceid)

    async with aiohttp.ClientSession(headers=headers) as sess:
        async with sess.get(FULL_API_URL, timeout=15) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()

    return data.get("data", {})
