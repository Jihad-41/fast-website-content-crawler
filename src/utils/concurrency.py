thonimport asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional

import aiohttp

from utils.logger import get_logger

logger = get_logger(__name__)

@dataclass
class FetchResult:
    url: str
    status: Optional[int]
    html: Optional[str]
    error: Optional[str]

async def _fetch_single(
    session: aiohttp.ClientSession,
    url: str,
    semaphore: asyncio.Semaphore,
    timeout: int,
) -> FetchResult:
    async with semaphore:
        try:
            logger.debug(f"Fetching URL: {url}")
            async with session.get(url, timeout=timeout) as resp:
                text = await resp.text(errors="ignore")
                logger.debug(f"Fetched {url} with status {resp.status}")
                return FetchResult(
                    url=url,
                    status=resp.status,
                    html=text if resp.status == 200 else None,
                    error=None if resp.status == 200 else f"HTTP {resp.status}",
                )
        except asyncio.TimeoutError:
            msg = "Request timed out"
            logger.warning(f"{msg} for URL: {url}")
            return FetchResult(url=url, status=None, html=None, error=msg)
        except Exception as exc:
            msg = f"Request failed: {exc}"
            logger.warning(f"{msg} for URL: {url}")
            return FetchResult(url=url, status=None, html=None, error=msg)

async def fetch_all(
    urls: List[str],
    concurrency: int = 10,
    timeout: int = 15,
    headers: Optional[Dict[str, str]] = None,
) -> List[FetchResult]:
    """
    Fetch multiple URLs concurrently using aiohttp.

    Returns a list of FetchResult objects with HTML or error info.
    """
    if not urls:
        return []

    connector = aiohttp.TCPConnector(limit_per_host=concurrency)
    semaphore = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(headers=headers or {}, connector=connector) as session:
        tasks = [
            asyncio.create_task(_fetch_single(session, url, semaphore, timeout))
            for url in urls
        ]
        logger.info(f"Starting fetch for {len(urls)} URLs with concurrency={concurrency}")
        results = await asyncio.gather(*tasks)
        logger.info("Completed fetching all URLs")
        return results