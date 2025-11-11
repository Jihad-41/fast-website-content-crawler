thonimport argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from crawler.html_parser import parse_html
from crawler.url_manager import UrlManager
from utils.concurrency import fetch_all
from utils.logger import get_logger

logger = get_logger(__name__)

def load_settings(path: str) -> Dict[str, Any]:
    if not path:
        return {}
    config_path = Path(path)
    if not config_path.exists():
        logger.warning(f"Config file not found at {config_path}, using defaults")
        return {}
    try:
        with config_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded settings from {config_path}")
        return data
    except Exception as exc:
        logger.warning(f"Failed to read settings from {config_path}: {exc}")
        return {}

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fast Website Content Crawler - Bitbash"
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        required=True,
        help="Path to a text file containing one URL per line",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        required=True,
        help="Path to the JSON file where results will be written",
    )
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="",
        help="Optional path to settings JSON (see src/config/settings.example.json)",
    )
    return parser.parse_args()

async def crawl(
    url_file: str,
    output_file: str,
    settings: Dict[str, Any],
) -> None:
    url_manager = UrlManager.from_file(url_file)
    urls = url_manager.urls

    if not urls:
        logger.error("No valid URLs found in input file")
        return

    concurrency = int(settings.get("concurrency", 20))
    timeout = int(settings.get("requestTimeoutSeconds", 15))
    user_agent = settings.get(
        "userAgent",
        "FastWebsiteContentCrawler/1.0",
    )

    headers = {"User-Agent": user_agent}

    logger.info(
        f"Starting crawl for {len(urls)} URLs | "
        f"concurrency={concurrency}, timeout={timeout}s"
    )

    fetch_results = await fetch_all(
        urls=urls,
        concurrency=concurrency,
        timeout=timeout,
        headers=headers,
    )

    parsed_results: List[Dict[str, Any]] = []
    failures = 0

    for result in fetch_results:
        if result.html is None:
            failures += 1
            logger.debug(
                f"Skipping parse for {result.url} due to fetch error: {result.error}"
            )
            continue

        try:
            parsed = parse_html(result.url, result.html)
            parsed_results.append(parsed)
        except Exception as exc:
            failures += 1
            logger.warning(f"Failed to parse HTML for {result.url}: {exc}")

    pretty = bool(settings.get("outputPrettyPrint", True))
    indent = 2 if pretty else None

    out_path = Path(output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(parsed_results, f, ensure_ascii=False, indent=indent)

    logger.info(
        f"Crawl finished. Success: {len(parsed_results)}, "
        f"Failed: {failures}. Output written to {out_path}"
    )

def main() -> None:
    args = parse_args()
    settings = load_settings(args.config)
    try:
        asyncio.run(crawl(args.input, args.output, settings))
    except KeyboardInterrupt:
        logger.warning("Crawl interrupted by user")
    except Exception as exc:
        logger.error(f"Unexpected error during crawl: {exc}")

if __name__ == "__main__":