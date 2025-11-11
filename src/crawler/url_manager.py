thonimport re
from dataclasses import dataclass, field
from typing import Iterable, List, Set
from urllib.parse import urlparse, urlunparse

from utils.logger import get_logger

logger = get_logger(__name__)

def _normalize_scheme(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    if not parsed.netloc and parsed.path:
        # e.g. example.com -> netloc
        netloc = parsed.path
        path = ""
    else:
        netloc = parsed.netloc
        path = parsed.path
    normalized = urlunparse(
        (scheme, netloc, path or "/", parsed.params, parsed.query, parsed.fragment)
    )
    return normalized

def _strip_www(host: str) -> str:
    return host[4:] if host.lower().startswith("www.") else host

@dataclass
class UrlManager:
    raw_urls: Iterable[str]
    urls: List[str] = field(init=False)

    def __post_init__(self) -> None:
        self.urls = self._prepare_urls(self.raw_urls)

    @staticmethod
    def _prepare_urls(raw_urls: Iterable[str]) -> List[str]:
        seen: Set[str] = set()
        cleaned: List[str] = []

        for line in raw_urls:
            url = line.strip()
            if not url:
                continue

            # Basic URL-like validation
            if not re.search(r"\.", url):
                logger.debug(f"Skipping non-URL string: {url}")
                continue

            normalized = _normalize_scheme(url)
            parsed = urlparse(normalized)

            host_key = _strip_www(parsed.netloc.lower())
            path_key = (parsed.path or "/").rstrip("/")

            key = f"{host_key}{path_key}"
            if key in seen:
                logger.debug(f"Skipping duplicate URL: {normalized}")
                continue

            seen.add(key)
            cleaned.append(normalized)

        logger.info(f"Prepared {len(cleaned)} unique URLs from input")
        return cleaned

    @classmethod
    def from_file(cls, path: str) -> "UrlManager":
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        logger.info(f"Loaded {len(lines)} lines from {path}")
        return cls(lines)