thonfrom datetime import datetime, timezone
from typing import Dict, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from langdetect import DetectorFactory, LangDetectException, detect

from crawler.text_cleaner import clean_main_content
from utils.logger import get_logger

logger = get_logger(__name__)

# Make language detection deterministic
DetectorFactory.seed = 42

def _extract_title(soup: BeautifulSoup) -> str:
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    return ""

def _extract_meta_description(soup: BeautifulSoup) -> str:
    tag = soup.find("meta", attrs={"name": "description"})
    if tag and tag.get("content"):
        return tag["content"].strip()
    og_desc = soup.find("meta", attrs={"property": "og:description"})
    if og_desc and og_desc.get("content"):
        return og_desc["content"].strip()
    return ""

def _extract_headings(soup: BeautifulSoup) -> List[str]:
    headings: List[str] = []
    for level in ["h1", "h2", "h3"]:
        for tag in soup.find_all(level):
            text = tag.get_text(" ", strip=True)
            if text:
                headings.append(text)
    return headings

def _extract_main_text(soup: BeautifulSoup) -> str:
    # Remove obvious non-content tags
    for tag_name in [
        "script",
        "style",
        "noscript",
        "header",
        "footer",
        "nav",
        "aside",
        "form",
        "svg",
    ]:
        for t in soup.find_all(tag_name):
            t.decompose()

    body = soup.body or soup
    text = body.get_text("\n", strip=True)
    return text

def _extract_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    urls: List[str] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        absolute = urljoin(base_url, href)
        if absolute not in seen:
            seen.add(absolute)
            urls.append(absolute)
    return urls

def _detect_language(text: str) -> str:
    if not text:
        return ""
    try:
        lang = detect(text)
        return lang
    except LangDetectException:
        logger.debug("Language detection failed; returning empty language code")
        return ""

def parse_html(url: str, html: str) -> Dict:
    """
    Parse a single HTML document into the structured format described in the README.
    """
    soup = BeautifulSoup(html, "lxml")

    title = _extract_title(soup)
    meta_description = _extract_meta_description(soup)
    headings = _extract_headings(soup)
    raw_main_text = _extract_main_text(soup)
    main_content = clean_main_content(raw_main_text)
    links = _extract_links(soup, base_url=url)
    language = _detect_language(main_content)
    word_count = len(main_content.split()) if main_content else 0
    crawl_timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "url": url,
        "title": title,
        "metaDescription": meta_description,
        "headings": headings,
        "mainContent": main_content,
        "links": links,
        "language": language,
        "wordCount": word_count,
        "crawlTimestamp": crawl_timestamp,
    }