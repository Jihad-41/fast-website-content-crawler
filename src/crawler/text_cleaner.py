thonimport re
from typing import Iterable

from utils.logger import get_logger

logger = get_logger(__name__)

WHITESPACE_RE = re.compile(r"\s+")
MULTILINE_RE = re.compile(r"\n{3,}")

def normalize_whitespace(text: str) -> str:
    """
    Collapse excessive whitespace while preserving paragraph breaks.
    """
    if not text:
        return ""

    # Make sure newlines are preserved then normalize spaces.
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = WHITESPACE_RE.sub(" ", text)
    # Bring back paragraph breaks where double spaces around punctuation existed.
    text = text.replace(".  ", ".\n").replace("?  ", "?\n").replace("!  ", "!\n")
    text = MULTILINE_RE.sub("\n\n", text)
    text = text.strip()
    return text

def deduplicate_lines(lines: Iterable[str]) -> str:
    """
    Remove consecutively repeated lines while keeping order.
    """
    last_line = None
    result_lines = []

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        if line == last_line:
            continue

        last_line = line
        result_lines.append(line)

    return "\n".join(result_lines)

def clean_main_content(raw_text: str) -> str:
    """
    High-level cleaning entry point for page text.
    """
    if not raw_text:
        return ""

    logger.debug("Cleaning main content text")
    # Split into rough lines, dedupe, then normalize whitespace.
    lines = raw_text.split("\n")
    deduped = deduplicate_lines(lines)
    normalized = normalize_whitespace(deduped)
    return normalized