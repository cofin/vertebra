"""Text utilities for DMA."""

import re
import unicodedata


def slugify(text: str) -> str:
    """Convert a string to a URL-friendly slug.

    Args:
        text: The text to slugify

    Returns:
        A slugified version of the text
    """
    if not text:
        return ""

    # Convert to lowercase and normalize unicode
    text = unicodedata.normalize("NFKD", text.lower())

    # Remove non-ascii characters
    text = text.encode("ascii", "ignore").decode("ascii")

    # Replace spaces and other non-word characters with hyphens
    text = re.sub(r"[^a-z0-9]+", "-", text)

    # Remove leading/trailing hyphens
    return text.strip("-")
