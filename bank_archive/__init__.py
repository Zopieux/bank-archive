from collections import defaultdict
from decimal import Decimal
from typing import NamedTuple, List, Tuple

import fitz


class PageRect(NamedTuple):
    page: fitz.Page
    rect: fitz.Rect
    columns: List[float]
    account: str


class StatementRow(NamedTuple):
    date: Tuple[int, int]
    description: str
    value: Decimal


class MalformedError(ValueError):
    pass


class Downloader:
    pass


class Extractor:
    @staticmethod
    def find_words_rect(page, *words):
        found_words = defaultdict(list)
        for (a, b, c, d, word, x, y, z) in page.getText("words"):
            found_words[word].append(fitz.Rect(a, b, c, d))
        return zip(*(found_words[w] for w in words))
