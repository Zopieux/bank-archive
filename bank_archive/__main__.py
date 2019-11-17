import io
import json
import multiprocessing
from typing import Iterable

import fitz
import pandas
import tabula

from bank_archive import PageRect
from bank_archive.cde import CaisseEpargneExtractor


def extract_tables(cls, fobj, page_rect: Iterable[PageRect]):
    data = [
        {
            "page": pr.page.number + 1,
            "extraction_method": "guess",
            "columns": ",".join(map(str, pr.columns)),
            "x1": pr.rect.x0,
            "x2": pr.rect.x1,
            "y1": pr.rect.y0,
            "y2": pr.rect.y1,
        }
        for pr in page_rect
        if pr is not None
    ]
    template = io.StringIO(json.dumps(data))
    tables = map(cls.fix_table, tabula.read_pdf_with_template(fobj, template))

    current = None
    for page in page_rect:
        if page is None:
            # Break. Yield the concatenated so far.
            if current is not None:
                yield current
            current = None
        else:
            # Continuation. Concat to current.
            try:
                latest = next(tables)
                if current is None:
                    current = page.account, latest
                else:
                    current = page.account, current[1].append(latest, ignore_index=True)
            except StopIteration:
                break

    if current is not None:
        yield current


def extract_doc(fobj):
    doc = fitz.open(fobj)
    print(doc.name)

    cls = CaisseEpargneExtractor

    starts = cls.iter_starts(doc)
    ends = cls.iter_ends(doc)

    page_rect = []
    for (spage, account, start) in starts:
        while True:
            epage, is_break, end = next(ends)
            if spage.number == epage.number:
                break

        area = cls.search_area(start, end)
        page_rect.append(PageRect(spage, area, cls.columns_x(start, end), account))
        if is_break:
            page_rect.append(None)

    tables = list(extract_tables(cls, fobj, page_rect))

    all_dfs = []
    for account, raw_table in tables:
        df = cls.extract_rows(raw_table)
        df.iloc[:, 0] = df.iloc[:, 0].apply(lambda dm: cls.parse_date(doc, dm))
        # Insert account column at the start (same value for all rows, obviously).
        df.insert(0, "Account", df.iloc[:, 0].apply(lambda x: account))
        all_dfs.append(df)

    return pandas.concat(all_dfs, ignore_index=True)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("file", nargs="+", type=argparse.FileType("rb"))
    args = p.parse_args()

    files = [f.name for f in args.file]

    pool = multiprocessing.Pool()
    all_data_per_doc = list(pool.imap(extract_doc, files))
    pool.terminate()
    pool.join()

    all_data = pandas.concat(all_data_per_doc, ignore_index=True)

    all_data.to_csv("data/df/bank.csv", mode="w")
    all_data.to_html("data/df/bank.html")
