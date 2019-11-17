import datetime
import re
import time
from decimal import Decimal
from functools import reduce
from typing import Iterable

import fitz
import pandas
import requests
from lxml import html
from requests.adapters import HTTPAdapter
from requests.cookies import cookiejar_from_dict

from bank_archive import Extractor, Downloader, StatementRow, MalformedError

REGEXP_WEBFORM = re.compile(
    r"""WebForm_PostBackOptions\s*\(\s*["'](.*?)["'],\s*["'](.*?)["']"""
)
REGEXP_DISPOSITION_FILENAME = re.compile('filename="(.*?)"')
REGEXP_ACCOUNT_NUM = re.compile(r"N°([\s0-9a-z]+)")


class CaisseEpargneExtractor(Extractor):
    COLUMNS = ["date", "description", "debit", "credit"]

    @classmethod
    def parse_date(cls, doc: fitz.Document, date: str):
        st_year, st_month = map(
            int,
            re.search(r"RELEVES_.+?_([0-9]{4})([0-9]{2})[0-9]{2}", doc.name).groups(),
        )
        day, month = map(int, date.split("/", 1))
        if st_month == 1 and month == 12:
            # Fucking hell: dates in month 12 are for the previous year for the January statement.
            st_year -= 1
        return datetime.date(st_year, month, day)

    @classmethod
    def iter_starts(cls, doc: fitz.Document) -> Iterable[fitz.Rect]:
        for page in doc:
            for rects in cls.find_words_rect(page, "Date", "Détail", "Débit", "Crédit"):
                # The account name is always slightly above the table head.
                r = fitz.Rect().includeRect(rects[0]).includeRect(rects[-1])
                r.y0 -= 22
                r.y1 -= 12
                # Margin for error as we want words to be fully inside the rect.
                r.x0 -= 5
                r.x1 += 5
                account = " ".join(
                    w[4] for w in page.getText("words") if fitz.Rect(w[:4]) in r
                )
                account = REGEXP_ACCOUNT_NUM.search(account).group(1).strip()
                yield page, account, rects

    @classmethod
    def iter_ends(cls, doc: fitz.Document) -> Iterable[fitz.Rect]:
        for page in doc:
            yield from ((page, True, r) for r in page.searchFor("NOUVEAU SOLDE"))
            for pat in ("Perte ou vol", "Caisse d'Epargne et de Prévoyance"):
                rect = next(iter(page.searchFor(pat)), None)
                if rect:
                    rect.y0 -= 10
                    yield page, False, rect
                    break

    @staticmethod
    def _fix_start(start, end):
        (date, det, deb, cred) = start
        bottom = end.tl.y

        deb.x0 -= 20
        deb.x1 += 5
        cred.x0 -= 20
        cred.x1 += 5

        date.includePoint(det.bl)
        date.x0 -= 3
        date.x1 -= 5

        det.includePoint(deb.bl)
        det.x0 -= 3
        det.x1 -= 1

        date.y1 = bottom
        det.y1 = bottom
        deb.y1 = bottom
        cred.y1 = bottom

        return date, det, deb, cred

    @classmethod
    def columns_x(cls, start: fitz.Rect, end: fitz.Rect):
        date, det, deb, cred = cls._fix_start(start, end)
        return [date.tl.x, det.tl.x, deb.tl.x, cred.tl.x]

    @classmethod
    def search_area(cls, start: fitz.Rect, end: fitz.Rect):
        rects = cls._fix_start(start, end)
        merged = reduce(lambda a, b: a.includeRect(b), rects, fitz.Rect())
        return merged

    @classmethod
    def fix_table(cls, table):
        if table.shape[1] < len(cls.COLUMNS):
            raise MalformedError("table does not have enough columns")
        if table.shape[1] > len(cls.COLUMNS):
            extra = table.iloc[:, 2:-2]
            table.iloc[:, 1] = table.iloc[:, 1].str.cat(extra, sep="\n", na_rep="")
            table.drop(extra, inplace=True, axis=1)

        columns = {c: new_name for c, new_name in zip(table, cls.COLUMNS)}
        table.rename(columns=columns, inplace=True)
        return table

    @classmethod
    def extract_rows(cls, table):
        results = []
        current: StatementRow = None

        def parse_value(v):
            return Decimal(v.replace(",", ".").replace(" ", ""))

        for _, (date, descr, debit, credit) in table.iterrows():
            if pandas.isna(descr):
                continue
            if pandas.isna(date):
                # Continuation.
                if not current:
                    # Heading garbage.
                    continue
                if not pandas.isna(debit):
                    continue
                if not pandas.isna(credit):
                    continue
                description = current.description + "\n" + descr
                current = StatementRow(current.date, description, current.value)
            else:
                # Header itself.
                if date.strip().lower() == "date":
                    continue
                if current:
                    results.append(current)
                if pandas.isna(debit) and pandas.isna(credit):
                    raise MalformedError("no debit nor credit on date line")
                if pandas.isna(debit):
                    value = parse_value(credit)
                else:
                    value = -parse_value(debit)
                current = StatementRow(date, descr, value)

        if current:
            results.append(current)

        df = pandas.DataFrame(
            ((r.date, r.description, r.value) for r in results),
            columns=("Date", "Description", "Value"),
        )
        return df


class CaisseEpargneDownloader(Downloader):
    def __init__(self, cookies):
        self.s = requests.session()
        self.s.mount("https://", HTTPAdapter(pool_connections=1, pool_maxsize=1))
        self.s.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:70.0) Gecko/20100101 Firefox/70.0"
        }
        self.s.cookies = cookiejar_from_dict(cookies)
        self.last_page = None

    def check_login(self):
        assert ".ASPXFORMSAUTH" in self.s.cookies
        assert "ASP.NET_SessionId" in self.s.cookies
        assert "CEPARAL" in self.s.cookies
        r = self.s.post("https://www.net382.caisse-epargne.fr/Portail.aspx")
        r.raise_for_status()
        assert "datalayer.client.id" in r.text
        return self._set_last_page(r)

    def _set_last_page(self, response):
        self.last_page = html.fromstring(response.content)
        return self.last_page

    def _main_form(self):
        for form in self.last_page.forms:
            if form.get("id") == "main":
                return form
        raise StopIteration()

    def _navigate(
        self, pattern_or_element, field_overrides=None, update_last_page=True
    ):
        if isinstance(pattern_or_element, str):
            link = self.last_page.xpath(f"//a[{pattern_or_element}]")[0]
        else:
            link = pattern_or_element
        link_data = REGEXP_WEBFORM.search(link.get("href"))
        if not link_data:
            raise ValueError("link href does not match expected pattern")
        target, argument = link_data.groups()
        form = cls._main_form()
        fields = {
            **form.fields,
            "__EVENTTARGET": target,
            "__EVENTARGUMENT": argument,
            "m_ScriptManager": f"MM$m_UpdatePanel|{target}",
            **(field_overrides or {}),
        }
        fields = {key: value for key, value in fields.items() if value is not None}
        for i in range(5):
            try:
                r = self.s.post(
                    "https://www.net382.caisse-epargne.fr/Portail.aspx", data=fields
                )
                r.raise_for_status()
                break
            except requests.exceptions.ConnectionError:
                time.sleep(2)
        else:
            raise RuntimeError()
        if update_last_page:
            return self._set_last_page(r)
        else:
            return r

    def documents_home(self):
        self._navigate('./*[contains(text(), "e-Documents")]')
        return self._navigate('@title="Rechercher"')

    def documents_per_year_overrides(self):
        form = self._main_form()
        select = form.xpath('//select[contains(@name, "ConsultationAnnee")]')[0]
        name = select.get("name")
        years = select.value_options
        for year in years:
            yield {name: year}

    def documents_list_downloads(self, overrides):
        r = self._navigate(
            'contains(@id, "RechercherConsultation")', field_overrides=overrides
        )
        links = r.xpath('//a[contains(@href, "$LnkBtPieceJointe")]')
        yield from links

    def documents_download(self, link):
        r = self._navigate(link, update_last_page=False)
        assert "pdf" in r.headers.get("content-type", "")
        filename = REGEXP_DISPOSITION_FILENAME.search(
            r.headers["content-disposition"]
        ).group(1)
        return filename, r.content


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.DEBUG)

    cls = CaisseEpargneDownloader(
        cookies={
            ".ASPXFORMSAUTH": "COPY PASTE ME",
            "ASP.NET_SessionId": "COPY PASTE ME",
            "CEPARAL": "COPY PASTE ME",
        }
    )
    cls.check_login()
    time.sleep(1)

    cls.documents_home()
    time.sleep(1)

    for override in cls.documents_per_year_overrides():
        for link in cls.documents_list_downloads(override):
            filename, content = cls.documents_download(link)
            with open(f"data/pdf/{filename}", "wb") as f:
                f.write(content)
            time.sleep(1)
