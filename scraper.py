"""Script de scraping sécurisé pour récupération de sites et emails d'entreprises.

Ce module est conçu pour un usage automatisé (ex. GitHub Actions). Il lit les noms
d'entreprises depuis une feuille Google Sheets, récupère leur site web via l'API
Google Custom Search, extrait les emails trouvés sur la page et écrit le résultat
dans une seconde feuille. Toutes les informations sensibles proviennent des
variables d'environnement.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import List, Set, Tuple
from urllib.parse import urljoin

import gspread
import requests
from bs4 import BeautifulSoup
from requests import Response
from requests.exceptions import RequestException

CSE_API_URL = "https://www.googleapis.com/customsearch/v1"
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
CREDS_FILENAME = "sheets_creds.json"
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "100"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "10"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.0"))
DEEP_SCRAPE = os.getenv("DEEP_SCRAPE", "false").lower() in ("1", "true", "yes")
EXTRA_PATHS = [
    "/contact",
    "/contact-us",
    "/contacts",
    "/about",
    "/a-propos",
    "/mentions-legales",
]


class ScraperError(RuntimeError):
    """Exception générique pour les erreurs contrôlées du scraper."""


def get_env_var(name: str) -> str:
    """Retourne la valeur d'une variable d'environnement ou lève une erreur explicite."""

    value = os.getenv(name)
    if not value:
        raise ScraperError(f"Variable d'environnement manquante: {name}")
    return value


def write_credentials_file(path: Path) -> None:
    """Écrit le JSON des credentials Google dans un fichier temporaire."""

    raw_credentials = get_env_var("GOOGLE_CREDENTIALS")
    try:
        data = json.loads(raw_credentials)
    except json.JSONDecodeError as exc:
        raise ScraperError("Le contenu de GOOGLE_CREDENTIALS n'est pas un JSON valide") from exc

    # Ré-écriture pour garantir un JSON propre.
    path.write_text(json.dumps(data), encoding="utf-8")


def connect_worksheets(creds_path: Path) -> Tuple[gspread.Worksheet, gspread.Worksheet]:
    """Retourne les worksheets de lecture et écriture."""

    gc = gspread.service_account(filename=str(creds_path))
    sheet_id = get_env_var("GOOGLE_SHEET_ID")
    spreadsheet = gc.open_by_key(sheet_id)

    ws_in = spreadsheet.worksheet("Feuille 1")
    try:
        ws_out = spreadsheet.worksheet("Feuille 2")
    except gspread.WorksheetNotFound:
        ws_out = spreadsheet.add_worksheet(title="Feuille 2", rows=1000, cols=3)

    return ws_in, ws_out


def read_targets(ws_in: gspread.Worksheet) -> List[str]:
    """Récupère la première colonne de la feuille d'entrée en ignorant l'en-tête."""

    column_values = ws_in.col_values(1)
    targets = [cell.strip() for cell in column_values[1:] if cell and cell.strip()]
    return targets


def fetch_results_paginated(
    query: str,
    api_key: str,
    cx_id: str,
    max_results: int = MAX_RESULTS,
) -> Tuple[List[str], str]:
    """Retourne jusqu'à `max_results` URLs via l'API Google Custom Search (pagination)."""

    try:
        target = max(1, min(int(max_results), 100))
    except Exception:
        target = 10

    links: List[str] = []
    start = 1

    while len(links) < target and start <= 100:
        remaining = target - len(links)
        batch_size = min(10, remaining)
        params = {
            "key": api_key,
            "cx": cx_id,
            "q": query,
            "num": batch_size,
            "start": start,
        }

        try:
            response = requests.get(CSE_API_URL, params=params, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
        except RequestException as exc:
            return links, f"Erreur CSE: {exc}"

        try:
            payload = response.json()
        except ValueError as exc:
            return links, f"Réponse CSE invalide: {exc}"

        items = payload.get("items", []) or []
        if not items:
            break

        batch_links = [item.get("link") for item in items if item.get("link")]
        links.extend(batch_links)

        start += batch_size
        time.sleep(REQUEST_DELAY)

    if not links:
        return [], "Aucun résultat CSE"

    seen = set()
    unique_links: List[str] = []
    for url in links:
        if url not in seen:
            seen.add(url)
            unique_links.append(url)

    return unique_links, ""


def extract_emails_from_html(html: str) -> Set[str]:
    """Retourne l'ensemble des emails trouvés dans le HTML (texte et liens mailto)."""

    emails = set(EMAIL_REGEX.findall(html))

    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a"):
        href = anchor.get("href")
        if not href:
            continue
        if href.startswith("mailto:"):
            candidate = href[7:].split("?")[0].strip()
            if candidate and EMAIL_REGEX.fullmatch(candidate):
                emails.add(candidate)

    return emails


def fetch_page(url: str) -> Tuple[Response | None, str]:
    """Télécharge la page web cible et retourne la réponse."""

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; EmailScraper/1.0; +https://example.com)",
    }

    try:
        response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return response, ""
    except RequestException as exc:
        return None, f"Erreur HTTP: {exc}"

def main() -> int:
    try:
        api_key = get_env_var("CSE_API_KEY")
        cx_id = get_env_var("CSE_CX_ID")

        creds_path = Path(CREDS_FILENAME)
        write_credentials_file(creds_path)

        ws_in, ws_out = connect_worksheets(creds_path)
        targets = read_targets(ws_in)

        results: List[List[str]] = [["Cible", "Site Internet", "Mails"]]

        for query in targets:
            links, search_error = fetch_results_paginated(query, api_key, cx_id, MAX_RESULTS)
            if search_error and not links:
                results.append([query, "", f"Recherche: {search_error}"])
                time.sleep(REQUEST_DELAY)
                continue

            for link in links:
                response, http_error = fetch_page(link)
                emails: Set[str] = set()

                if response is not None:
                    emails |= extract_emails_from_html(response.text)

                    if DEEP_SCRAPE:
                        base_url = response.url or link
                        for path in EXTRA_PATHS:
                            extra_url = urljoin(base_url, path)
                            extra_response, _ = fetch_page(extra_url)
                            if extra_response is not None:
                                emails |= extract_emails_from_html(extra_response.text)
                                time.sleep(REQUEST_DELAY)

                if http_error and not emails:
                    results.append([query, link, f"Scraping: {http_error}"])
                else:
                    emails_str = "; ".join(sorted(emails)) if emails else "Aucun email détecté"
                    results.append([query, link, emails_str])

                time.sleep(REQUEST_DELAY)

        ws_out.clear()
        ws_out.update("A1", results)

        return 0

    except ScraperError as exc:
        print(f"Erreur de configuration: {exc}", file=sys.stderr)
        return 1

    except Exception as exc:  # pylint: disable=broad-except
        print(f"Erreur inattendue: {exc}", file=sys.stderr)
        return 1

    finally:
        try:
            creds_file = Path(CREDS_FILENAME)
            if creds_file.exists():
                creds_file.unlink()
        except OSError:
            # On ne souhaite pas faire échouer le script pour une erreur de nettoyage.
            pass


if __name__ == "__main__":
    sys.exit(main())

