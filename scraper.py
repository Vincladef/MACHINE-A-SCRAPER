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
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin, urlparse
from datetime import datetime

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
APPEND_MODE = os.getenv("APPEND_MODE", "false").lower() in ("1", "true", "yes")
CSE_LR = os.getenv("CSE_LR", "lang_fr")
CSE_GL = os.getenv("CSE_GL", "fr")
CSE_CR = os.getenv("CSE_CR", "countryFR")
ALLOW_TLDS = {t for t in os.getenv("ALLOW_TLDS", "fr").lower().split(",") if t}
SKIP_SUBS = {s.strip().lower() for s in os.getenv("SKIP_SUBS", "blog.,docs.,help.,support.").split(",") if s}
MAX_ITERATIONS = int(os.getenv("MAX_ITERATIONS", "3"))
TARGET_EMAIL_COUNT = int(os.getenv("TARGET_EMAIL_COUNT", "100"))
EXTRA_PATHS = [
    "/contact",
    "/contact-us",
    "/contacts",
    "/about",
    "/a-propos",
    "/mentions-legales",
]

# Filtres anti-faux positifs et domaines problématiques
BAD_TLDS = {"png", "jpg", "jpeg", "gif", "webp", "svg", "css", "js", "ico", "pdf", "xml", "json"}
BLOCKED_DOMAINS = {
    "reddit.com",
    "upwork.com",
    "salesforce.com",
    "tealhq.com",
    "facebook.com",
    "linkedin.com",
    "instagram.com",
    "x.com",
    "youtube.com",
}
EXTRA_QUERY = os.getenv("EXTRA_QUERY", "").strip()
EXCLUDE_TERMS = [term for term in os.getenv("EXCLUDE_TERMS", "").split() if term]
PERPLEXITY_API_URL = os.getenv("PERPLEXITY_API_URL", "https://api.perplexity.ai/chat/completions")
PERPLEXITY_MODEL = os.getenv("PERPLEXITY_MODEL", "llama-3.1-sonar-large-128k-chat")
PERPLEXITY_TIMEOUT = int(os.getenv("PERPLEXITY_TIMEOUT", "30"))
PERPLEXITY_MAX_SITES = int(os.getenv("PERPLEXITY_MAX_SITES", "40"))

SOURCE_LABELS = {
    "perplexity": "Perplexity",
    "cse": "Google CSE",
    "input": "URL fournie",
}


def is_probable_email(addr: str) -> bool:
    if len(addr) > 254:
        return False
    local, _, domain = addr.partition("@");
    if not local or not domain or len(local) > 64:
        return False
    parts = domain.lower().split(".")
    if len(parts) < 2:
        return False
    tld = parts[-1]
    if tld in BAD_TLDS:
        return False
    return True


def build_query(base: str) -> str:
    components = [base]
    if EXTRA_QUERY:
        components.append(EXTRA_QUERY)
    query = " ".join(components).strip()
    if EXCLUDE_TERMS:
        query += " " + " ".join(f"-{term}" for term in EXCLUDE_TERMS)
    return query


def dedupe_preserve_order(items: List[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def is_url(value: str) -> bool:
    lowered = value.lower()
    return lowered.startswith("http://") or lowered.startswith("https://")


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

    print(
        "Feuille cible: "
        f"https://docs.google.com/spreadsheets/d/{sheet_id} "
        f"(titre: {spreadsheet.title})"
    )

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
            "q": build_query(query),
            "num": batch_size,
            "start": start,
        }
        if CSE_LR:
            params["lr"] = CSE_LR
        if CSE_GL:
            params["gl"] = CSE_GL
        if CSE_CR:
            params["cr"] = CSE_CR

        try:
            response = requests.get(CSE_API_URL, params=params, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
        except requests.HTTPError as exc:  # type: ignore[attr-defined]
            status_code = getattr(exc.response, "status_code", "HTTP")
            detail = str(exc)
            try:
                detail = exc.response.json().get("error", {}).get("message") or detail
            except Exception:
                pass
            return links, f"Erreur CSE {status_code}: {detail}"
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
    """Retourne l'ensemble des emails probables trouvés dans le HTML."""

    emails: Set[str] = set()

    # 1) Regex directe
    for candidate in EMAIL_REGEX.findall(html):
        if is_probable_email(candidate):
            emails.add(candidate)

    # 2) Liens mailto
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a"):
        href = anchor.get("href") or ""
        if href.startswith("mailto:"):
            candidate = href.replace("mailto:", "").split("?")[0].strip()
            if candidate and re.fullmatch(EMAIL_REGEX, candidate) and is_probable_email(candidate):
                emails.add(candidate)

    return emails


def fetch_sites_from_perplexity(
    query: str,
    max_sites: int,
    exclude: Set[str] | None = None,
) -> Tuple[List[str], str]:
    api_key = os.getenv("PERPLEXITY_API_KEY")
    if not api_key:
        return [], "PERPLEXITY_API_KEY manquante"

    exclude = exclude or set()
    avoid_clause = (
        "\nÉvite absolument ces URL déjà vues: " + ", ".join(sorted(exclude))
        if exclude
        else ""
    )

    payload = {
        "model": PERPLEXITY_MODEL,
        "temperature": 0,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Tu aides à identifier des sites web pertinents. "
                    "Réponds STRICTEMENT en JSON selon ce schéma: "
                    "{\"sites\":[{\"url\":\"https://...\",\"notes\":\"...\"}, ...]}."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Fournis jusqu'à {max_sites} sites web actifs et pertinents en France pour: '{query}'. "
                    "Priorise les domaines francophones/ou français. Pas de doublons." + avoid_clause
                ),
            },
        ],
        "max_output_tokens": 500,
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            PERPLEXITY_API_URL,
            json=payload,
            headers=headers,
            timeout=PERPLEXITY_TIMEOUT,
        )
        response.raise_for_status()
    except RequestException as exc:
        return [], f"Erreur Perplexity: {exc}"

    try:
        data = response.json()
    except ValueError as exc:
        return [], f"Réponse Perplexity invalide: {exc}"

    choices = data.get("choices", []) or []
    if not choices:
        return [], "Réponse Perplexity vide"

    content = choices[0].get("message", {}).get("content", "")
    urls: List[str] = []

    if content:
        try:
            parsed = json.loads(content)
            entries = []
            if isinstance(parsed, dict):
                entries = parsed.get("sites", []) or parsed.get("urls", [])
            elif isinstance(parsed, list):
                entries = parsed

            for entry in entries:
                if isinstance(entry, dict):
                    url = entry.get("url") or entry.get("href") or entry.get("lien")
                else:
                    url = entry
                if not url:
                    continue
                url = str(url).strip()
                if not url.lower().startswith("http"):
                    continue
                urls.append(url)
        except json.JSONDecodeError:
            urls = [
                match.strip().rstrip(".,);]")
                for match in re.findall(r"https?://[^\s\]\)\"'>]+", content)
            ]

    unique_urls = dedupe_preserve_order(urls)[:max_sites]
    if not unique_urls:
        return [], "Aucun site exploitable via Perplexity"

    return unique_urls, ""


def generate_candidate_links(
    query: str,
    api_key: str,
    cx_id: str,
    max_results: int,
    exclude: Set[str] | None = None,
) -> Tuple[List[Tuple[str, str]], str]:
    cleaned = query.strip()
    if not cleaned:
        return [], "Cible vide"

    links: List[Tuple[str, str]] = []
    errors: List[str] = []

    if is_url(cleaned):
        return [(cleaned, "input")], ""

    already_seen = exclude or set()

    llm_urls, llm_error = fetch_sites_from_perplexity(
        cleaned,
        PERPLEXITY_MAX_SITES,
        already_seen,
    )
    if llm_urls:
        for url in llm_urls:
            if url in already_seen:
                continue
            already_seen.add(url)
            links.append((url, "perplexity"))
    elif llm_error:
        errors.append(llm_error)

    if not links:
        cse_urls, cse_error = fetch_results_paginated(cleaned, api_key, cx_id, max_results)
        if cse_urls:
            for url in cse_urls:
                if url in already_seen:
                    continue
                already_seen.add(url)
                links.append((url, "cse"))
        elif cse_error:
            errors.append(cse_error)

    deduped: List[Tuple[str, str]] = []
    seen_links: Set[str] = set()
    for url, origin in links:
        if url not in seen_links:
            seen_links.add(url)
            deduped.append((url, origin))

    return deduped, "; ".join(errors)


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
        print(f"Cibles lues: {len(targets)}")

        run_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        header = ["Cible", "Site Internet", "Mails"]

        site_records: Dict[str, dict] = {}
        collected_emails: Set[str] = set()
        processed_links: Set[str] = set()

        for iteration in range(1, MAX_ITERATIONS + 1):
            if len(collected_emails) >= TARGET_EMAIL_COUNT:
                break
            print(f"Iteration {iteration}/{MAX_ITERATIONS} - emails collectés: {len(collected_emails)}")

            for query in targets:
                if len(collected_emails) >= TARGET_EMAIL_COUNT:
                    break

                candidates, source_error = generate_candidate_links(
                    query,
                    api_key,
                    cx_id,
                    MAX_RESULTS,
                    processed_links,
                )
                if source_error:
                    print(f"Info - {query}: {source_error}")

                if not candidates:
                    continue

                for link, origin in candidates:
                    if len(collected_emails) >= TARGET_EMAIL_COUNT:
                        break

                    if link in processed_links:
                        continue
                    processed_links.add(link)

                    host = (urlparse(link).hostname or "").lower()
                    tld = host.split(".")[-1] if host else ""
                    if ALLOW_TLDS and tld and tld not in ALLOW_TLDS:
                        continue
                    if SKIP_SUBS and any(host.startswith(prefix) for prefix in SKIP_SUBS):
                        continue
                    if any(host.endswith(d) for d in BLOCKED_DOMAINS):
                        continue

                    record = site_records.setdefault(
                        link,
                        {
                            "queries": set(),
                            "sources": set(),
                            "emails": set(),
                        },
                    )
                    record["queries"].add(query)
                    record["sources"].add(SOURCE_LABELS.get(origin, origin))

                    response, http_error = fetch_page(link)
                    emails_found: Set[str] = set()

                    if response is not None:
                        emails_found |= extract_emails_from_html(response.text)

                        if DEEP_SCRAPE:
                            base_url = response.url or link
                            for path in EXTRA_PATHS:
                                extra_url = urljoin(base_url, path)
                                extra_response, _ = fetch_page(extra_url)
                                if extra_response is not None:
                                    emails_found |= extract_emails_from_html(extra_response.text)
                                    time.sleep(REQUEST_DELAY)

                            try:
                                soup = BeautifulSoup(response.text, "html.parser")
                                keywords = ("contact", "about", "à propos", "a propos", "support", "legal", "mentions", "impressum")
                                discovered = set()
                                for a in soup.find_all("a"):
                                    text = (a.get_text() or "").lower().strip()
                                    href = a.get("href") or ""
                                    if href and any(k in text for k in keywords):
                                        discovered.add(urljoin(base_url, href))

                                for extra_url in list(discovered)[:10]:
                                    r2, _ = fetch_page(extra_url)
                                    if r2 is not None:
                                        emails_found |= extract_emails_from_html(r2.text)
                                        time.sleep(REQUEST_DELAY)
                            except Exception:
                                pass

                    if http_error and not emails_found:
                        continue

                    for email in sorted(emails_found):
                        if not is_probable_email(email):
                            continue
                        normalized = email.lower()
                        if normalized in collected_emails:
                            continue
                        collected_emails.add(normalized)
                        record["emails"].add(email)
                        if len(collected_emails) >= TARGET_EMAIL_COUNT:
                            break

                    time.sleep(REQUEST_DELAY)

        rows: List[List[str]] = []
        for link, data in site_records.items():
            if not data["emails"]:
                continue
            queries_str = "; ".join(sorted(data["queries"]))
            sources_str = ", ".join(sorted(data["sources"]))
            emails_str = "; ".join(sorted(data["emails"]))
            rows.append([queries_str, link, f"{sources_str} | {emails_str}"])

        rows.sort(key=lambda row: (row[0], row[1]))

        output: List[List[str]] = [[f"Run {run_timestamp}", "", ""], header]
        output.extend(rows)

        if APPEND_MODE:
            existing = ws_out.get_all_values()
            start_row = len(existing) + 1 if existing else 1
            ws_out.update(range_name=f"A{start_row}", values=output)
            print(f"Écriture OK (append run): +{len(output) - 2} lignes utiles.")
        else:
            ws_out.clear()
            ws_out.update(range_name="A1", values=output)
            print(f"Écriture OK (replace): {len(output) - 2} lignes utiles.")

        if len(collected_emails) < TARGET_EMAIL_COUNT:
            print(
                f"Objectif non atteint: {len(collected_emails)} emails collectés sur {TARGET_EMAIL_COUNT}."
            )

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

