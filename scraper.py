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
MAX_ITERATIONS = int(os.getenv("MAX_ITERATIONS", "10"))
TARGET_EMAIL_COUNT = int(os.getenv("TARGET_EMAIL_COUNT", "100"))
SCRAPER_MODE = os.getenv("SCRAPER_MODE", "scrape_emails").lower()
INPUT_SHEET_NAME = os.getenv("INPUT_SHEET_NAME", "Feuille 1")
SITES_SHEET_NAME = os.getenv("SITES_SHEET_NAME", "Feuille 2")
EMAILS_SHEET_NAME = os.getenv("EMAILS_SHEET_NAME", "Feuille 3")
TARGET_SITE_COUNT = int(os.getenv("TARGET_SITE_COUNT", "100"))
USE_CSE_FALLBACK = os.getenv("USE_CSE_FALLBACK", "false").lower() in ("1", "true", "yes")
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
# Gemini configuration (remplace Perplexity)
GEMINI_API_URL = os.getenv(
    "GEMINI_API_URL",
    "https://generativelanguage.googleapis.com/v1/models/{model}:generateContent",
)
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GEMINI_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT", "30"))
# Compat: si GEMINI_MAX_SITES non défini, on réutilise PERPLEXITY_MAX_SITES s'il existe
GEMINI_MAX_SITES = int(os.getenv("GEMINI_MAX_SITES", os.getenv("PERPLEXITY_MAX_SITES", "40")))
GEMINI_RETRIES = int(os.getenv("GEMINI_RETRIES", os.getenv("PERPLEXITY_RETRIES", "3")))
FALLBACK_TLDS = [t.strip().lower() for t in os.getenv("FALLBACK_TLDS", "com,net,org,io,co,eu").split(",") if t]

SOURCE_LABELS = {
    "gemini": "Gemini",
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


def connect_worksheets(
    creds_path: Path,
) -> Tuple[gspread.Worksheet, gspread.Worksheet, gspread.Worksheet]:
    """Retourne les worksheets de lecture, sites et emails."""

    gc = gspread.service_account(filename=str(creds_path))
    sheet_id = get_env_var("GOOGLE_SHEET_ID")
    spreadsheet = gc.open_by_key(sheet_id)

    print(
        "Feuille cible: "
        f"https://docs.google.com/spreadsheets/d/{sheet_id} "
        f"(titre: {spreadsheet.title})"
    )

    def get_or_create(name: str) -> gspread.Worksheet:
        try:
            return spreadsheet.worksheet(name)
        except gspread.WorksheetNotFound:
            return spreadsheet.add_worksheet(title=name, rows=1000, cols=3)

    ws_input = get_or_create(INPUT_SHEET_NAME)
    ws_sites = get_or_create(SITES_SHEET_NAME)
    ws_emails = get_or_create(EMAILS_SHEET_NAME)

    return ws_input, ws_sites, ws_emails


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


def fetch_sites_from_gemini(
    query: str,
    max_sites: int,
    exclude: Set[str] | None = None,
) -> Tuple[List[str], str]:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return [], "GEMINI_API_KEY manquante"

    exclude = exclude or set()
    avoid_clause = (
        "\nÉvite absolument ces URL déjà vues: " + ", ".join(sorted(exclude))
        if exclude
        else ""
    )

    # Prompt unique, sortie JSON stricte
    user_text = (
        "Pour la requête suivante, retourne UNIQUEMENT un JSON valide. "
        "Schéma exact: {\"sites\":[{\"url\":\"https://exemple.com\"}]} . "
        "Règles: uniquement des URLs http(s) de sites officiels (pas de réseaux sociaux, pas d'agrégateurs), "
        "d'abord francophones/France, pas de doublons. "
        f"Cible: '{query}'. Nombre maximum: {max_sites}. "
        + avoid_clause
    )

    def _attempt(model_name: str) -> Tuple[Response | None, str]:
        endpoint = GEMINI_API_URL.format(model=model_name)
        params = {"key": api_key}
        payload = {
            "contents": [
                {
                    "parts": [{"text": user_text}],
                }
            ],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 1024,
                "response_mime_type": "application/json",
            },
        }
        try:
            resp = requests.post(endpoint, params=params, json=payload, timeout=GEMINI_TIMEOUT)
            if resp.status_code >= 400:
                try:
                    err_json = resp.json()
                    err_msg = (
                        err_json.get("error", {}).get("message") or
                        (err_json.get("candidates", [{}])[0].get("finishReason") if isinstance(err_json.get("candidates"), list) else None)
                    )
                except Exception:
                    err_msg = None
                detail = err_msg or resp.text
                return None, f"Erreur Gemini {resp.status_code}: {detail}"
            resp.raise_for_status()
            return resp, ""
        except RequestException as exc:
            return None, f"Erreur Gemini: {exc}"

    model_candidates = [GEMINI_MODEL]
    # Ajoute la variante -001 si absente, sinon tente aussi la variante sans -001
    if GEMINI_MODEL.endswith("-001"):
        base = GEMINI_MODEL[:-4]
        if base:
            model_candidates.append(base)
    else:
        model_candidates.append(f"{GEMINI_MODEL}-001")

    # Essaye v1 puis v1beta
    endpoint_formats = [
        "https://generativelanguage.googleapis.com/v1/models/{model}:generateContent",
        "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
    ]

    response: Response | None = None
    last_err = ""
    for m in model_candidates:
        for fmt in endpoint_formats:
            # Override temporaire de l'endpoint format pour l'essai
            original = GEMINI_API_URL
            try:
                globals()["GEMINI_API_URL"] = fmt
                response, last_err = _attempt(m)
            finally:
                globals()["GEMINI_API_URL"] = original
            if response is not None:
                break
        if response is not None:
            break

    if response is None:
        return [], last_err or "Erreur Gemini inconnue"

    try:
        data = response.json()
    except ValueError as exc:
        return [], f"Réponse Gemini invalide: {exc}"

    candidates = data.get("candidates", []) or []
    if not candidates:
        return [], "Réponse Gemini vide"

    # Texte renvoyé dans candidates[0].content.parts[*].text
    text_parts = []
    try:
        content = candidates[0].get("content", {})
        parts = content.get("parts", []) or []
        for p in parts:
            t = p.get("text")
            if t:
                text_parts.append(t)
    except Exception:
        pass

    combined_text = "\n".join(text_parts).strip()
    urls: List[str] = []
    if combined_text:
        try:
            parsed = json.loads(combined_text)
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
                for match in re.findall(r"https?://[^\s\]\)\"'>]+", combined_text)
            ]

    unique_urls = dedupe_preserve_order(urls)[:max_sites]
    if not unique_urls:
        return [], "Aucun site exploitable via Gemini"

    return unique_urls, ""


def generate_candidate_links(
    query: str,
    api_key: str,
    cx_id: str,
    max_results: int,
    exclude: Set[str] | None = None,
    use_gemini: bool = True,
) -> Tuple[List[Tuple[str, str]], str]:
    cleaned = query.strip()
    if not cleaned:
        return [], "Cible vide"

    links: List[Tuple[str, str]] = []
    errors: List[str] = []

    if is_url(cleaned):
        return [(cleaned, "input")], ""

    already_seen = set(exclude or set())
    gemini_errors: List[str] = []

    if use_gemini:
        for attempt in range(GEMINI_RETRIES):
            urls, err = fetch_sites_from_gemini(
                cleaned,
                GEMINI_MAX_SITES,
                already_seen,
            )
            if not urls:
                if err:
                    gemini_errors.append(f"Essai {attempt + 1}: {err}")
                continue

            new_found = False
            for url in urls:
                if url in already_seen:
                    continue
                already_seen.add(url)
                links.append((url, "gemini"))
                new_found = True

            if not new_found:
                break

        if gemini_errors:
            errors.extend(gemini_errors)

    if not links and USE_CSE_FALLBACK:
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


def collect_sites_mode(
    targets: List[str],
    ws_sites: gspread.Worksheet,
    api_key: str,
    cx_id: str,
) -> int:
    if not targets:
        print("Aucune cible fournie dans la feuille d'entrée.")
        ws_sites.clear()
        ws_sites.update("A1", [["Run - aucun", "", ""], ["Cible", "Site Internet", "Source"]])
        return 0

    run_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    sites: Dict[str, Dict[str, Set[str]]] = {}
    processed_links: Set[str] = set()
    gemini_available = bool(os.getenv("GEMINI_API_KEY"))

    base_allowed_tlds = set(ALLOW_TLDS)
    fallback_sequence = [t for t in FALLBACK_TLDS if t and t not in base_allowed_tlds]

    for iteration in range(1, MAX_ITERATIONS + 1):
        if len(sites) >= TARGET_SITE_COUNT:
            break

        allowed_tlds = set(base_allowed_tlds)
        if fallback_sequence:
            newly_added = fallback_sequence[: max(0, iteration - 1)]
            if newly_added:
                allowed_tlds.update(newly_added)
                print(f"Itération {iteration}: TLDs autorisés -> {', '.join(sorted(allowed_tlds))}")

        for query in targets:
            if len(sites) >= TARGET_SITE_COUNT:
                break

            candidates, source_error = generate_candidate_links(
                query,
                api_key,
                cx_id,
                MAX_RESULTS,
                processed_links,
                use_gemini=gemini_available,
            )
            if source_error:
                print(f"Info sites - {query}: {source_error}")

            if not candidates:
                continue

            for link, origin in candidates:
                if len(sites) >= TARGET_SITE_COUNT:
                    break

                host = (urlparse(link).hostname or "").lower()
                tld = host.split(".")[-1] if host else ""
                if allowed_tlds and tld and tld not in allowed_tlds:
                    continue
                if SKIP_SUBS and any(host.startswith(prefix) for prefix in SKIP_SUBS):
                    continue
                if any(host.endswith(d) for d in BLOCKED_DOMAINS):
                    continue

                record = sites.setdefault(link, {"queries": set(), "sources": set()})
                record["queries"].add(query)
                record["sources"].add(SOURCE_LABELS.get(origin, origin))
                processed_links.add(link)

                if len(sites) >= TARGET_SITE_COUNT:
                    break

            time.sleep(REQUEST_DELAY)

    rows: List[List[str]] = []
    for url, data in sites.items():
        queries = "; ".join(sorted(data["queries"]))
        sources = ", ".join(sorted(data["sources"]))
        rows.append([queries, url, sources])

    rows.sort(key=lambda row: (row[0], row[1]))

    output: List[List[str]] = [[f"Run {run_timestamp}", "", ""], ["Cible", "Site Internet", "Source"]]
    output.extend(rows)

    ws_sites.clear()
    if output:
        ws_sites.update(range_name="A1", values=output)

    print(f"Sites collectés: {len(rows)} / {TARGET_SITE_COUNT}")
    if len(rows) < TARGET_SITE_COUNT:
        print("Objectif de sites non atteint - complétez manuellement ou relancez.")

    return 0


def scrape_emails_mode(
    ws_sites: gspread.Worksheet,
    ws_emails: gspread.Worksheet,
    api_key: str,
    cx_id: str,
) -> int:
    site_values = ws_sites.get_all_values()
    site_entries: List[Dict[str, str]] = []

    for row in site_values:
        if not row or all(not (cell or "").strip() for cell in row):
            continue
        first = (row[0] or "").strip()
        if first.startswith("Run "):
            continue
        if first.lower() == "cible":
            continue

        url = (row[1] if len(row) > 1 else "").strip()
        if not url:
            continue
        source = (row[2] if len(row) > 2 else "").strip()
        site_entries.append({
            "target": first,
            "url": url,
            "source": source,
        })

    if not site_entries:
        print("Aucun site à scraper dans la feuille des sites.")
        return 0

    site_records: Dict[str, Dict[str, Set[str]]] = {}
    ordered_urls: List[str] = []

    for entry in site_entries:
        url = entry["url"]
        record = site_records.get(url)
        if record is None:
            record = {
                "queries": set(),
                "sources": set(),
                "emails": set(),
            }
            site_records[url] = record
            ordered_urls.append(url)
        if entry["target"]:
            record["queries"].add(entry["target"])
        if entry["source"]:
            record["sources"].add(entry["source"])

    collected_emails: Set[str] = set()
    allowed_tlds = set(ALLOW_TLDS) | set(FALLBACK_TLDS)

    for url in ordered_urls:
        if TARGET_EMAIL_COUNT and len(collected_emails) >= TARGET_EMAIL_COUNT:
            break

        record = site_records[url]
        host = (urlparse(url).hostname or "").lower()
        tld = host.split(".")[-1] if host else ""
        if allowed_tlds and tld and tld not in allowed_tlds:
            continue
        if SKIP_SUBS and any(host.startswith(prefix) for prefix in SKIP_SUBS):
            continue
        if any(host.endswith(d) for d in BLOCKED_DOMAINS):
            continue

        response, http_error = fetch_page(url)
        emails_found: Set[str] = set()

        if response is not None:
            emails_found |= extract_emails_from_html(response.text)

            if DEEP_SCRAPE:
                base_url = response.url or url
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
            if TARGET_EMAIL_COUNT and len(collected_emails) >= TARGET_EMAIL_COUNT:
                break

        time.sleep(REQUEST_DELAY)

    run_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    rows: List[List[str]] = []
    for url in ordered_urls:
        record = site_records[url]
        if not record["emails"]:
            continue
        queries = "; ".join(sorted(record["queries"]))
        sources = ", ".join(sorted(record["sources"]))
        emails = "; ".join(sorted(record["emails"]))
        rows.append([queries, url, f"{sources} | {emails}" if sources else emails])

    rows.sort(key=lambda row: (row[0], row[1]))

    output: List[List[str]] = [[f"Run {run_timestamp}", "", ""], ["Cible", "Site Internet", "Mails"]]
    output.extend(rows)

    existing_emails = ws_emails.get_all_values() if APPEND_MODE else []
    if APPEND_MODE and existing_emails:
        start_row = len(existing_emails) + 1
        ws_emails.update(range_name=f"A{start_row}", values=output)
    else:
        ws_emails.clear()
        ws_emails.update(range_name="A1", values=output)

    print(f"Emails collectés: {len(collected_emails)} / {TARGET_EMAIL_COUNT}")
    if len(collected_emails) < TARGET_EMAIL_COUNT:
        print("Objectif d'emails non atteint - relance possible après enrichissement des sites.")

    return 0


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

        ws_input, ws_sites, ws_emails = connect_worksheets(creds_path)
        targets = read_targets(ws_input)
        print(f"Cibles lues: {len(targets)}")

        if SCRAPER_MODE == "collect_sites":
            return collect_sites_mode(targets, ws_sites, api_key, cx_id)
        if SCRAPER_MODE == "scrape_emails":
            return scrape_emails_mode(ws_sites, ws_emails, api_key, cx_id)

        print(f"Mode SCRAPER_MODE inconnu: {SCRAPER_MODE}")
        return 1

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

