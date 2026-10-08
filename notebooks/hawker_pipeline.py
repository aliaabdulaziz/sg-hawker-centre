# Databricks notebook source
"""
Singapore Hawker Density Lakehouse Pipeline
Databricks Community Edition · data.gov.sg · SLA OneMap · Folium

Copy each block below (between the CELL banners) into its own notebook cell.
If you import this file as a Databricks .py notebook, the `# COMMAND ----------`
separators already split the four cells.
"""

# COMMAND ----------
# =============================================================================
# CELL 1 — Live API Ingestion (data.gov.sg datastore)
# =============================================================================
# Fetches every row of NEA "List of Government Markets Hawker Centres"
# (resource_id d_68a42f09f350881996d83f9cd73ab02f) via paginated datastore_search.

import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import folium
import pandas as pd
import requests
from folium.plugins import HeatMap, MarkerCluster
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("hawker_pipeline")

DATASTORE_URL = "https://data.gov.sg/api/action/datastore_search"
RESOURCE_ID = "d_68a42f09f350881996d83f9cd73ab02f"
ONEMAP_SEARCH_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
PAGE_SIZE = 100
HTTP_TIMEOUT = 30
USER_AGENT = "sg-hawker-centre-pipeline/1.0 (databricks; educational)"

def load_dotenv_file(path: Path = Path(".env")) -> None:
    """Load KEY=VALUE pairs from a local .env without adding a dependency."""
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def optional_secret(widget_name: str, env_name: str) -> str:
    """Read a non-committed credential from a Databricks widget, else the environment."""
    try:
        value = str(dbutils.widgets.get(widget_name) or "")  # noqa: F821
        if value.strip():
            return value.strip()
    except Exception:
        pass
    return os.environ.get(env_name, "").strip()


load_dotenv_file()


# Optional. Never commit these values — this repo is public.
# Databricks: dbutils.widgets.text("onemap_token" / "carto_tile_key", "")
# Local: export ONEMAP_TOKEN=... CARTO_TILE_KEY=...
ONEMAP_TOKEN = optional_secret("onemap_token", "ONEMAP_TOKEN")
CARTO_TILE_KEY = optional_secret("carto_tile_key", "CARTO_TILE_KEY")


def build_http_session() -> requests.Session:
    """Session with retries for transient 429/5xx responses."""
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


HTTP = build_http_session()


def fetch_hawker_records(session: requests.Session = HTTP) -> List[Dict[str, Any]]:
    """Page through datastore_search until every live record is collected."""
    records: List[Dict[str, Any]] = []
    offset = 0
    total: Optional[int] = None

    while True:
        params = {"resource_id": RESOURCE_ID, "limit": PAGE_SIZE, "offset": offset}
        response = session.get(DATASTORE_URL, params=params, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise RuntimeError(f"data.gov.sg datastore_search failed: {payload}")

        result = payload["result"]
        batch = result.get("records") or []
        total = int(result.get("total", 0))
        records.extend(batch)
        logger.info("Ingested %s / %s hawker records (offset=%s)", len(records), total, offset)

        if not batch or len(records) >= total:
            break
        offset += PAGE_SIZE

    if total is not None and len(records) != total:
        logger.warning("Expected %s records but received %s", total, len(records))
    return records


raw_records = fetch_hawker_records()
hawker_df = pd.DataFrame(raw_records)
if hawker_df.empty:
    raise RuntimeError("data.gov.sg returned zero hawker-centre records.")

logger.info("Live ingest complete: %s rows, columns=%s", len(hawker_df), list(hawker_df.columns))
display(hawker_df.head(10))  # noqa: F821 — Databricks builtin; prints in Jupyter

# COMMAND ----------
# =============================================================================
# CELL 2 — Postal Standardization (6-digit leading-zero padding)
# =============================================================================
# Extracts S(xxxxx) tokens from location_of_centre and forces Singapore's
# 6-digit postal format (e.g. 69111 → 069111). Also casts stall counts.

POSTAL_RE = re.compile(r"S\s*\(\s*(\d{4,6})\s*\)", re.IGNORECASE)

STALL_COLUMNS = ("no_of_stalls", "no_of_cooked_food_stalls", "no_of_mkt_produce_stalls")

# First two digits of a 6-digit postal code = SingPost postal sector.
# Labels follow the conventional district grouping used in planning analyses.
POSTAL_SECTOR_LABELS: Dict[str, str] = {
    "01": "Raffles Place / Cecil / Marina",
    "02": "Raffles Place / Cecil / Marina",
    "03": "Raffles Place / Cecil / Marina",
    "04": "Raffles Place / Cecil / Marina",
    "05": "Raffles Place / Cecil / Marina",
    "06": "Raffles Place / Cecil / Marina",
    "07": "Anson / Tanjong Pagar",
    "08": "Anson / Tanjong Pagar",
    "09": "Telok Blangah / Harbourfront",
    "10": "Telok Blangah / Harbourfront",
    "11": "Pasir Panjang / Hong Leong Garden / Clementi New Town",
    "12": "Pasir Panjang / Hong Leong Garden / Clementi New Town",
    "13": "Pasir Panjang / Hong Leong Garden / Clementi New Town",
    "14": "Queenstown / Tiong Bahru",
    "15": "Queenstown / Tiong Bahru",
    "16": "Queenstown / Tiong Bahru",
    "17": "High Street / Beach Road (part)",
    "18": "Middle Road / Golden Mile",
    "19": "Middle Road / Golden Mile",
    "20": "Little India",
    "21": "Little India",
    "22": "Orchard / Cairnhill / River Valley",
    "23": "Orchard / Cairnhill / River Valley",
    "24": "Ardmore / Bukit Timah / Holland Road / Tanglin",
    "25": "Ardmore / Bukit Timah / Holland Road / Tanglin",
    "26": "Ardmore / Bukit Timah / Holland Road / Tanglin",
    "27": "Ardmore / Bukit Timah / Holland Road / Tanglin",
    "28": "Watten Estate / Novena / Thomson",
    "29": "Watten Estate / Novena / Thomson",
    "30": "Watten Estate / Novena / Thomson",
    "31": "Balestier / Toa Payoh / Serangoon",
    "32": "Balestier / Toa Payoh / Serangoon",
    "33": "Balestier / Toa Payoh / Serangoon",
    "34": "Macpherson / Braddell",
    "35": "Macpherson / Braddell",
    "36": "Macpherson / Braddell",
    "37": "Macpherson / Braddell",
    "38": "Geylang / Eunos",
    "39": "Geylang / Eunos",
    "40": "Geylang / Eunos",
    "41": "Geylang / Eunos",
    "42": "Katong / Joo Chiat / Amber Road",
    "43": "Katong / Joo Chiat / Amber Road",
    "44": "Katong / Joo Chiat / Amber Road",
    "45": "Katong / Joo Chiat / Amber Road",
    "46": "Bedok / Upper East Coast / Eastwood",
    "47": "Bedok / Upper East Coast / Eastwood",
    "48": "Bedok / Upper East Coast / Eastwood",
    "49": "Loyang / Changi",
    "50": "Loyang / Changi",
    "51": "Tampines / Pasir Ris",
    "52": "Tampines / Pasir Ris",
    "53": "Serangoon Garden / Hougang / Ponggol",
    "54": "Serangoon Garden / Hougang / Ponggol",
    "55": "Serangoon Garden / Hougang / Ponggol",
    "56": "Bishan / Ang Mo Kio",
    "57": "Bishan / Ang Mo Kio",
    "58": "Upper Bukit Timah / Clementi Park / Ulu Pandan",
    "59": "Upper Bukit Timah / Clementi Park / Ulu Pandan",
    "60": "Jurong",
    "61": "Jurong",
    "62": "Jurong",
    "63": "Jurong",
    "64": "Jurong",
    "65": "Hillview / Dairy Farm / Bukit Panjang / Choa Chu Kang",
    "66": "Hillview / Dairy Farm / Bukit Panjang / Choa Chu Kang",
    "67": "Hillview / Dairy Farm / Bukit Panjang / Choa Chu Kang",
    "68": "Hillview / Dairy Farm / Bukit Panjang / Choa Chu Kang",
    "69": "Lim Chu Kang / Tengah",
    "70": "Lim Chu Kang / Tengah",
    "71": "Lim Chu Kang / Tengah",
    "72": "Kranji / Woodgrove",
    "73": "Kranji / Woodgrove",
    "75": "Yishun / Sembawang",
    "76": "Yishun / Sembawang",
    "77": "Upper Thomson / Springleaf",
    "78": "Upper Thomson / Springleaf",
    "79": "Seletar",
    "80": "Seletar",
    "81": "Loyang / Changi",
    "82": "Punggol / Sengkang",
}


def extract_postal_code(location: Any) -> Optional[str]:
    """Pull a Singapore postal code from an S(...) address fragment and zfill to 6 digits."""
    if location is None or (isinstance(location, float) and pd.isna(location)):
        return None
    match = POSTAL_RE.search(str(location))
    if not match:
        digits = re.sub(r"\D", "", str(location))
        if 4 <= len(digits) <= 6:
            return digits.zfill(6)
        return None
    return match.group(1).zfill(6)


def standardize_hawker_frame(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    if "_id" in clean.columns:
        clean = clean.drop(columns=["_id"])

    for col in STALL_COLUMNS:
        if col in clean.columns:
            clean[col] = pd.to_numeric(clean[col], errors="coerce").fillna(0).astype(int)

    clean["postal_code"] = clean["location_of_centre"].map(extract_postal_code)
    missing = clean["postal_code"].isna().sum()
    if missing:
        logger.warning("%s rows have no parseable postal code", missing)

    clean["postal_sector"] = clean["postal_code"].str.slice(0, 2)
    clean["planning_sector"] = clean["postal_sector"].map(
        lambda s: POSTAL_SECTOR_LABELS.get(s, f"Sector {s}") if pd.notna(s) else "Unknown"
    )
    clean["centre_type"] = clean["type_of_centre"].map(
        {"HC": "Hawker Centre", "MHC": "Market & Hawker Centre"}
    ).fillna(clean["type_of_centre"])
    return clean


hawker_df = standardize_hawker_frame(hawker_df)
display(hawker_df[["name_of_centre", "location_of_centre", "postal_code", "planning_sector"]].head(15))  # noqa: F821

# COMMAND ----------
# =============================================================================
# CELL 3 — Geocoding Engine (SLA OneMap, unique postal codes)
# =============================================================================
# Loads data/onemap_geocode_cache.csv first. Only postal codes missing from the
# cache (or previously failed with status "error") hit OneMap, with a 0.1s pause.
# The cache is rewritten after each new lookup so a stopped run keeps progress.
# Set widget/env FORCE_GEOCODE=true to ignore the cache and refresh everything.

GEOCODE_SLEEP_SECONDS = 0.1
GEOCODE_CACHE_PATH = Path("data") / "onemap_geocode_cache.csv"
FORCE_GEOCODE = optional_secret("force_geocode", "FORCE_GEOCODE").lower() in {"1", "true", "yes"}
CACHE_COLUMNS = ["postal_code", "latitude", "longitude", "geocode_status"]


def _onemap_headers() -> Dict[str, str]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if ONEMAP_TOKEN:
        headers["Authorization"] = ONEMAP_TOKEN
    return headers


def geocode_postal_code(postal_code: str, session: requests.Session = HTTP) -> Optional[Dict[str, float]]:
    """Resolve a 6-digit postal code to WGS84 lat/lon via OneMap Search."""
    params = {
        "searchVal": postal_code,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
        "pageNum": 1,
    }
    response = session.get(
        ONEMAP_SEARCH_URL,
        params=params,
        headers=_onemap_headers(),
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    results: List[Dict[str, Any]] = payload.get("results") or []
    if not results:
        return None

    match = next((row for row in results if str(row.get("POSTAL", "")).zfill(6) == postal_code), results[0])
    try:
        return {"latitude": float(match["LATITUDE"]), "longitude": float(match["LONGITUDE"])}
    except (KeyError, TypeError, ValueError):
        return None


def load_geocode_cache(path: Path) -> pd.DataFrame:
    """Read cached lat/lon. Postal codes are re-padded — CSV readers drop leading zeros."""
    if not path.is_file():
        return pd.DataFrame(columns=CACHE_COLUMNS)
    cache = pd.read_csv(path, dtype={"postal_code": str})
    cache["postal_code"] = cache["postal_code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    for col in ("latitude", "longitude"):
        if col in cache.columns:
            cache[col] = pd.to_numeric(cache[col], errors="coerce")
    if "geocode_status" not in cache.columns:
        cache["geocode_status"] = cache["latitude"].notna().map({True: "ok", False: "error"})
    cache = cache.drop_duplicates(subset=["postal_code"], keep="last")
    return cache[CACHE_COLUMNS]


def save_geocode_cache(cache: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    persist = cache.copy()
    persist["postal_code"] = persist["postal_code"].astype(str).str.zfill(6)
    persist[CACHE_COLUMNS].drop_duplicates(subset=["postal_code"], keep="last").sort_values(
        "postal_code"
    ).to_csv(path, index=False)


def lookup_one_postal(postal_code: str) -> Dict[str, Any]:
    try:
        coords = geocode_postal_code(postal_code)
        if coords:
            return {"postal_code": postal_code, **coords, "geocode_status": "ok"}
        logger.warning("OneMap returned no geometry for postal %s", postal_code)
        return {"postal_code": postal_code, "latitude": None, "longitude": None, "geocode_status": "not_found"}
    except requests.RequestException as exc:
        logger.error("OneMap request failed for postal %s: %s", postal_code, exc)
        return {"postal_code": postal_code, "latitude": None, "longitude": None, "geocode_status": "error"}


def geocode_unique_postals(
    postal_codes: Iterable[str],
    cache_path: Path = GEOCODE_CACHE_PATH,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Reuse on-disk cache; OneMap is called only for new or previously failed codes."""
    unique_codes = sorted({code for code in postal_codes if isinstance(code, str) and code})
    cache = pd.DataFrame(columns=CACHE_COLUMNS) if force_refresh else load_geocode_cache(cache_path)

    reusable = cache[cache["geocode_status"].isin(["ok", "not_found"])] if not cache.empty else cache
    cached_ok = set(reusable["postal_code"]) if not reusable.empty else set()
    to_fetch = [code for code in unique_codes if code not in cached_ok]

    logger.info(
        "Geocode cache: %s reusable / %s unique postals (%s to fetch%s)",
        len(cached_ok & set(unique_codes)),
        len(unique_codes),
        len(to_fetch),
        "; FORCE_GEOCODE" if force_refresh else "",
    )

    running = cache.copy()
    new_rows: List[Dict[str, Any]] = []
    for index, postal_code in enumerate(to_fetch, start=1):
        row = lookup_one_postal(postal_code)
        new_rows.append(row)
        running = pd.concat([running, pd.DataFrame([row])], ignore_index=True)
        save_geocode_cache(running, cache_path)
        if index < len(to_fetch):
            time.sleep(GEOCODE_SLEEP_SECONDS)
        if index % 25 == 0 or index == len(to_fetch):
            logger.info("Fetched %s / %s uncached postal codes from OneMap", index, len(to_fetch))

    geocode_df = pd.concat([reusable, pd.DataFrame(new_rows)], ignore_index=True) if new_rows else reusable.copy()
    needed = pd.DataFrame({"postal_code": unique_codes})
    geocode_df = needed.merge(geocode_df, on="postal_code", how="left")
    save_geocode_cache(running if not running.empty else geocode_df, cache_path)
    return geocode_df


unique_postals = hawker_df["postal_code"].dropna().astype(str)
geocode_df = geocode_unique_postals(unique_postals, force_refresh=FORCE_GEOCODE)
hawker_geo_df = hawker_df.merge(geocode_df, on="postal_code", how="left")

mapped = hawker_geo_df["latitude"].notna().sum()
logger.info("Geocoding complete: %s / %s centres have coordinates", mapped, len(hawker_geo_df))
display(hawker_geo_df.head(10))  # noqa: F821

# COMMAND ----------
# =============================================================================
# CELL 4 — Aggregation & Interactive Folium Map (Databricks displayHTML)
# =============================================================================
# Groups centres by planning sector, plots MarkerCluster + stall-weighted HeatMap,
# writes a standalone HTML file, and renders via displayHTML() in Databricks.

SG_MAP_CENTER = (1.3521, 103.8198)
OUTPUT_PATH = Path("output") / "hawker_density_map.html"
CARTO_TILE_ATTR = (
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> '
    'contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
)


def resolve_basemap(carto_key: str):
    """Keyed Carto Voyager when CARTO_TILE_KEY is set; otherwise OSM (no key, no watermark)."""
    if carto_key:
        tile_url = (
            "https://basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png"
            f"?key={carto_key}"
        )
        return tile_url, CARTO_TILE_ATTR
    return "OpenStreetMap", None


def aggregate_by_planning_sector(df: pd.DataFrame) -> pd.DataFrame:
    mapped_df = df.dropna(subset=["latitude", "longitude"])
    sector_summary = (
        mapped_df.groupby("planning_sector", dropna=False)
        .agg(
            hawker_centres=("name_of_centre", "count"),
            total_stalls=("no_of_stalls", "sum"),
            cooked_food_stalls=("no_of_cooked_food_stalls", "sum"),
            market_produce_stalls=("no_of_mkt_produce_stalls", "sum"),
            avg_latitude=("latitude", "mean"),
            avg_longitude=("longitude", "mean"),
        )
        .sort_values("hawker_centres", ascending=False)
        .reset_index()
    )
    return sector_summary


sector_summary = aggregate_by_planning_sector(hawker_geo_df)
display(sector_summary)  # noqa: F821


def build_hawker_map(df: pd.DataFrame, sector_df: pd.DataFrame) -> folium.Map:
    mapped_df = df.dropna(subset=["latitude", "longitude"]).copy()
    if mapped_df.empty:
        raise RuntimeError("No geocoded hawker centres available to plot.")

    tiles, tile_attr = resolve_basemap(CARTO_TILE_KEY)
    map_kwargs = dict(
        location=list(SG_MAP_CENTER),
        zoom_start=12,
        tiles=tiles,
        control_scale=True,
        prefer_canvas=True,
    )
    if tile_attr:
        map_kwargs["attr"] = tile_attr
    folium_map = folium.Map(**map_kwargs)

    cluster = MarkerCluster(name="Hawker centres").add_to(folium_map)
    type_colours = {"HC": "#d9480f", "MHC": "#1c7ed6"}

    for row in mapped_df.itertuples(index=False):
        colour = type_colours.get(getattr(row, "type_of_centre", ""), "#495057")
        popup_html = (
            f"<b>{row.name_of_centre}</b><br>"
            f"{row.location_of_centre}<br>"
            f"Postal: {row.postal_code}<br>"
            f"Sector: {row.planning_sector}<br>"
            f"Type: {getattr(row, 'centre_type', row.type_of_centre)}<br>"
            f"Stalls: {row.no_of_stalls} "
            f"(cooked {row.no_of_cooked_food_stalls} / market {row.no_of_mkt_produce_stalls})"
        )
        folium.CircleMarker(
            location=(row.latitude, row.longitude),
            radius=max(5, min(14, int(row.no_of_stalls) ** 0.5)),
            color=colour,
            fill=True,
            fill_color=colour,
            fill_opacity=0.85,
            weight=1,
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=row.name_of_centre,
        ).add_to(cluster)

    heat_data = [
        [lat, lon, max(float(weight), 1.0)]
        for lat, lon, weight in zip(
            mapped_df["latitude"], mapped_df["longitude"], mapped_df["no_of_stalls"]
        )
    ]
    HeatMap(
        heat_data,
        name="Stall-density heat layer",
        min_opacity=0.25,
        radius=18,
        blur=15,
        max_zoom=16,
    ).add_to(folium_map)

    for row in sector_df.itertuples(index=False):
        if pd.isna(row.avg_latitude) or pd.isna(row.avg_longitude):
            continue
        folium.Marker(
            location=(row.avg_latitude, row.avg_longitude),
            icon=folium.DivIcon(
                html=(
                    f'<div style="font-size:10px;font-weight:700;color:#212529;'
                    f'background:#fff;border:1px solid #adb5bd;border-radius:4px;'
                    f'padding:2px 4px;white-space:nowrap;">{row.hawker_centres}</div>'
                )
            ),
            tooltip=f"{row.planning_sector}: {row.hawker_centres} centres, {row.total_stalls} stalls",
        ).add_to(folium_map)

    folium.LayerControl(collapsed=False).add_to(folium_map)
    return folium_map


hawker_map = build_hawker_map(hawker_geo_df, sector_summary)

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
hawker_map.save(str(OUTPUT_PATH))
logger.info("Wrote standalone map to %s", OUTPUT_PATH.resolve())

# Databricks: inject the Folium iframe HTML. Keep _repr_html_() (iframe) rather
# than get_root().render() so the notebook cell stays lightweight.
map_html = hawker_map._repr_html_()
try:
    displayHTML(map_html)  # noqa: F821 — Databricks builtin
except NameError:
    from IPython.display import HTML, display as ipy_display

    ipy_display(HTML(map_html))
