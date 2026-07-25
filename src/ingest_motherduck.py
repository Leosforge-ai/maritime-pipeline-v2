# src/ingest_motherduck.py
import argparse
import logging
import os
import pickle
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import requests
from scipy.spatial import KDTree

from src.ais_sources import AISSource, AisstreamSource, NoaaMarineCadastreSource
from src.constants import PORT_FUNCTION_FILTER, PORT_STATUS_CODES

# We import simple utils, ignoring the Modal decorators in the original file
# effectively
from src.geospacial import to_radians

# --- CONFIG ---
MD_CONN_STR = "md:"
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "").strip()
COUNTRIES_TABLE = "reference.countries"
PORTS_TABLE = "reference.ports"
RAW_AIS_TABLE = "silver.raw_ais_pings"
DEFAULT_DRY_RUN_PATH = "dryrun_ais.duckdb"

# --- LOGGING SETUP ---
logger = logging.getLogger("ais_ingest")


def get_db_connection(dry_run: bool = False, dry_run_path: str = DEFAULT_DRY_RUN_PATH):
    """Return a DuckDB connection. In --dry-run mode this is a local file (no
    MOTHERDUCK_TOKEN required); otherwise it's MotherDuck (`md:`)."""
    if dry_run:
        logger.debug(f"[dry-run] Connecting to local DuckDB file {dry_run_path}...")
        return duckdb.connect(dry_run_path)

    if not MOTHERDUCK_TOKEN:
        raise ValueError("MOTHERDUCK_TOKEN not found in environment variables")
    logger.debug(f"Connecting to MotherDuck with DuckDB {duckdb.__version__}...")
    con = duckdb.connect(MD_CONN_STR)

    con.execute("CREATE DATABASE IF NOT EXISTS my_voyage_db")
    con.execute("USE my_voyage_db")
    return con


def retry_request(url, headers=None, retries=3, stream=False):
    """Helper to fetch URL with retries."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, stream=stream, timeout=20)
            if resp.status_code == 200:
                return resp
            logger.warning(
                f"Attempt {attempt + 1}/{retries} failed for {url}: Status {resp.status_code}"
            )
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{retries} error for {url}: {e}")

        if attempt < retries - 1:
            time.sleep(2**attempt)  # Exponential backoff

    logger.error(f"Failed to fetch {url} after {retries} attempts.")
    return None


def parse_unece_coord(coord_str):
    """Parses UNECE coordinate string (e.g., '5157N', '00408E') into float."""
    if not coord_str or len(coord_str) < 5:
        return None
    try:
        factor = -1 if coord_str[-1] in ["S", "W"] else 1

        # Format is D...M...X
        # Lat: 2 digits deg, 2 digits min. Lon: 3 digits deg, 2 digits min.
        minutes = float(coord_str[-3:-1])
        degrees = float(coord_str[:-3])

        return (degrees + (minutes / 60.0)) * factor
    except Exception:
        return None


def scrape_iso_countries(con):
    """Scrapes ISO 3166-2 country codes from Wikipedia."""
    from bs4 import BeautifulSoup

    logger.info("📖 Scraping ISO country codes from Wikipedia...")
    url = "https://en.wikipedia.org/wiki/ISO_3166-2"
    headers = {"User-Agent": "Mozilla/5.0"}

    resp = retry_request(url, headers=headers)
    if not resp:
        raise RuntimeError("Could not fetch ISO codes.")

    soup = BeautifulSoup(resp.text, "html.parser")
    h2 = soup.find("h2", {"id": "Current_codes"})
    if not h2:
        # Fallback to hardcoded major countries if scraping fails structure
        logger.error("Could not find 'Current_codes' section. Wiki format may have changed.")
        return []

    table = h2.find_next("table")
    iso_codes = set()

    for tr in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if cols:
            code = cols[0]
            if len(code) == 2 and code.isupper():
                iso_codes.add(code)

    sorted_codes = sorted(list(iso_codes))
    logger.info(f"✅ Found {len(sorted_codes)} ISO country codes.")

    # Save to DB
    con.execute(f"DROP TABLE IF EXISTS {COUNTRIES_TABLE}")
    con.execute(f"CREATE TABLE {COUNTRIES_TABLE} (code VARCHAR, updated_at TIMESTAMP)")

    # Insert in batches or simple loop
    now = datetime.now(timezone.utc)
    data = [(code, now) for code in sorted_codes]
    con.executemany(f"INSERT INTO {COUNTRIES_TABLE} VALUES (?, ?)", data)

    return sorted_codes


def scrape_ports(con, iso_codes):
    """Scrapes UN/LOCODE ports for the given country codes."""
    from bs4 import BeautifulSoup

    logger.info(f"⚓ Scraping ports for {len(iso_codes)} countries...")
    headers = {"User-Agent": "Mozilla/5.0"}
    all_ports = []

    def fetch_country_ports(iso):
        url = f"https://service.unece.org/trade/locode/{iso.lower()}.htm"
        resp = retry_request(url, headers=headers, retries=3)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.find_all("tr")
        country_ports = []

        for tr in rows[10:]:  # Skip header rows
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 10:
                continue

            # Format: ' AR ', ' BLA ' -> 'ARBLA'
            locode_raw = cells[1].replace("\xa0", "").replace(" ", "").upper()

            if not locode_raw or len(locode_raw) < 3:
                continue

            full_locode = f"{iso}{locode_raw}" if len(locode_raw) == 3 else locode_raw
            name = cells[2]
            function = cells[5]
            status = cells[6].strip()
            coords_str = cells[9].strip()

            # Filters
            if PORT_FUNCTION_FILTER not in function:
                continue
            if status[:2] not in PORT_STATUS_CODES:
                continue
            if not coords_str:
                continue

            lat = parse_unece_coord(coords_str.split(" ")[0])
            lon = parse_unece_coord(coords_str.split(" ")[1]) if " " in coords_str else None

            if lat is not None and lon is not None:
                country_ports.append({"LOCODE": full_locode, "Name": name, "lat": lat, "lon": lon})
        return country_ports

    # Parallel scraping
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_iso = {executor.submit(fetch_country_ports, iso): iso for iso in iso_codes}
        for i, future in enumerate(as_completed(future_to_iso)):
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(iso_codes)} countries processed...")
            res = future.result()
            if res:
                all_ports.extend(res)

    if not all_ports:
        logger.error("No ports scraped!")
        return False

    logger.info(f"✅ Extracted {len(all_ports)} total ports. Saving to DB...")

    # Save to DuckDB
    con.execute(f"DROP TABLE IF EXISTS {PORTS_TABLE}")
    con.execute(
        f"""
        CREATE TABLE {PORTS_TABLE} (
            LOCODE VARCHAR, Name VARCHAR,
            lat DOUBLE, lon DOUBLE,
            updated_at TIMESTAMP
        )
        """
    )

    # Create Polars DF for bulk insert
    df = pl.DataFrame(all_ports).with_columns(
        pl.lit(datetime.now(timezone.utc)).alias("updated_at")
    )
    con.register("df_ports_new", df)
    con.execute(f"INSERT INTO {PORTS_TABLE} SELECT * FROM df_ports_new")
    con.unregister("df_ports_new")

    return True


def ensure_reference_data(con):
    """Checks if ports exist in MotherDuck; if not, bootstraps them."""
    con.execute("CREATE SCHEMA IF NOT EXISTS reference")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # 1. Check Countries (Update yearly)
    try:
        res = con.sql(f"SELECT MAX(updated_at) FROM {COUNTRIES_TABLE}").fetchone()
        last_country_update = res[0] if res else None
    except Exception:
        last_country_update = None

    iso_codes = []
    if not last_country_update or (datetime.now() - last_country_update).days > 365:
        logger.info("Countries table missing or stale (>1 year). Updating...")
        iso_codes = scrape_iso_countries(con)
    else:
        logger.info("✅ Countries table is up to date.")
        iso_codes = [r[0] for r in con.sql(f"SELECT code FROM {COUNTRIES_TABLE}").fetchall()]

    # 2. Check Ports (Update every 6 months)
    try:
        res = con.sql(f"SELECT MAX(updated_at) FROM {PORTS_TABLE}").fetchone()
        last_port_update = res[0] if res else None
    except Exception:
        last_port_update = None

    if not last_port_update or (datetime.now() - last_port_update).days > 180:
        logger.info("Ports table missing or stale (>6 months). Updating...")
        success = scrape_ports(con, iso_codes)
        if not success:
            logger.error("Scraping failed. Checking for existing data...")
            # If scraping failed entirely and we have no table, we must fallback
            try:
                count = con.sql(f"SELECT COUNT(*) FROM {PORTS_TABLE}").fetchone()[0]
                if count > 0:
                    logger.warning("Using existing stale data.")
                    return
            except Exception:
                pass

            logger.warning(
                "⚠️ No ports available. Bootstrapping with HARDCODED major ports as fallback."
            )
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PORTS_TABLE} (
                LOCODE VARCHAR,
                Name VARCHAR,
                lat DOUBLE,
                lon DOUBLE,
                updated_at TIMESTAMP
            )
        """
        )

        # Insert major ports to ensure the pipeline functions on first run
        # (Rotterdam, Singapore, Shanghai, Los Angeles, New York, etc.)
        con.execute(
            f"""
            INSERT INTO {PORTS_TABLE} VALUES
            ('NLRTM', 'Rotterdam', 51.95, 4.13, CURRENT_TIMESTAMP),
            ('SGSIN', 'Singapore', 1.28, 103.85, CURRENT_TIMESTAMP),
            ('CNSHA', 'Shanghai', 31.23, 121.47, CURRENT_TIMESTAMP),
            ('CNNGB', 'Ningbo', 29.86, 121.52, CURRENT_TIMESTAMP),
            ('KRPUS', 'Busan', 35.10, 129.04, CURRENT_TIMESTAMP),
            ('USLAX', 'Los Angeles', 33.74, -118.26, CURRENT_TIMESTAMP),
            ('USLGB', 'Long Beach', 33.77, -118.19, CURRENT_TIMESTAMP),
            ('USNYC', 'New York', 40.71, -74.00, CURRENT_TIMESTAMP),
            ('DEHAM', 'Hamburg', 53.55, 9.99, CURRENT_TIMESTAMP),
            ('BEANR', 'Antwerp', 51.22, 4.40, CURRENT_TIMESTAMP),
            ('JPTYO', 'Tokyo', 35.68, 139.76, CURRENT_TIMESTAMP),
            ('AEJEA', 'Jebel Ali', 25.00, 55.06, CURRENT_TIMESTAMP)
        """
        )
        logger.info("✅ Bootstrapped ports table with major global hubs.")
    else:
        logger.info("✅ Ports table is up to date.")


def load_ports_for_kdtree(con):
    """Loads ports into memory for KDTree filtering."""
    cache_file = Path("ports_kdtree.pkl")

    # 1. Try loading from local disk cache first
    if cache_file.exists():
        # Cache valid for 24 hours
        if (time.time() - cache_file.stat().st_mtime) < 86400:
            try:
                with open(cache_file, "rb") as f:
                    data = pickle.load(f)
                logger.info("✅ Loaded KDTree from local disk cache.")
                return data["tree"], data["locodes"], data["names"]
            except Exception:
                logger.warning("⚠️ Cache file corrupted, rebuilding...")

    # 2. Build from MotherDuck if cache missing or stale
    logger.info("🏗️ Building KDTree from MotherDuck ports table...")
    df = con.sql(f"SELECT LOCODE, Name, lat, lon FROM {PORTS_TABLE}").pl()

    port_locodes = df["LOCODE"].to_numpy()
    port_names = df["Name"].to_numpy()
    # Convert to radians for distance calc
    coords = np.deg2rad(df[["lat", "lon"]].to_numpy())
    tree = KDTree(coords)

    # 3. Save to cache
    try:
        with open(cache_file, "wb") as f:
            pickle.dump({"tree": tree, "locodes": port_locodes, "names": port_names}, f)
        logger.info("💾 Saved KDTree to local disk cache.")
    except Exception as e:
        logger.warning(f"⚠️ Could not save KDTree cache: {e}")

    return tree, port_locodes, port_names


def filter_by_port_proximity(ais: pl.DataFrame, tree, port_locodes, port_names):
    """Filter a normalized AIS DataFrame (mmsi, imo, vessel_name, latitude, longitude,
    base_date_time) down to pings within ~5km of a known port, source-agnostic.

    Returns None if there is nothing left after filtering.
    """
    # Filter Nulls
    ais = ais.filter(pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null())

    if ais.height == 0:
        return None

    logger.info(f"🔎 Filtering {ais.height} pings against {len(port_locodes)} ports...")

    # Coordinate Transform & KDTree Query
    ship_coords = to_radians(ais)  # Uses your existing util
    dist, idx = tree.query(ship_coords, k=1)

    # 5km radius filter (approx 0.00078 radians, but let's use the explicit dist logic)
    # Earth radius ~6371km. 5km / 6371 = 0.000784
    mask = (dist * 6371.0) <= 5.0

    if not mask.any():
        logger.info("No pings near ports found.")
        return None

    valid_idx = idx[mask]
    filtered_ais = ais.filter(mask)

    # Explicitly free the large raw dataframe from memory
    del ais
    del dist
    del idx

    # Enrich with Port Info
    filtered_ais = filtered_ais.with_columns(
        [
            pl.Series("port_locode", port_locodes[valid_idx]),
            pl.Series(
                "dep_time",
                filtered_ais["base_date_time"].str.to_datetime(strict=False),
            ),  # Renaming base_date_time
        ]
    ).select(
        [
            "mmsi",
            "imo",
            "vessel_name",
            "latitude",
            "longitude",
            "dep_time",
            "port_locode",
        ]
    )

    logger.info(f"✅ Retained {filtered_ais.height} relevant pings.")
    return filtered_ais


def process_date(
    source: AISSource,
    target_date: date,
    tree,
    locodes,
    names,
    dry_run: bool = False,
    dry_run_path: str = DEFAULT_DRY_RUN_PATH,
):
    """Ingests a single day of data. Never raises — logs and returns on any failure
    so one bad/missing day never aborts the whole run."""
    logger.info(f"🚀 Starting Ingest for {target_date}")

    # Create a fresh connection for this thread to ensure thread safety
    con = get_db_connection(dry_run=dry_run, dry_run_path=dry_run_path)
    try:
        raw = source.fetch(target_date)
        if raw is None:
            logger.warning(f"⚠️ No data published for {target_date} (source returned nothing).")
            return

        df_filtered = filter_by_port_proximity(raw, tree, locodes, names)

        if df_filtered is not None and not df_filtered.is_empty():
            dest = "local dry-run DB" if dry_run else "MotherDuck"
            logger.info(f"📤 Uploading {df_filtered.height} rows for {target_date} to {dest}...")

            # Ensure table exists (idempotent)
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RAW_AIS_TABLE} (
                    mmsi BIGINT,
                    imo VARCHAR,
                    vessel_name VARCHAR,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    dep_time TIMESTAMP,
                    port_locode VARCHAR,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Insert Data
            con.sql(f"INSERT INTO {RAW_AIS_TABLE} SELECT *, CURRENT_TIMESTAMP FROM df_filtered")
            logger.info(f"🎉 Ingestion Complete for {target_date}.")
        else:
            logger.info(f"⚠️ No data to ingest for {target_date}.")
    except Exception as e:
        # Log and continue: one missing/broken day must never crash the whole backfill.
        logger.error(f"❌ Failed to process {target_date}: {e}")
    finally:
        con.close()


def resolve_dates_to_process(
    source: AISSource,
    args: argparse.Namespace,
    today: date | None = None,
) -> list[date]:
    """Availability-aware date resolution.

    - Explicit --year/--month/--day: process exactly that range (unchanged
      behavior), regardless of what the source reports as "available" — missing
      days within the range are simply skipped (logged) by process_date.
    - --backfill-from YYYY-MM-DD: ingest every date the source has actually
      published between that date and the latest available date (inclusive).
    - Default (nothing given): probe the source for its single latest
      available date. NOAA publication lags real time by months, so this is
      *not* "yesterday" — see NoaaMarineCadastreSource docstring.
    """
    today = today or datetime.now(timezone.utc).date()

    if args.year and args.month and args.day:
        d = date(args.year, args.month, args.day)
        return [d] if d <= today else []

    if args.year and args.month:
        start = date(args.year, args.month, 1)
        if args.month == 12:
            end = date(args.year, 12, 31)
        else:
            end = date(args.year, args.month + 1, 1) - timedelta(days=1)
        end = min(end, today)
        return [start + timedelta(days=n) for n in range((end - start).days + 1)]

    if args.year:
        start = date(args.year, 1, 1)
        end = min(date(args.year, 12, 31), today)
        return [start + timedelta(days=n) for n in range((end - start).days + 1)]

    if args.backfill_from:
        backfill_start = datetime.strptime(args.backfill_from, "%Y-%m-%d").date()
        latest = source.latest_available_date(not_after=today)
        if latest is None:
            logger.error("Could not determine latest available date from source; aborting.")
            return []
        logger.info(f"📅 Backfilling published dates from {backfill_start} through {latest}...")
        return source.available_dates(since=backfill_start, until=latest)

    # Default: single most recent published date (NOT "yesterday" — probe the source).
    latest = source.latest_available_date(not_after=today)
    if latest is None:
        logger.error("Could not determine latest available date from source; aborting.")
        return []
    logger.info(f"📅 Latest available date from source: {latest}")
    return [latest]


def main():
    # Set logging config
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description="Ingest AIS Data to MotherDuck")
    parser.add_argument("--year", type=int, help="Year (YYYY)")
    parser.add_argument("--month", type=int, help="Month (1-12)")
    parser.add_argument("--day", type=int, help="Day (1-31)")
    parser.add_argument(
        "--backfill-from",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Ingest every published date from this date through the source's latest "
        "available date (gap backfill).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write to a local DuckDB file instead of MotherDuck. No MOTHERDUCK_TOKEN needed.",
    )
    parser.add_argument(
        "--dry-run-path",
        type=str,
        default=DEFAULT_DRY_RUN_PATH,
        help=f"Local DuckDB file path used with --dry-run (default: {DEFAULT_DRY_RUN_PATH}).",
    )
    parser.add_argument(
        "--source",
        choices=["noaa", "aisstream"],
        default="noaa",
        help="AIS data source: 'noaa' (default) for the NOAA MarineCadastre daily "
        "batch archive, or 'aisstream' for a bounded live collect from aisstream.io "
        "(beta, supplementary only — requires AISSTREAM_API_KEY).",
    )
    parser.add_argument(
        "--collect-seconds",
        type=int,
        default=60,
        help="Live collection window in seconds, used only with --source aisstream (default: 60).",
    )
    args = parser.parse_args()

    if args.source == "aisstream":
        if args.year or args.month or args.day or args.backfill_from:
            logger.warning(
                "--year/--month/--day/--backfill-from are ignored with --source aisstream "
                "(it is live-only; use --collect-seconds instead)."
            )
        source: AISSource = AisstreamSource(collect_seconds=args.collect_seconds)
    else:
        source = NoaaMarineCadastreSource()

    con = get_db_connection(dry_run=args.dry_run, dry_run_path=args.dry_run_path)
    ensure_reference_data(con)
    tree, locodes, names = load_ports_for_kdtree(con)

    # Ensure the destination table exists at least once before threading
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_AIS_TABLE} (
            mmsi BIGINT, imo VARCHAR, vessel_name VARCHAR,
            latitude DOUBLE, longitude DOUBLE,
            dep_time TIMESTAMP, port_locode VARCHAR,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.close()  # Close main connection, threads will open their own

    dates_to_process = resolve_dates_to_process(source, args)
    if not dates_to_process:
        logger.warning("⚠️ No dates to process.")
        return

    logger.info(
        f"📅 Processing {len(dates_to_process)} date(s): "
        f"{dates_to_process[0]}..{dates_to_process[-1]}"
    )

    # Process in parallel (Max 2 workers to prevent OOM on 7GB RAM runners)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                process_date,
                source,
                d,
                tree,
                locodes,
                names,
                args.dry_run,
                args.dry_run_path,
            )
            for d in dates_to_process
        ]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                logger.error(f"Thread failed: {e}")


if __name__ == "__main__":
    main()
