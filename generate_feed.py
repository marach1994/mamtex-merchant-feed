import csv
import io
import logging
import os
import time
import requests

FEED_URL = (
    "https://www.mamtex.cz/export/products.csv"
    "?patternId=290&partnerId=8"
    "&hash=6ab834315cf9d11e825e5f753589c85b9b9598a212eb72db02650da05496df43"
)
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "docs", "feed.tsv")
MAX_RETRIES = 3
RETRY_DELAY = 5

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def fetch_csv(url: str) -> str:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.content.decode("windows-1250")
        except requests.RequestException as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    raise RuntimeError(f"Failed to fetch feed after {MAX_RETRIES} attempts")


def parse_price(value: str) -> float:
    return float(value.replace(",", ".").strip())


def round_margin(margin_pct: float, step: float = 2.5) -> float:
    return round(margin_pct / step) * step


def format_margin(rounded: float) -> str:
    return str(int(rounded)) if rounded == int(rounded) else str(rounded)


def process(raw_csv: str) -> tuple[list[dict], list[str]]:
    reader = csv.DictReader(io.StringIO(raw_csv), delimiter=";")
    rows = []
    skipped = []

    for record in reader:
        product_id = record.get("code", "").strip()
        price_raw = record.get("price", "").strip()
        purchase_raw = record.get("purchasePrice", "").strip()
        supplier = record.get("supplier", "").strip()

        if not purchase_raw:
            log.info("SKIP id=%s: missing purchasePrice", product_id)
            skipped.append(product_id)
            continue

        try:
            price = parse_price(price_raw)
            purchase = parse_price(purchase_raw)
        except ValueError:
            log.info("SKIP id=%s: non-numeric price/purchasePrice", product_id)
            skipped.append(product_id)
            continue

        if price == 0:
            log.info("SKIP id=%s: price is zero", product_id)
            skipped.append(product_id)
            continue

        margin_pct = (price - purchase) / price * 100
        rounded = round_margin(margin_pct)
        label = format_margin(rounded)

        action_label = ""
        action_raw = record.get("actionPrice", "").strip()
        if action_raw:
            try:
                action_price = parse_price(action_raw)
                if action_price > 0:
                    action_margin = (action_price - purchase) / action_price * 100
                    action_label = format_margin(round_margin(action_margin))
            except ValueError:
                pass

        rows.append({
            "id": product_id,
            "custom_label_2": label,
            "custom_label_3": supplier,
            "custom_label_4": action_label,
        })

    return rows, skipped


def write_tsv(rows: list[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id", "custom_label_2", "custom_label_3", "custom_label_4"],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    log.info("Fetching feed from %s", FEED_URL)
    raw = fetch_csv(FEED_URL)

    rows, skipped = process(raw)

    log.info("Writing %d rows to %s", len(rows), OUTPUT_PATH)
    write_tsv(rows, OUTPUT_PATH)
    log.info("Done. Skipped %d products.", len(skipped))


if __name__ == "__main__":
    main()
