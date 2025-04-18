import requests
import time
import argparse
from collections import defaultdict, deque

SOLR_URL = "http://localhost:8983/solr/nexustiles/select"
ALERT_REPEAT_THRESHOLD = 3
HISTORY_BUFFER_SIZE = 5  # number of recent tile counts to keep per granule

granule_history = defaultdict(lambda: deque(maxlen=HISTORY_BUFFER_SIZE))
seen_records = set()

def poll_solr(filter_granule=None, show_summary=True, only_new=False):
    try:
        response = requests.get(SOLR_URL, params={
            "q": f"granule_s:{filter_granule}" if filter_granule else "*:*",
            "fl": "granule_s,tile_count_i,status_s",
            "rows": 100,
            "wt": "json"
        })
        response.raise_for_status()
        docs = response.json()["response"]["docs"]
    except Exception as e:
        print(f"\n[ERROR] Failed to poll Solr: {e}\n")
        return False

    granule_stats = defaultdict(list)
    new_items_found = False

    for doc in docs:
        granule = doc.get("granule_s", "Unknown")
        tile_count = doc.get("tile_count_i", -1)
        status = doc.get("status_s", "N/A")
        record_key = (granule, tile_count)

        if only_new and record_key in seen_records:
            continue  # Skip previously seen records

        seen_records.add(record_key)
        new_items_found = True

        granule_stats[granule].append((tile_count, status))
        granule_history[granule].append(tile_count)

    if not granule_stats and only_new:
        return False

    print("\nGranule Summary (Latest Results):")
    print("-" * 60)

    total = 0
    failed = 0

    for granule, records in granule_stats.items():
        total += 1
        for tile_count, status in records:
            print(f"Granule: {granule}")
            print(f"  Tile Count: {tile_count}")
            print(f"  Status: {status}\n")

        zero_count = sum(1 for tile_count, _ in records if tile_count == 0)
        if zero_count >= ALERT_REPEAT_THRESHOLD:
            print(f"⚠️  ALERT: Granule '{granule}' has {zero_count} failed ingestions (0 tiles).\n")
            failed += 1

    if show_summary:
        print("Summary Statistics:")
        print("-" * 60)
        print(f"Total Granules Checked: {total}")
        print(f"Granules With ≥ {ALERT_REPEAT_THRESHOLD} Failures: {failed}")
        print("\nRecent Tile Count History:")
        for granule, counts in granule_history.items():
            print(f"  {granule[:40]}...: {list(counts)}")

    return new_items_found

def watch_solr(filter_granule=None, interval=10):
    dot_states = ["", ".", "..", "..."]
    dot_index = 0
    try:
        while True:
            dots = dot_states[dot_index % len(dot_states)]
            print(f"\rPolling Solr for granule info{dots} ", end="", flush=True)
            dot_index += 1

            new_results = poll_solr(filter_granule, show_summary=False, only_new=True)
            if new_results:
                print("\n")  # Only move to new line if something was found
            else:
                time.sleep(0.7)  # Faster animation for dots
                continue

            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped watching Solr.")
        poll_solr(filter_granule, show_summary=True, only_new=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor Solr for ingestion summaries.")
    parser.add_argument("--granule", type=str, help="Optional granule name to filter by")
    parser.add_argument("--watch", action="store_true", help="Continuously watch Solr at intervals")
    parser.add_argument("--interval", type=int, default=10, help="Polling interval in seconds (default: 10)")
    args = parser.parse_args()

    if args.watch:
        watch_solr(filter_granule=args.granule, interval=args.interval)
    else:
        poll_solr(filter_granule=args.granule)
