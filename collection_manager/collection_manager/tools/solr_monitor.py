import requests
import time
from collections import defaultdict

SOLR_URL = "http://localhost:8983/solr/nexustiles/select"
QUERY_PARAMS = {
    "q": "*:*",
    "fl": "granule_s,tile_count_i,status_s",
    "rows": 100,
    "wt": "json"
}

LOW_TILE_THRESHOLD = 1000
ALERT_REPEAT_THRESHOLD = 3


def poll_solr():
    print("Polling Solr for granule info...\n")
    try:
        response = requests.get(SOLR_URL, params=QUERY_PARAMS)
        response.raise_for_status()
        docs = response.json()["response"]["docs"]
    except Exception as e:
        print(f"[ERROR] Failed to poll Solr: {e}")
        return

    granule_stats = defaultdict(list)

    for doc in docs:
        granule = doc.get("granule_s", "Unknown")
        tile_count = doc.get("tile_count_i", -1)
        status = doc.get("status_s", "N/A")
        granule_stats[granule].append((tile_count, status))

    print("Granule Summary (Latest Results):")
    print("-" * 60)

    for granule, records in granule_stats.items():
        for tile_count, status in records:
            print(f"Granule: {granule}")
            print(f"  Tile Count: {tile_count}")
            print(f"  Status: {status}\n")

        # Failure detection logic
        low_count = sum(1 for tile_count, _ in records if tile_count < LOW_TILE_THRESHOLD)
        if low_count >= ALERT_REPEAT_THRESHOLD:
            print(f"⚠️  ALERT: Granule '{granule}' has {low_count} low-tile ingestions (< {LOW_TILE_THRESHOLD}).\n")


if __name__ == "__main__":
    poll_solr()
