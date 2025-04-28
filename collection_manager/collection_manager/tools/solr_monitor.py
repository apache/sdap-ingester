import requests
import time
import argparse
from collections import defaultdict, deque
import pika
import threading
import sys

SOLR_URL = "http://localhost:8983/solr/nexustiles/select"
ALERT_REPEAT_THRESHOLD = 3
HISTORY_BUFFER_SIZE = 5  # number of recent tile counts to keep per granule

granule_history = defaultdict(lambda: deque(maxlen=HISTORY_BUFFER_SIZE))
seen_records = set()
rabbitmq_polling_active = True
last_rmq_depth = None
current_rmq_depth = "..."

# RabbitMQ polling function
def get_rmq_queue_depth(host, username, password, queue_name="nexus"):
    try:
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host, credentials=credentials))
        channel = connection.channel()
        queue = channel.queue_declare(queue=queue_name, passive=True)
        count = queue.method.message_count
        connection.close()
        return count
    except Exception as e:
        return f"[RabbitMQ ERROR] {e}"

def poll_rabbitmq_periodically(host, username, password, interval=1):
    global last_rmq_depth, current_rmq_depth
    while True:
        if rabbitmq_polling_active:
            count = get_rmq_queue_depth(host, username, password)
            if count != last_rmq_depth:
                current_rmq_depth = count
                last_rmq_depth = count
        time.sleep(interval)

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
            continue

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

def watch_solr(filter_granule=None, interval=10, rabbit_host="localhost", rabbit_user="user", rabbit_pass="bitnami"):
    dot_states = ["", ".", "..", "..."]
    dot_index = 0

    threading.Thread(target=poll_rabbitmq_periodically, args=(rabbit_host, rabbit_user, rabbit_pass), daemon=True).start()

    #print("Polling Solr for granule info | 📦 RabbitMQ Queue Depth: ...")

    try:
        while True:
            dots = dot_states[dot_index % len(dot_states)]
            dot_index += 1

            sys.stdout.write(f"\rPolling Solr for granule info{dots} | 📦 RabbitMQ Queue Depth: {current_rmq_depth}   ")
            sys.stdout.flush()

            global rabbitmq_polling_active
            rabbitmq_polling_active = False
            new_results = poll_solr(filter_granule, show_summary=False, only_new=True)
            rabbitmq_polling_active = True

            if new_results:
                print("\n")

            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped watching Solr.")
        poll_solr(filter_granule, show_summary=True, only_new=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor Solr for ingestion summaries.")
    parser.add_argument("--granule", type=str, help="Optional granule name to filter by")
    parser.add_argument("--watch", action="store_true", help="Continuously watch Solr at intervals")
    parser.add_argument("--interval", type=int, default=10, help="Polling interval in seconds (default: 10)")
    parser.add_argument("--rabbitmq-host", type=str, default="localhost", help="RabbitMQ host")
    parser.add_argument("--rabbitmq-user", type=str, default="user", help="RabbitMQ username")
    parser.add_argument("--rabbitmq-pass", type=str, default="bitnami", help="RabbitMQ password")
    args = parser.parse_args()

    if args.watch:
        watch_solr(filter_granule=args.granule, interval=args.interval, rabbit_host=args.rabbitmq_host, rabbit_user=args.rabbitmq_user, rabbit_pass=args.rabbitmq_pass)
    else:
        poll_solr(filter_granule=args.granule)
