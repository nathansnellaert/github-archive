import gzip
import json
from datetime import datetime, timedelta
from collections import Counter
from subsets_utils import get, save_raw_json, load_state, save_state

def run():
    """Fetch GitHub event data from GH Archive - aggregate by hour for past 30 days."""
    state = load_state("github")
    processed_dates = set(state.get("processed_dates", []))

    end_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=30)

    # Load existing aggregates or start fresh
    hourly_counts = Counter()

    current = start_time
    dates_to_process = []

    # Collect dates that need processing
    while current < end_time:
        date_str = current.strftime("%Y-%m-%d")
        if date_str not in processed_dates:
            dates_to_process.append(date_str)
        current += timedelta(days=1)

    dates_to_process = list(set(dates_to_process))
    print(f"  Processing {len(dates_to_process)} days of GitHub data...")

    for date_str in sorted(dates_to_process):
        print(f"  Fetching {date_str}...")
        day_events = 0

        for hour in range(24):
            url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"

            try:
                response = get(url)
                if response.status_code == 200:
                    decompressed = gzip.decompress(response.content)
                    lines = decompressed.decode('utf-8').strip().split('\n')

                    hour_key = f"{date_str}T{hour:02d}:00:00Z"

                    for line in lines:
                        if line:
                            event = json.loads(line)
                            event_type = event.get("type", "Unknown")
                            hourly_counts[(hour_key, event_type)] += 1
                            day_events += 1
            except Exception as e:
                print(f"    Hour {hour} failed: {e}")

        print(f"    -> {day_events:,} events")

        # Mark date as processed
        processed_dates.add(date_str)
        save_state("github", {"processed_dates": list(processed_dates)})

    # Convert to list format
    records = [
        {"hour": hour, "event_type": event_type, "event_count": count}
        for (hour, event_type), count in sorted(hourly_counts.items())
    ]

    print(f"  Total: {len(records)} hourly event type records")
    save_raw_json(records, "github_hourly_aggregates")
