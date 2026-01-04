import argparse
from subsets_utils import validate_environment

from ingest import events as ingest_events
from transforms.events import main as transform_events


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true")
    parser.add_argument("--transform-only", action="store_true")
    args = parser.parse_args()

    validate_environment()

    if not args.transform_only:
        print("\n--- Ingesting GitHub events ---")
        ingest_events.run()

    if not args.ingest_only:
        print("\n--- Transforming GitHub events ---")
        transform_events.run()

    print("\nDone!")


if __name__ == "__main__":
    main()
