"""Process entry point used by run.sh."""

import argparse
import signal
import sys

from broker.broker import Broker
from config import load_broker_config, load_publisher_config, load_subscriber_config
from publisher.publisher import Publisher
from subscriber.subscriber import Subscriber
from protocol import valid_name


def main():
    parser = argparse.ArgumentParser(description="Environmental publish/subscribe system")
    parser.add_argument("role", choices=("broker", "publisher", "subscriber"))
    parser.add_argument("--id", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    if not valid_name(args.id):
        parser.error("--id must be nonempty and contain no whitespace/control characters")
    try:
        implementations = {
            "broker": (Broker, load_broker_config),
            "publisher": (Publisher, load_publisher_config),
            "subscriber": (Subscriber, load_subscriber_config),
        }
        implementation, load_config = implementations[args.role]
        process = implementation(args.id, load_config(args.config))
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda signum, frame: process.stop())
        process.run()
    except (OSError, ValueError) as exc:
        print(f"{args.role}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
