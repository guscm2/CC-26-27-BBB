"""Process entry point used by run.sh."""

import argparse
import signal
import sys

from broker.broker import Broker
from config import load_broker_config
from protocol import valid_name


def main():
    parser = argparse.ArgumentParser(description="Environmental publish/subscribe system")
    parser.add_argument("role", choices=("broker", "publisher", "subscriber"))
    parser.add_argument("--id", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    if not valid_name(args.id):
        parser.error("--id must be nonempty and contain no whitespace/control characters")
    if args.role != "broker":
        parser.error(f"{args.role} is not implemented yet")
    try:
        broker = Broker(args.id, load_broker_config(args.config))
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda signum, frame: broker.stop())
        broker.run()
    except (OSError, ValueError) as exc:
        print(f"{args.role}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
