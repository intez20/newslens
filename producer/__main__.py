"""Entry point: python -m producer"""

import logging
import signal
import sys

from dotenv import load_dotenv

from producer.producer_scheduler import ProducerScheduler


def main() -> None:
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        stream=sys.stdout,
    )

    scheduler = ProducerScheduler()

    def _handle_signal(signum, frame):
        logging.getLogger(__name__).info("Received signal %s — shutting down", signum)
        scheduler.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.stop()


if __name__ == "__main__":
    main()
