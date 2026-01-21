from __future__ import annotations

import asyncio
import signal
import sys

from loguru import logger

from src.config import load_settings
from src.consumers.rabbitmq_consumer import RabbitMQConsumer


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    def _stop() -> None:
        if not stop_event.is_set():
            logger.info("shutdown requested")
            stop_event.set()

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                pass
    except RuntimeError:
        pass


async def _amain() -> int:
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    settings = load_settings()
    if not settings.analytics_consumer_enabled:
        print(
            "Analytics consumer disabled (set ANALYTICS_CONSUMER_ENABLED=yes to enable)."
        )
        return 0

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    consumer = RabbitMQConsumer.from_env()
    await consumer.run(stop_event)
    return 0


def main() -> int:
    try:
        return asyncio.run(_amain())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
