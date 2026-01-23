from __future__ import annotations

import asyncio
import os
import signal
import sys

from loguru import logger
import uvicorn

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


async def _serve_health(stop_event: asyncio.Event) -> None:
    enabled = (os.getenv("CONSUMER_HEALTH_ENABLED", "yes") or "").strip().lower()
    if enabled not in {"1", "true", "yes", "y", "on"}:
        return

    host = os.getenv("CONSUMER_HEALTH_HOST", "0.0.0.0")
    port = int(os.getenv("CONSUMER_HEALTH_PORT", "9100"))

    config = uvicorn.Config(
        "src.consumers.health_api:app",
        host=host,
        port=port,
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    try:
        await stop_event.wait()
    finally:
        server.should_exit = True
        await task


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
    health_task = asyncio.create_task(_serve_health(stop_event))
    try:
        await consumer.run(stop_event)
    finally:
        stop_event.set()
        await health_task
    return 0


def main() -> int:
    try:
        return asyncio.run(_amain())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
