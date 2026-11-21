import logging
import typing
from uuid import uuid4

from aiokafka import AIOKafkaConsumer

from src.application.ports.usecases import IndexAdPort, RemoveAdPort
from src.trace_context import is_valid_trace_id, reset_trace_id, set_trace_id

logger = logging.getLogger(__name__)


def _message_trace_id(value: object) -> str:
    payload = value.get("payload") if isinstance(value, dict) else None
    if isinstance(payload, dict):
        candidate = payload.get("trace_id")
        if isinstance(candidate, str) and is_valid_trace_id(candidate):
            return candidate
    return str(uuid4())


class KafkaAdsConsumer:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        index_ad: IndexAdPort,
        remove_ad: RemoveAdPort,
    ) -> None:
        self._consumer = consumer
        self._index_ad = index_ad
        self._remove_ad = remove_ad

    async def run(self) -> None:
        async for msg in self._consumer:
            token = set_trace_id(_message_trace_id(msg.value))
            try:
                try:
                    await self._handle(msg.value)
                except Exception as exc:
                    logger.error(
                        "failed to handle message partition=%s offset=%s error=%s",
                        msg.partition,
                        msg.offset,
                        type(exc).__name__,
                    )
                    continue

                try:
                    await self._consumer.commit()
                except Exception as exc:
                    logger.error(
                        "failed to commit offset partition=%s offset=%s error=%s",
                        msg.partition,
                        msg.offset,
                        type(exc).__name__,
                    )
                    raise
                logger.info(
                    "processed message partition=%s offset=%s",
                    msg.partition,
                    msg.offset,
                )
            finally:
                reset_trace_id(token)

    async def _handle(self, value: dict[str, typing.Any]) -> None:
        event = value.get("event")
        payload = value.get("payload") or {}
        ad_id = payload.get("ad_id")
        if not isinstance(ad_id, int):
            logger.warning("skip message without ad_id")
            return

        if event in ("ad.created", "ad.updated"):
            await self._index_ad.execute(ad_id)
        elif event == "ad.deleted":
            await self._remove_ad.execute(ad_id)
        else:
            logger.warning("unknown event type")
