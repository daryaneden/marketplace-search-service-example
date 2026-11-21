import asyncio
import logging
from dataclasses import asdict
from io import StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from httpx import AsyncClient, MockTransport, Request, Response

from bin import consumer as worker
from src.application.services.kafka_ads_consumer import KafkaAdsConsumer
from src.application.usecases.index_ad import IndexAd
from src.application.usecases.remove_ad import RemoveAd
from src.infrastructure.http.ad_client import AdServiceAdSource
from src.trace_context import (
    TRACE_HEADER,
    configure_trace_logging,
    get_trace_id,
    reset_trace_id,
    set_trace_id,
)
from tests.conftest import make_snapshot

TRACE_A = "550e8400-e29b-41d4-a716-446655440000"
TRACE_B = "8b738d87-25b4-43bd-9d6b-bbb2c07939ad"
OUTER_TRACE = "ee777944-bb03-4db9-bc67-277b4d7f4973"


def event(name, ad_id, **metadata):
    return {"event": name, "payload": {"ad_id": ad_id, **metadata}}


class FakeKafkaConsumer:
    def __init__(self, *values, commit_error=None):
        self.values = values
        self.commit_error = commit_error
        self.offset = None
        self.fetch_contexts = []
        self.commits = []

    async def __aiter__(self):
        for offset, value in enumerate(self.values):
            self.fetch_contexts.append((asyncio.current_task(), get_trace_id()))
            self.offset = offset
            yield SimpleNamespace(value=value, topic="ads", partition=0, offset=offset)
        self.fetch_contexts.append((asyncio.current_task(), get_trace_id()))

    async def commit(self):
        self.commits.append((self.offset, get_trace_id()))
        if self.commit_error is not None:
            raise self.commit_error


@pytest.fixture
def application_logger(monkeypatch):
    logger = logging.getLogger("src")
    monkeypatch.setattr(logger, "handlers", [])
    monkeypatch.setattr(logger, "level", logging.NOTSET)
    monkeypatch.setattr(logger, "propagate", True)
    return logger


@pytest.fixture
def log_output(application_logger):
    configure_trace_logging()
    stream = StringIO()
    application_logger.handlers[0].setStream(stream)
    return stream


def assert_log(stream, level, trace_id):
    lines = stream.getvalue().splitlines()
    assert any(
        line.startswith(f"{level} ") and f"trace_id={trace_id} " in line
        for line in lines
    ), lines


def assert_uuid4(trace_id):
    assert trace_id is not None, "Expected an active trace_id for this message"
    assert UUID(trace_id).version == 4
    assert str(UUID(trace_id)) == trace_id


@pytest.mark.parametrize("name", ["ad.created", "ad.updated"])
async def test_index_events_use_message_context_for_handler_and_http(
    name, fake_uow, log_output
):
    kafka = FakeKafkaConsumer(event(name, 7, trace_id=TRACE_A))
    requests = []
    handler_contexts = []

    async def respond(request: Request) -> Response:
        requests.append((request.url.path, request.headers.get_list(TRACE_HEADER)))
        return Response(200, json={"id": 7, **asdict(make_snapshot(ad_id=7))})

    async with AsyncClient(transport=MockTransport(respond)) as client:
        index = IndexAd(fake_uow, AdServiceAdSource(client, "http://ad"))

        async def handle(ad_id):
            handler_contexts.append(get_trace_id())
            await index.execute(ad_id)

        consumer = KafkaAdsConsumer(kafka, SimpleNamespace(execute=handle), Mock())
        token = set_trace_id(OUTER_TRACE)
        try:
            await consumer.run()
            assert get_trace_id() == OUTER_TRACE
        finally:
            reset_trace_id(token)

    assert 7 in fake_uow.search.snapshot()
    assert requests == [("/internal/ads/7", [TRACE_A])]
    assert handler_contexts == [TRACE_A]
    assert kafka.commits == [(0, TRACE_A)]
    assert_log(log_output, "INFO", TRACE_A)


async def test_delete_has_message_context_and_log_without_http(fake_uow, log_output):
    await fake_uow.search.upsert(7, "old", "description", 10, "test", "city")
    kafka = FakeKafkaConsumer(
        event("ad.deleted", 7, trace_id=TRACE_A),
        event("ad.deleted", 7, trace_id=TRACE_A),
    )
    requests = []
    removed = []

    async def respond(request: Request) -> Response:
        requests.append(request.url.path)
        return Response(404)

    async with AsyncClient(transport=MockTransport(respond)) as client:
        index = IndexAd(fake_uow, AdServiceAdSource(client, "http://ad"))
        remove = RemoveAd(fake_uow)

        async def handle(ad_id):
            removed.append((ad_id, get_trace_id()))
            await remove.execute(ad_id)

        await KafkaAdsConsumer(kafka, index, SimpleNamespace(execute=handle)).run()

    assert requests == []
    assert fake_uow.search.snapshot() == {}
    assert [ad_id for ad_id, _ in removed] == [7, 7]
    assert removed == [(7, TRACE_A), (7, TRACE_A)]
    assert kafka.commits == [(0, TRACE_A), (1, TRACE_A)]
    assert_log(log_output, "INFO", TRACE_A)


async def test_successive_events_do_not_leak_context_in_same_task(log_output):
    kafka = FakeKafkaConsumer(
        event("ad.created", 7, trace_id=TRACE_A),
        event("ad.updated", 8, trace_id=TRACE_B),
    )
    task = asyncio.current_task()
    seen = []

    async def index(ad_id):
        seen.append((ad_id, asyncio.current_task(), get_trace_id()))

    token = set_trace_id(OUTER_TRACE)
    try:
        await KafkaAdsConsumer(kafka, SimpleNamespace(execute=index), Mock()).run()
        assert get_trace_id() == OUTER_TRACE
    finally:
        reset_trace_id(token)

    assert kafka.fetch_contexts == [(task, OUTER_TRACE)] * 3
    assert seen == [(7, task, TRACE_A), (8, task, TRACE_B)]
    assert kafka.commits == [(0, TRACE_A), (1, TRACE_B)]
    assert_log(log_output, "INFO", TRACE_A)
    assert_log(log_output, "INFO", TRACE_B)


async def test_handler_error_resets_context_before_legacy_event(log_output):
    kafka = FakeKafkaConsumer(
        event("ad.created", 7, trace_id=TRACE_A, private="raw-event-probe"),
        event("ad.updated", 8),
    )
    task = asyncio.current_task()
    seen = []

    async def index(ad_id):
        seen.append((ad_id, asyncio.current_task(), get_trace_id()))
        if ad_id == 7:
            raise RuntimeError("index failed")

    token = set_trace_id(OUTER_TRACE)
    try:
        await KafkaAdsConsumer(kafka, SimpleNamespace(execute=index), Mock()).run()
        assert get_trace_id() == OUTER_TRACE
    finally:
        reset_trace_id(token)

    assert kafka.fetch_contexts == [(task, OUTER_TRACE)] * 3
    assert [(ad_id, current) for ad_id, current, _ in seen] == [(7, task), (8, task)]
    assert [offset for offset, _ in kafka.commits] == [1]
    legacy_id = seen[1][2]
    assert_uuid4(legacy_id)
    assert legacy_id not in {TRACE_A, OUTER_TRACE}
    assert seen[0][2] == TRACE_A
    assert kafka.commits == [(1, legacy_id)]
    assert_log(log_output, "ERROR", TRACE_A)
    assert_log(log_output, "INFO", legacy_id)
    assert "raw-event-probe" not in log_output.getvalue()


async def test_commit_error_logs_in_event_context_and_propagates(log_output):
    failure = RuntimeError("commit failed")
    kafka = FakeKafkaConsumer(
        event("ad.created", 7, trace_id=TRACE_A),
        event("ad.updated", 8, trace_id=TRACE_B),
        commit_error=failure,
    )
    index = AsyncMock()
    token = set_trace_id(OUTER_TRACE)
    try:
        with pytest.raises(RuntimeError, match="commit failed") as error:
            await KafkaAdsConsumer(kafka, SimpleNamespace(execute=index), Mock()).run()
        assert error.value is failure
        assert get_trace_id() == OUTER_TRACE
    finally:
        reset_trace_id(token)

    index.assert_awaited_once_with(7)
    assert kafka.commits == [(0, TRACE_A)]
    assert_log(log_output, "ERROR", TRACE_A)


async def test_cancellation_during_commit_restores_context(monkeypatch):
    kafka = FakeKafkaConsumer(
        event("ad.created", 7, trace_id=TRACE_A),
        event("ad.updated", 8, trace_id=TRACE_B),
    )
    index = AsyncMock()
    entered_commit = asyncio.Event()
    seen = []

    async def commit():
        seen.append(get_trace_id())
        entered_commit.set()
        await asyncio.Future()

    async def run():
        token = set_trace_id(OUTER_TRACE)
        try:
            await KafkaAdsConsumer(kafka, SimpleNamespace(execute=index), Mock()).run()
        finally:
            seen.append(get_trace_id())
            reset_trace_id(token)

    monkeypatch.setattr(kafka, "commit", commit)
    async with asyncio.timeout(5), asyncio.TaskGroup() as tasks:
        task = tasks.create_task(run())
        await entered_commit.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    index.assert_awaited_once_with(7)
    assert seen == [TRACE_A, OUTER_TRACE]


@pytest.mark.parametrize(
    "metadata",
    [{}, {"trace_id": "not-a-uuid"}, {"trace_id": TRACE_A.upper()}, {"trace_id": 123}],
    ids=["legacy", "malformed", "uppercase", "non-string"],
)
async def test_fallback_trace_is_new_for_each_delivery(metadata, log_output):
    value = event("ad.created", 7, **metadata)
    kafka = FakeKafkaConsumer(value, value)
    seen = []

    async def index(ad_id):
        seen.append((ad_id, get_trace_id()))

    await KafkaAdsConsumer(kafka, SimpleNamespace(execute=index), Mock()).run()

    assert [ad_id for ad_id, _ in seen] == [7, 7]
    assert [offset for offset, _ in kafka.commits] == [0, 1]
    assert get_trace_id() is None
    assert value == event("ad.created", 7, **metadata)
    first, second = (trace_id for _, trace_id in seen)
    for trace_id in (first, second):
        assert_uuid4(trace_id)
        assert_log(log_output, "INFO", trace_id)
    assert first != second
    assert kafka.commits == [(0, first), (1, second)]


async def test_repeated_valid_delivery_keeps_id_and_upserts_by_ad_id(
    fake_uow, fake_ad_source
):
    value = event("ad.created", 7, trace_id=TRACE_A)
    kafka = FakeKafkaConsumer(value, value)
    fake_ad_source.set(make_snapshot(ad_id=7, title="current title"))
    consumer = KafkaAdsConsumer(kafka, IndexAd(fake_uow, fake_ad_source), Mock())

    await consumer.run()

    assert fake_ad_source.calls == [7, 7]
    docs = fake_uow.search.snapshot()
    assert list(docs) == [7]
    assert docs[7].title == "current title"
    assert kafka.commits == [(0, TRACE_A), (1, TRACE_A)]


async def test_unknown_missing_id_and_malformed_events_keep_commit_policy(log_output):
    kafka = FakeKafkaConsumer(
        event("unknown.event", 7, trace_id=TRACE_A),
        {"event": "ad.updated", "payload": {"trace_id": TRACE_A}},
        {"event": "ad.created", "payload": ["unexpected"]},
        event("ad.deleted", 7, trace_id=TRACE_B),
    )
    index = AsyncMock()
    remove = AsyncMock()

    await KafkaAdsConsumer(
        kafka, SimpleNamespace(execute=index), SimpleNamespace(execute=remove)
    ).run()

    index.assert_not_awaited()
    remove.assert_awaited_once_with(7)
    assert [offset for offset, _ in kafka.commits] == [0, 1, 3]
    lines = log_output.getvalue().splitlines()
    assert sum(line.startswith("WARNING ") for line in lines) == 2
    assert sum(line.startswith("ERROR ") for line in lines) == 1


def test_logging_outside_message_has_neutral_id(log_output):
    logging.getLogger("src.consumer_test").info("outside message")

    assert_log(log_output, "INFO", "-")


async def test_worker_initializes_one_handler_without_changing_root(
    monkeypatch, application_logger
):
    root = logging.getLogger()
    stream = StringIO()
    observed_handlers = []

    async def run():
        handlers = tuple(application_logger.handlers)
        observed_handlers.append(handlers)
        if handlers:
            handlers[0].setStream(stream)
        logging.getLogger("src.consumer_test").info("worker outside message")

    kafka = AsyncMock()
    engine = AsyncMock()
    kafka_factory = Mock(return_value=kafka)
    monkeypatch.setattr(worker, "AIOKafkaConsumer", kafka_factory)
    monkeypatch.setattr(worker, "create_engine", lambda settings: engine)
    monkeypatch.setattr(worker, "create_session_factory", Mock())
    monkeypatch.setattr(worker, "KafkaAdsConsumer", Mock(return_value=Mock(run=run)))

    with monkeypatch.context() as patch:
        # A quiet root makes a call to basicConfig observable under pytest.
        patch.setattr(root, "handlers", [])
        patch.setattr(root, "level", logging.WARNING)
        await worker.main()
        await worker.main()
        root_state = (tuple(root.handlers), root.level)

    assert kafka.start.await_count == kafka.stop.await_count == 2
    assert engine.dispose.await_count == 2
    options = kafka_factory.call_args.kwargs
    assert options["enable_auto_commit"] is False
    assert options["value_deserializer"](b'{"event":"ad.created"}') == {
        "event": "ad.created"
    }
    first, second = observed_handlers
    assert len(first) == 1, "Worker startup must configure the application handler"
    assert second == first
    assert root_state == ((), logging.WARNING)
    assert stream.getvalue().count("trace_id=- worker outside message") == 2
