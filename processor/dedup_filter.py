"""Stateful deduplication and word-count filter for the stream processor.

The core filtering logic is in ``should_emit`` — a pure function testable
without a running Flink cluster.  ``DeduplicateAndFilterFunction`` wraps
it with Flink keyed state for production use.

PyFlink imports are deferred so that unit tests for ``should_emit`` can
run without the ``pyflink`` package installed locally.
"""

import logging

logger = logging.getLogger(__name__)

MIN_WORD_COUNT = 200


# ── Pure logic (unit-testable without pyflink) ────────────────────
def should_emit(body: str, min_words: int = MIN_WORD_COUNT) -> bool:
    """Return True if *body* has at least *min_words* words."""
    return len(body.split()) >= min_words


# ── Flink KeyedProcessFunction ───────────────────────────────────
def _build_dedup_function(min_words: int = MIN_WORD_COUNT):
    """Factory that imports pyflink at call time and returns the class."""
    from pyflink.common import Time, Types
    from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
    from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

    class DeduplicateAndFilterFunction(KeyedProcessFunction):
        """Drops duplicates (by article_id key) and short articles.

        State backend: RocksDB ``ValueState[bool]`` with 48-hour TTL.
        """

        def __init__(self):
            self._min_words = min_words
            self._seen = None  # initialised in open()

        def open(self, runtime_context: RuntimeContext):
            ttl_config = (
                StateTtlConfig.new_builder(Time.hours(48))
                .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build()
            )
            descriptor = ValueStateDescriptor("seen", Types.BOOLEAN())
            descriptor.enable_time_to_live(ttl_config)
            self._seen = runtime_context.get_state(descriptor)

        def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
            if self._seen.value():
                return  # duplicate — already processed

            self._seen.update(True)

            if not should_emit(value["body"], self._min_words):
                return  # body too short

            yield value

    return DeduplicateAndFilterFunction()


class DeduplicateAndFilterFunction:
    """Proxy that lazily builds the real Flink function on first use.

    Allows ``from processor.dedup_filter import DeduplicateAndFilterFunction``
    without requiring pyflink at import time.
    """

    def __new__(cls, min_words: int = MIN_WORD_COUNT):
        return _build_dedup_function(min_words)
