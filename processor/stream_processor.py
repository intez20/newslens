"""NewsLens PyFlink streaming job.

Consumes articles from ``raw-news``, deduplicates via RocksDB keyed state,
filters short articles, and routes to ``tech-news`` / ``finance-news`` /
``world-news`` based on the article's section field.

Run:
    flink run -pym processor -pyfs /opt/flink/jobs/
or:
    python -m processor
"""

import json
import logging

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

from processor.config import ProcessorConfig
from processor.dedup_filter import DeduplicateAndFilterFunction
from processor.section_router import route_section

logger = logging.getLogger(__name__)


# ── Kafka helpers ─────────────────────────────────────────────────
def _build_kafka_source(config: ProcessorConfig) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.topic_raw)
        .set_group_id("newslens-processor")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def _build_kafka_sink(config: ProcessorConfig, topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


# ── Job definition ────────────────────────────────────────────────
def build_job(env: StreamExecutionEnvironment, config: ProcessorConfig) -> None:
    """Wire source → dedup/filter → section routing → sinks."""

    # Source — raw-news Kafka topic
    source = _build_kafka_source(config)
    raw_stream = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "raw-news-source"
    )

    # Parse JSON string → Python dict
    parsed = raw_stream.map(lambda s: json.loads(s))

    # Key by article_id → dedup + word-count filter
    filtered = (
        parsed.key_by(lambda e: e["article_id"])
        .process(DeduplicateAndFilterFunction())
    )

    # Route by section → 3 downstream topics
    tech = filtered.filter(
        lambda e: route_section(e["section"]) == "tech-news"
    )
    finance = filtered.filter(
        lambda e: route_section(e["section"]) == "finance-news"
    )
    world = filtered.filter(
        lambda e: route_section(e["section"]) == "world-news"
    )

    # Serialize dict → JSON string (output_type tells PyFlink to convert
    # the pickled Python str back to a Java String for the Kafka sink).
    tech.map(json.dumps, output_type=Types.STRING()).sink_to(_build_kafka_sink(config, config.topic_tech))
    finance.map(json.dumps, output_type=Types.STRING()).sink_to(_build_kafka_sink(config, config.topic_finance))
    world.map(json.dumps, output_type=Types.STRING()).sink_to(_build_kafka_sink(config, config.topic_world))


def main() -> None:
    config = ProcessorConfig.from_env()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(config.parallelism)

    # Checkpointing (exactly-once with RocksDB state backend)
    env.enable_checkpointing(config.checkpoint_interval_ms)
    cp = env.get_checkpoint_config()
    cp.set_min_pause_between_checkpoints(10_000)
    cp.set_checkpoint_timeout(120_000)

    build_job(env, config)

    logger.info("Starting NewsLens stream processor")
    env.execute("newslens-stream-processor")
