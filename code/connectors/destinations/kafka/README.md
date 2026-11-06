# alelt_dst_kafka — Kafka destination (Apache Kafka, Redpanda, Kafka-API brokers)

`alelt_dst_kafka.destination:KafkaDestination` (engine name `kafka`). Client: `confluent-kafka` (Apache-2.0, librdkafka).

| Area | Detail |
|---|---|
| URL (`ALELT_DEST_URL`) | `kafka://b1:9092,b2:9092?topic_prefix=alelt&partitions=3&replication=1&security_protocol=SASL_SSL&sasl_mechanism=PLAIN&sasl_username=${U}&sasl_password=${P}`; `opt.<librdkafka key>=value` is passed through. |
| Topics (created when missing) | `<prefix>.<schema>.<table>` change messages, key = JSON of the primary key (keyless: profile id column), `cleanup.policy=compact`; `<prefix>.<control>.checkpoint` (compacted, 1 partition, key = stream); `<prefix>.<control>.audit`. |
| Message value | JSON `{"op": "u" / "d" / "truncate", "table", "seq", "position", "batch_id", "partial", "row": {…, <synced>, <deleted>}}`; decimals and big ints as strings, dates ISO-8601, bytes hex, JSON nested. Hard delete mode: tombstone. Truncate: key `__alelt_truncate__`. |
| Exactly-once | one Kafka transaction per batch = all change messages + checkpoint record + audit record; `transactional.id = <prefix>-<stream>` fences zombie producers. Crash mid-apply ⇒ transaction aborted, invisible to `read_committed` consumers. Replay of an applied batch = no-op (checkpoint >= to_pos). Compare-and-set (F-05): after `begin_transaction` the checkpoint is re-read (read_committed, log order by offset - never producer timestamps) and must equal the planned base; a fenced producer is evicted and the batch rejected (`CheckpointConflictError`). Snapshot loads use one transactional id per table. |
| Limits | Consumers must use `isolation.level=read_committed`. `transactions=0` (brokers without transactions) degrades to at-least-once: consumers dedupe by (key, seq). |

Verified: conformance on the in-process MemoryBus and on a real broker (lab Redpanda, `lab up redpanda`: 22 passed incl. crash mid-apply).

Tests: `pytest connectors/destinations/kafka/tests`; lab: `ALELT_TEST_KAFKA_URL=kafka://127.0.0.1:47092?partitions=1&replication=1`.
