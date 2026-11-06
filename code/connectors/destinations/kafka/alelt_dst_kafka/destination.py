"""Kafka destination (Apache Kafka, Redpanda, any Kafka-API broker supporting transactions). Client: confluent-kafka.

URL (ALELT_DEST_URL):
  kafka://broker1:9092,broker2:9092?topic_prefix=alelt&partitions=3&replication=1
       &security_protocol=SASL_SSL&sasl_mechanism=PLAIN&sasl_username=${U}&sasl_password=${P}
  `opt.<librdkafka key>=value` is passed through. `transactions=0` (brokers without Kafka transactions, e.g. some
  managed Kafka-API services) degrades to at-least-once: consumers must dedupe by (key, seq).

Topics (created when missing):
  <prefix>.<schema>.<table>       change messages, key = JSON of the primary key (keyless: profile id column),
                                  cleanup.policy=compact by default (latest state per key)
  <prefix>.<control>.checkpoint   compacted, 1 partition, key = stream, value = {seq, position, batch_id, at}
  <prefix>.<control>.audit        one record per applied batch

Message value (JSON, UTF-8):
  {"op": "u"|"d"|"truncate", "table": "<schema>.<table>", "seq": n, "position": "<source pos>",
   "batch_id": "...", "partial": bool, "row": {col: value, <synced>: ts, <deleted>: bool}}
  Values: decimals and big ints as strings, dates/times ISO-8601, bytes hex, JSON as nested JSON.
  Hard delete mode: a delete is a tombstone (value = null). A truncate is a record with key "__alelt_truncate__"
  written to EVERY partition of the topic (value carries "partition"/"partitions"): a consumer reading partitions
  independently sees it before that partition's later rows; with compaction each partition keeps its last one.

Exactly-once: one Kafka transaction per batch = all change messages + checkpoint record (incl. per-table
watermarks) + audit record; transactional.id = <prefix>-<stream> (fences zombie producers). Compare-and-set (F-05):
after begin_transaction the checkpoint is re-read (read_committed, by OFFSET of the 1-partition control topic -
never by producer timestamp) and must equal the state the batch was planned from; a producer fenced by a newer
writer cannot commit (ProducerFenced -> CheckpointConflictError, producer evicted). Crash mid-apply =>
transaction aborted (or timed out) => read_committed consumers never see it. Replay of an applied batch = no-op.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, time as dtime
from decimal import Decimal
from typing import Any

from alelt_common.models import ColumnInfo, Position, TableInfo
from alelt_common.spi import ApplyStats, CheckpointConflictError
from alelt_dst_kit import ControlState, KitDestination, TablePlan
from alelt_dst_kit.base import AlreadyApplied

log = logging.getLogger("alelt_dst_kafka")
TRUNCATE_KEY = "__alelt_truncate__"


def jsonable(v: Any) -> Any:
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, int) and abs(v) > 2 ** 53:
        return str(v)
    if isinstance(v, (datetime, date, dtime)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    return v


def dumps(v: Any) -> bytes:
    return json.dumps(v, default=jsonable, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------------------- bus abstraction
class Bus:
    """Minimal broker interface (real: ConfluentBus, tests: MemoryBus)."""

    def ensure_topic(self, name: str, compact: bool, partitions: int) -> None: ...
    def producer(self, transactional_id: str) -> Any: ...
    def read(self, topic: str) -> list[tuple[bytes | None, bytes | None]]: ...

    def read_since(self, topic: str, start: dict[int, int]
                   ) -> tuple[list[tuple[bytes | None, bytes | None]], dict[int, int]]:
        """Records after the given next-offsets per partition (missing partition = from the beginning) and the new
        next-offsets. Default: full read."""
        return self.read(topic), {}


class ConfluentBus(Bus):
    def __init__(self, conf: dict[str, Any], replication: int = 1, timeout_s: float = 30.0,
                 transactions: bool = True) -> None:
        self.conf, self.replication, self.timeout, self.transactions = conf, replication, timeout_s, transactions
        self._producers: dict[str, Any] = {}
        self._known: set[str] = set()

    def ensure_topic(self, name: str, compact: bool, partitions: int) -> None:
        if name in self._known:
            return
        from confluent_kafka.admin import AdminClient, NewTopic

        adm = AdminClient(self.conf)
        md = adm.list_topics(timeout=self.timeout)
        if name not in md.topics:
            cfg = {"cleanup.policy": "compact"} if compact else {}
            fs = adm.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=self.replication,
                                             config=cfg)], operation_timeout=self.timeout)
            for f in fs.values():
                try:
                    f.result(self.timeout)
                except Exception as e:  # noqa: BLE001
                    if "TOPIC_ALREADY_EXISTS" not in str(e):
                        raise
        self._known.add(name)

    def producer(self, transactional_id: str) -> Any:
        p = self._producers.get(transactional_id)
        if p is None:
            from confluent_kafka import Producer

            conf = {**self.conf, "enable.idempotence": True, "acks": "all", "linger.ms": 20,
                    "compression.type": "zstd", "message.timeout.ms": int(self.timeout * 1000)}
            if self.transactions:
                conf.update({"transactional.id": transactional_id,
                             "transaction.timeout.ms": max(int(self.timeout * 2000), 60000)})
            p = _ProducerWrap(Producer(conf), self.timeout, self.transactions)
            p.init()
            self._producers[transactional_id] = p
        return p

    def evict(self, transactional_id: str) -> None:
        """Drop a fenced / fatally failed producer (the next batch re-inits it and fences the other writer)."""
        self._producers.pop(transactional_id, None)

    def read(self, topic: str) -> list[tuple[bytes | None, bytes | None]]:
        return self.read_since(topic, {})[0]

    def read_since(self, topic: str, start: dict[int, int]
                   ) -> tuple[list[tuple[bytes | None, bytes | None]], dict[int, int]]:
        from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

        c = Consumer({**self.conf, "group.id": f"alelt-reader-{uuid.uuid4().hex[:8]}", "enable.auto.commit": False,
                      "isolation.level": "read_committed", "enable.partition.eof": True,
                      "auto.offset.reset": "earliest"})
        try:
            md = c.list_topics(topic, timeout=self.timeout)
            parts = list((md.topics.get(topic).partitions or {}).keys()) if md.topics.get(topic) else []
            if not parts:
                return [], dict(start)
            c.assign([TopicPartition(topic, p, start.get(p, OFFSET_BEGINNING)) for p in parts])
            nxt = dict(start)
            done: set[int] = set()
            out: list[tuple[int, int, bytes | None, bytes | None]] = []
            idle = 0
            while len(done) < len(parts) and idle < 50:
                m = c.poll(0.2)
                if m is None:
                    idle += 1
                    continue
                idle = 0
                if m.error():
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        done.add(m.partition())
                        continue
                    raise RuntimeError(str(m.error()))
                out.append((m.partition(), m.offset(), m.key(), m.value()))
                nxt[m.partition()] = max(nxt.get(m.partition(), 0), m.offset() + 1)
            if len(done) < len(parts):
                raise TimeoutError(f"reading {topic}: no EOF on {set(parts) - done}")
            out.sort(key=lambda x: (x[0], x[1]))     # log order per partition (DST-kafka-01: never CreateTime)
            return [(k, v) for _, _, k, v in out], nxt
        finally:
            c.close()


class _ProducerWrap:
    def __init__(self, p: Any, timeout: float, transactions: bool) -> None:
        self.p, self.timeout, self.tx = p, timeout, transactions

    def init(self) -> None:
        if self.tx:
            self.p.init_transactions(self.timeout)

    def begin(self) -> None:
        if self.tx:
            self.p.begin_transaction()

    def produce(self, topic: str, key: bytes | None, value: bytes | None, partition: int | None = None) -> None:
        while True:
            try:
                if partition is None:
                    self.p.produce(topic, key=key, value=value)
                else:
                    self.p.produce(topic, key=key, value=value, partition=partition)
                return
            except BufferError:
                self.p.poll(0.5)

    def commit(self) -> None:
        if self.tx:
            self.p.commit_transaction(self.timeout)
        elif self.p.flush(self.timeout):
            raise TimeoutError("kafka flush timed out")

    @staticmethod
    def fenced(e: BaseException) -> bool:
        err = e.args[0] if getattr(e, "args", None) else None
        name = str(getattr(err, "name", lambda: "")() if callable(getattr(err, "name", None)) else "")
        fatal = bool(getattr(err, "fatal", lambda: False)()) if callable(getattr(err, "fatal", None)) else False
        return fatal or "FENCED" in name.upper() or "fenced" in str(e).lower()

    def abort(self) -> None:
        if self.tx:
            try:
                self.p.abort_transaction(self.timeout)
            except Exception:  # noqa: BLE001 - fenced / already aborted
                log.exception("abort_transaction failed")


class MemoryBus(Bus):
    """In-process broker with transaction semantics (unit tests)."""

    def __init__(self) -> None:
        self.topics: dict[str, list[tuple[bytes | None, bytes | None]]] = {}
        self.epochs: dict[str, int] = {}
        self.partitions_seen: dict[str, list[int | None]] = {}

    def ensure_topic(self, name: str, compact: bool, partitions: int) -> None:
        self.topics.setdefault(name, [])

    def evict(self, transactional_id: str) -> None:
        pass

    def producer(self, transactional_id: str) -> Any:
        """Each call = init_transactions: bumps the epoch, fencing earlier producers of the same id."""
        bus = self
        bus.epochs[transactional_id] = epoch = bus.epochs.get(transactional_id, 0) + 1

        class P:
            buf: list[tuple[str, bytes | None, bytes | None]] = []

            def begin(self) -> None:
                self.buf = []

            def produce(self, topic: str, key: bytes | None, value: bytes | None,
                        partition: int | None = None) -> None:
                self.buf.append((topic, key, value))
                bus.partitions_seen.setdefault(topic, []).append(partition)

            def commit(self) -> None:
                if bus.epochs[transactional_id] != epoch:
                    self.buf = []
                    raise RuntimeError("ProducerFenced: a newer producer with the same transactional.id exists")
                for t, k, v in self.buf:
                    bus.topics.setdefault(t, []).append((k, v))
                self.buf = []

            def abort(self) -> None:
                self.buf = []
        return P()

    def read(self, topic: str) -> list[tuple[bytes | None, bytes | None]]:
        return list(self.topics.get(topic, []))

    def read_since(self, topic: str, start: dict[int, int]
                   ) -> tuple[list[tuple[bytes | None, bytes | None]], dict[int, int]]:
        recs = self.topics.get(topic, [])
        self.reads = getattr(self, "reads", 0) + len(recs) - start.get(0, 0)
        return list(recs[start.get(0, 0):]), {0: len(recs)}


# ---------------------------------------------------------------------------- destination
class KafkaDestination(KitDestination):
    keeps_timestamp_text = True          # stores the delivered text (XC-31)
    engine = "kafka"
    container_forbidden = r"[^A-Za-z0-9._-]"        # topic name characters (DST-loader-05)

    def __init__(self, url: str = "", schema: str = "landing", control_schema: str = "alelt_control",
                 output_profile: str | None = None, compare: Any = None, bus: Bus | None = None, **kw: Any) -> None:
        super().__init__(url, schema, control_schema, output_profile, compare, **kw)
        u = self.u
        self.prefix = u.get("topic_prefix") or "alelt"
        self.partitions = int(u.get("partitions") or 1)
        self.compact = (u.get("cleanup_policy") or "compact") == "compact"
        self._bus = bus
        self._info: dict[str, TableInfo] = {}
        self._ck_offsets: dict[int, int] = {}
        self._ck_states: dict[str, ControlState] = {}

    @property
    def bus(self) -> Bus:
        if self._bus is None:
            u = self.u
            conf: dict[str, Any] = {"bootstrap.servers": u.hosts, "socket.timeout.ms": 30000}
            for k, ck in (("security_protocol", "security.protocol"), ("sasl_mechanism", "sasl.mechanisms"),
                          ("sasl_username", "sasl.username"), ("sasl_password", "sasl.password"),
                          ("client_id", "client.id")):
                if u.get(k):
                    conf[ck] = u.get(k)
            conf.update({k[4:]: v for k, v in u.params.items() if k.startswith("opt.")})
            self._bus = ConfluentBus(conf, int(u.get("replication") or 1), float(u.get("timeout") or 30),
                                     u.flag("transactions", True))
        return self._bus

    def topic(self, dest: str) -> str:
        return f"{self.prefix}.{self.schema}.{self.ident(dest)}"

    def ctopic(self, name: str) -> str:
        return f"{self.prefix}.{self.control}.{name}"

    # ------------------------------------------------------------------ DDL
    def bootstrap(self) -> None:
        self.bus.ensure_topic(self.ctopic("checkpoint"), True, 1)
        self.bus.ensure_topic(self.ctopic("audit"), False, 1)

    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        self._info[self.ident(dest_name)] = table
        self.bus.ensure_topic(self.topic(dest_name), self.compact, self.partitions)

    def add_column(self, dest_name: str, col: ColumnInfo) -> None:
        """Schemaless JSON: new columns simply appear in later messages."""

    # ------------------------------------------------------------------ control
    def control_state(self, stream: str) -> ControlState:
        """Checkpoint of `stream`: incremental read of the 1-partition compacted control topic from the last offset
        seen by this process (DST-kafka-02) - never a full-topic scan per batch after the first call."""
        recs, self._ck_offsets = self.bus.read_since(self.ctopic("checkpoint"), self._ck_offsets)
        for k, v in recs:
            if k is None:
                continue
            name = k.decode()
            if v is None:
                self._ck_states.pop(name, None)             # tombstone: delete_checkpoint
            else:
                d = json.loads(v)
                self._ck_states[name] = ControlState(d["position"], int(d["seq"]), d.get("batch_id"),
                                                     dict(d.get("watermarks") or {}))
        st = self._ck_states.get(stream)
        return ControlState(st.position, st.seq, st.batch_id, dict(st.watermarks)) if st else ControlState()

    def delete_checkpoint(self, stream: str) -> None:
        """Tombstone on the compacted checkpoint topic (transient replay-pass streams, HC-21)."""
        p = self.bus.producer(f"{self.prefix}-{stream}")
        p.begin()
        try:
            p.produce(self.ctopic("checkpoint"), stream.encode(), None)
            p.commit()
        except BaseException:
            p.abort()
            raise

    # ------------------------------------------------------------------ apply
    def _key(self, pl: TablePlan, key: tuple[Any, ...]) -> bytes:
        return dumps({self.ident(k): v for k, v in zip(pl.keys, key)})

    def commit_batch(self, stream: str | None, plans: list[TablePlan], to_pos: Position | None, batch_id: str,
                     prev: ControlState, ts: datetime, stats: dict[str, ApplyStats]) -> None:
        seq = prev.seq + 1
        txid = (f"{self.prefix}-{stream}" if stream else             # snapshot: one id per table (DST-kafka-03)
                f"{self.prefix}-snapshot-{self.schema}-{'-'.join(sorted(self.ident(x.dest) for x in plans))}")
        p = self.bus.producer(txid)
        p.begin()
        try:
            if stream is not None and to_pos is not None:        # CAS as the (only unfenced) producer
                self.cas(stream, prev, self.control_state(stream), to_pos)
            for pl in plans:
                topic = self.topic(pl.dest)
                self.bus.ensure_topic(topic, self.compact, self.partitions)
                # snapshot rows carry seq 0: below every stream batch (a later truncate covers them)
                base = {"table": f"{self.schema}.{self.ident(pl.dest)}", "seq": seq if stream is not None else 0,
                        "batch_id": batch_id}
                if pl.truncate:                    # one marker PER PARTITION (DST-kafka-05): every partition
                    for part in range(self.partitions):  # orders it before its own later rows
                        p.produce(topic, TRUNCATE_KEY.encode(), dumps({**base, "op": "truncate", "partition": part,
                                                                       "partitions": self.partitions,
                                                                       "position": pl.truncate_position,
                                                                       "row": {self.c_synced: ts}}),
                                  partition=part if self.partitions > 1 else None)
                for op in pl.ops.values():
                    key = self._key(pl, op.key)
                    if op.op == "D" and self.hard:
                        p.produce(topic, key, None)                        # tombstone
                        continue
                    row = self.phys_row(pl, op, ts, full=False)
                    p.produce(topic, key, dumps({**base, "op": "d" if op.op == "D" else "u",
                                                 "position": op.position, "partial": op.partial, "row": row}))
                self.count(pl, stats[pl.dest])
            self._hook("after_data")
            if stream is not None and to_pos is not None:
                p.produce(self.ctopic("checkpoint"), stream.encode(),
                          dumps({"stream": stream, "seq": seq, "position": to_pos.text, "batch_id": batch_id,
                                 "at": ts, "watermarks": self.batch_wm}))
                p.produce(self.ctopic("audit"), stream.encode(),
                          dumps(self.audit_record(stream, prev, to_pos, batch_id, stats)))
            self._hook("before_commit")
            p.commit()                                                       # COMMIT POINT
        except BaseException as e:
            p.abort()
            if stream is not None and to_pos is not None and _ProducerWrap.fenced(e):
                evict = getattr(self.bus, "evict", None)
                if evict:
                    evict(txid)
                found = self.control_state(stream)
                if self.pos_ge(found.position, to_pos):
                    raise AlreadyApplied from e
                raise CheckpointConflictError(stream, prev.position, found.position,
                                              f"producer fenced by a newer writer: {e}") from e
            raise

    # ------------------------------------------------------------------ verify (consumer-side fold)
    def fetch_rows(self, dest_name: str) -> list[dict[str, Any]]:
        """Fold the topic (read_committed) into current rows; partial images merge over the previous version."""
        state: dict[bytes, dict[str, Any]] = {}
        seqs: dict[bytes, int] = {}
        for k, v in self.bus.read(self.topic(dest_name)):
            if k is None:
                continue
            if v is None:
                state.pop(k, None)
                continue
            msg = json.loads(v)
            if msg["op"] == "truncate":
                # one marker per partition (DST-kafka-05); partitions are read one after the other, so the
                # truncate of batch S affects rows last written by an EARLIER batch only (order-independent)
                cut = int(msg.get("seq") or 0)
                for rk in [x for x in state if seqs.get(x, 0) < cut]:
                    if self.hard:
                        state.pop(rk)
                    elif not state[rk][self.c_deleted]:
                        state[rk][self.c_deleted] = True
                        state[rk][self.c_synced] = msg["row"][self.c_synced]
                continue
            state[k] = {**state.get(k, {}), **msg["row"]}
            seqs[k] = int(msg.get("seq") or 0)
        return list(state.values())

    def aggregates(self, dest_name: str, columns: list[str]) -> dict[str, Any]:
        rows = [r for r in self.fetch_rows(dest_name) if not r.get(self.c_deleted)]
        out: dict[str, Any] = {"count": len(rows)}
        for c in columns:
            vals = [Decimal(str(r[self.ident(c)])) for r in rows if r.get(self.ident(c)) is not None]
            out[f"sum:{c}"] = sum(vals) if vals else None
            out[f"min:{c}"] = min(vals) if vals else None
            out[f"max:{c}"] = max(vals) if vals else None
        return out

    def audit_rows(self, stream: str) -> list[dict[str, Any]]:
        return [json.loads(v) for k, v in self.bus.read(self.ctopic("audit"))
                if k is not None and k.decode() == stream and v is not None]


__all__ = ["Bus", "ConfluentBus", "KafkaDestination", "MemoryBus"]
