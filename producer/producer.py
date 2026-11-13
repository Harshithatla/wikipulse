"""WikiPulse producer: Wikimedia EventStreams (recentchange) -> Kafka.

Design notes
------------
* Delivery: idempotent producer (acks=all, enable.idempotence) so broker-side
  retries never create duplicates. Upstream reconnects *can* replay events,
  which is fine: every event carries a globally unique `meta.id` and the
  warehouse load de-duplicates on it.
* Resume: Wikimedia puts a JSON array of per-partition offsets in the SSE `id:`
  field. We persist the latest one and send it back as `Last-Event-ID`, so a
  restart continues where it stopped instead of losing or re-reading hours.
* Keying: key = "<wiki>:<title>" keeps all edits to a page in one Kafka
  partition (ordered per page, and co-partitioned for the storm detector).
* Validation is *not* done here. Anything that isn't obviously a canary or
  outside the wiki allowlist is forwarded raw; the Spark job owns the schema
  contract and routes bad records to the DLQ. One place for the rules.
* `--source synthetic` emits schema-identical fake traffic with injected edit
  storms, so the whole pipeline is demoable offline and tests are deterministic.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import string
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from sse import parse_sse

log = logging.getLogger("wikipulse.producer")

WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia's User-Agent policy requires contact info; requests without it may be blocked.
DEFAULT_UA = "WikiPulse/1.0 (data-engineering capstone; set WIKIPULSE_CONTACT)"


# --------------------------------------------------------------------------- filtering
def parse_allowlist(raw: str | None) -> frozenset[str]:
    return frozenset(w.strip() for w in (raw or "").split(",") if w.strip())


def route(raw: str, allow: frozenset[str]) -> tuple[bool, str | None]:
    """Decide whether to forward an event and compute its Kafka key.

    Returns (forward, key). Malformed JSON is forwarded with key=None so the
    stream processor can dead-letter it with a reason.
    """
    try:
        evt = json.loads(raw)
    except json.JSONDecodeError:
        return True, None
    if not isinstance(evt, dict):
        return True, None
    meta = evt.get("meta") or {}
    if meta.get("domain") == "canary":  # Wikimedia monitoring heartbeats
        return False, None
    wiki = evt.get("wiki")
    if allow and wiki not in allow:
        return False, None
    title = evt.get("title")
    key = f"{wiki}:{title}" if wiki and title else None
    return True, key


# --------------------------------------------------------------------------- sources
class OffsetStore:
    """Persists the last SSE id atomically (write tmp + rename)."""

    def __init__(self, path: str | None):
        self.path = Path(path) if path else None

    def load(self) -> str | None:
        if self.path and self.path.exists():
            v = self.path.read_text().strip()
            return v or None
        return None

    def save(self, value: str) -> None:
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(value)
        tmp.replace(self.path)


def wikimedia_source(store: OffsetStore, user_agent: str, stop) -> Iterator[str]:
    import requests  # imported lazily so synthetic mode / tests need no network deps

    backoff = 1.0
    while not stop():
        headers = {"User-Agent": user_agent, "Accept": "text/event-stream"}
        last_id = store.load()
        if last_id:
            headers["Last-Event-ID"] = last_id
        try:
            log.info("connecting to EventStreams (resume=%s)", bool(last_id))
            with requests.get(WIKIMEDIA_URL, headers=headers, stream=True, timeout=(10, 60)) as r:
                r.raise_for_status()
                backoff = 1.0
                lines = r.iter_lines(decode_unicode=True)
                for n, ev in enumerate(parse_sse(line or "" for line in lines)):
                    if stop():
                        return
                    if ev.event != "message":
                        continue
                    yield ev.data
                    if ev.id and n % 200 == 0:
                        store.save(ev.id)
        except Exception as exc:  # network blips are routine on a 24/7 stream
            log.warning("stream error: %s; reconnecting in %.0fs", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


WIKIS = {
    "enwiki": "en.wikipedia.org",
    "dewiki": "de.wikipedia.org",
    "frwiki": "fr.wikipedia.org",
    "eswiki": "es.wikipedia.org",
    "jawiki": "ja.wikipedia.org",
}
TOPICS = ["Solar eclipse", "Election results", "Champions League final", "Earthquake",
          "Nobel Prize in Physics", "Python (programming language)", "Apache Kafka",
          "Mount Everest", "Climate change", "Artificial intelligence", "Olympic Games",
          "Taylor Swift", "SpaceX Starship", "Bitcoin", "World Cup"]
COMMENTS = ["copyedit", "/* History */ expand", "Reverted edits by {u} to last version",
            "Undid revision {r} by {u}", "add ref", "fix typo", "update infobox", ""]


class Synthetic:
    """Deterministic (seeded) fake recentchange traffic with periodic edit storms."""

    def __init__(self, seed: int = 7, storm_every: int = 2000):
        self.rng = random.Random(seed)
        self.storm_every = storm_every
        self.n = 0
        self.offset = 0
        self.storm: tuple[str, str, int] | None = None  # (wiki, title, remaining)
        self.users = ["".join(self.rng.choices(string.ascii_letters, k=8)) for _ in range(500)]

    def _event(self, wiki: str, title: str, user: str, bot: bool) -> dict:
        r = self.rng
        old_len = r.randint(500, 90_000)
        rev = r.randint(10**9, 2 * 10**9)
        ts = int(time.time())
        etype = r.choices(["edit", "new", "log", "categorize"], [80, 5, 10, 5])[0]
        self.offset += 1
        return {
            "$schema": "/mediawiki/recentchange/1.0.0",
            "meta": {
                "uri": f"https://{WIKIS[wiki]}/wiki/{title.replace(' ', '_')}",
                "request_id": str(uuid.UUID(int=r.getrandbits(128))),
                "id": str(uuid.UUID(int=r.getrandbits(128))),
                "dt": datetime.fromtimestamp(ts, timezone.utc).isoformat().replace("+00:00", "Z"),
                "domain": WIKIS[wiki],
                "stream": "mediawiki.recentchange",
                "topic": "eqiad.mediawiki.recentchange",
                "partition": 0,
                "offset": self.offset,
            },
            "id": rev + 7,
            "type": etype,
            "namespace": 0,
            "title": title,
            "comment": r.choice(COMMENTS).format(u=user, r=rev),
            "timestamp": ts,
            "user": user,
            "bot": bot,
            "minor": r.random() < 0.3,
            "patrolled": r.random() < 0.5,
            "server_name": WIKIS[wiki],
            "wiki": wiki,
            "length": {"old": old_len, "new": old_len + r.randint(-800, 2500)},
            "revision": {"old": rev, "new": rev + 1},
        }

    def next(self) -> str:
        r = self.rng
        self.n += 1
        if self.storm is None and self.n % self.storm_every == 0:
            self.storm = (r.choice(list(WIKIS)), r.choice(TOPICS), r.randint(60, 150))
        if self.storm and r.random() < 0.35:
            wiki, title, left = self.storm
            self.storm = (wiki, title, left - 1) if left > 1 else None
            evt = self._event(wiki, title, r.choice(self.users[:60]), bot=False)
            evt["type"] = "edit"
        else:
            evt = self._event(r.choice(list(WIKIS)), f"{r.choice(TOPICS)} {r.randint(1, 4000)}",
                              r.choice(self.users), bot=r.random() < 0.2)
        if r.random() < 0.002:  # exercise the DLQ path
            return '{"broken": '
        return json.dumps(evt, ensure_ascii=False)


def synthetic_source(rate: float, stop, seed: int, storm_every: int) -> Iterator[str]:
    gen = Synthetic(seed=seed, storm_every=storm_every)
    interval = 1.0 / rate if rate > 0 else 0
    while not stop():
        yield gen.next()
        if interval:
            time.sleep(interval)


# --------------------------------------------------------------------------- kafka
def make_producer(bootstrap: str):
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "zstd",
        "linger.ms": 50,
        "batch.size": 256 * 1024,
        "client.id": "wikipulse-producer",
    })


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--source", choices=["wikimedia", "synthetic"],
                   default=os.getenv("WIKIPULSE_SOURCE", "wikimedia"))
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("RAW_TOPIC", "wiki.recentchange.raw"))
    p.add_argument("--allowlist", default=os.getenv("WIKI_ALLOWLIST", ""))
    p.add_argument("--offset-file", default=os.getenv("OFFSET_FILE", "/state/last_event_id"))
    p.add_argument("--rate", type=float, default=float(os.getenv("SYNTHETIC_RATE", "50")))
    p.add_argument("--seed", type=int, default=int(os.getenv("SYNTHETIC_SEED", "7")))
    p.add_argument("--storm-every", type=int, default=int(os.getenv("SYNTHETIC_STORM_EVERY", "6000")),
                   help="start a synthetic edit storm every N events")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    allow = parse_allowlist(args.allowlist)
    stopping = False

    def _stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    stop = lambda: stopping  # noqa: E731

    ua = f"{DEFAULT_UA} {os.getenv('WIKIPULSE_CONTACT', '')}".strip()
    source = (wikimedia_source(OffsetStore(args.offset_file), ua, stop)
              if args.source == "wikimedia" else synthetic_source(args.rate, stop, args.seed, args.storm_every))

    producer = make_producer(args.bootstrap)
    failures = 0

    def on_delivery(err, _msg):
        nonlocal failures
        if err is not None:
            failures += 1
            log.error("delivery failed: %s", err)

    sent = skipped = 0
    t0 = time.monotonic()
    for raw in source:
        forward, key = route(raw, allow)
        if not forward:
            skipped += 1
            continue
        while True:
            try:
                producer.produce(args.topic, value=raw.encode(), key=key.encode() if key else None,
                                 headers={"source": args.source}, on_delivery=on_delivery)
                break
            except BufferError:  # local queue full: apply backpressure
                producer.poll(0.5)
        producer.poll(0)
        sent += 1
        if time.monotonic() - t0 >= 30:
            log.info("sent=%d skipped=%d failed=%d (%.1f msg/s)", sent, skipped, failures,
                     sent / (time.monotonic() - t0))
            sent = skipped = 0
            t0 = time.monotonic()

    log.info("flushing...")
    producer.flush(30)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
