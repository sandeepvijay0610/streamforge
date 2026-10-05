"""
StreamForge load generator.

Fires synthetic webhook events at the local ingestion API at a configured
rate. Uses ThreadPoolExecutor for concurrency. Suitable for sustained local
load tests of 10-2000 events/sec depending on machine.

Usage:
    python generate_load.py --rate 50 --duration 60
    python generate_load.py --rate 100 --duration 30 --providers a,b
"""

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from threading import Lock
from typing import Optional

MERCHANTS = ["AMAZON", "FLIPKART", "SWIGGY", "ZOMATO", "MYNTRA"]
PAYMENT_METHODS = ["UPI", "CARD", "WALLET", "BANK_TRANSFER"]
COUNTRIES = ["IN", "IN", "IN", "SG", "AE"]
CURRENCIES = ["INR", "inr", "₹", "rupees"]

PROVIDER_A_TEMPLATE = {
    "id": None,
    "customer": None,
    "value": None,
    "currency_code": None,
    "merchant_name": None,
    "method": None,
    "country": None,
    "result": None,
    "created": None,
}

PROVIDER_B_TEMPLATE = {
    "transaction_id": None,
    "user_id": None,
    "amount": None,
    "currency": None,
    "merchant": None,
    "payment_method": None,
    "country_code": None,
    "status": None,
    "timestamp": None,
}


@dataclass
class Stats:
    sent: int = 0
    accepted: int = 0
    rejected: int = 0
    errored: int = 0

    _lock: Lock = None

    def __post_init__(self):
        self._lock = Lock()

    def add(self, sent: int = 0, accepted: int = 0, rejected: int = 0, errored: int = 0):
        with self._lock:
            self.sent += sent
            self.accepted += accepted
            self.rejected += rejected
            self.errored += errored


def _provider_a_payload(event_id: str) -> dict:
    status = random.choices(
        ["success", "failed", "refunded"], weights=[80, 15, 5]
    )[0]
    return {
        "id": f"txn_{uuid.uuid4().hex[:12]}",
        "customer": f"usr_{random.randint(0, 49)}",
        "value": f"{random.uniform(50, 3000):.2f}",
        "currency_code": random.choice(CURRENCIES),
        "merchant_name": random.choice(MERCHANTS).lower(),
        "method": random.choice(["gpay", "upi", "card", "net_banking"]),
        "country": random.choice(["India", "India", "Singapore", "AE"]),
        "result": status,
        "created": _iso_timestamp(),
    }


def _provider_b_payload(event_id: str) -> dict:
    status = random.choices(
        ["SUCCESS", "FAILED", "REFUNDED"], weights=[80, 15, 5]
    )[0]
    return {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "user_id": f"usr_{random.randint(0, 49)}",
        "amount": round(random.uniform(50, 3000), 2),
        "currency": random.choice(["INR", "USD"]),
        "merchant": random.choice(MERCHANTS),
        "payment_method": random.choice(PAYMENT_METHODS),
        "country_code": random.choice(COUNTRIES),
        "status": status,
        "timestamp": _iso_timestamp(),
    }


def _iso_timestamp() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_event(provider: str) -> dict:
    event_id = f"evt_load_{uuid.uuid4().hex[:16]}"
    event_type = random.choices(
        [
            "payment.succeeded",
            "payment.failed",
            "payment.refunded",
        ],
        weights=[80, 15, 5],
    )[0]

    if provider == "a":
        payload = _provider_a_payload(event_id)
    else:
        payload = _provider_b_payload(event_id)

    return {
        "event_id": event_id,
        "event_type": event_type,
        "payload": payload,
    }


def _post(base_url: str, provider: str, event: dict, timeout: float) -> str:
    url = f"{base_url}/v1/webhooks/provider-{provider}"
    data = json.dumps(event).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return "accepted" if 200 <= resp.status < 300 else "rejected"
    except urllib.error.HTTPError:
        return "rejected"
    except Exception:
        return "errored"


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://localhost:8000")
    p.add_argument("--rate", type=int, default=50, help="events/sec target")
    p.add_argument("--duration", type=int, default=60, help="seconds")
    p.add_argument("--providers", default="a,b")
    p.add_argument("--timeout", type=float, default=5.0)
    p.add_argument("--workers", type=int, default=32)
    args = p.parse_args()

    providers = [x.strip().lower() for x in args.providers.split(",") if x.strip()]
    if not providers:
        print("no providers specified", file=sys.stderr)
        return 2

    total_events = args.rate * args.duration
    stats = Stats()

    print(
        f"Firing {total_events} events at {args.rate}/s for {args.duration}s "
        f"({args.workers} workers) to {args.url}"
    )

    start = time.monotonic()
    next_send = start

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = []
        for i in range(total_events):
            if i > 0:
                next_send = start + i / args.rate
                delay = next_send - time.monotonic()
                if delay > 0:
                    time.sleep(delay)

            provider = random.choice(providers)
            event = _make_event(provider)
            futures.append(pool.submit(_post, args.url, provider, event, args.timeout))

        for fut in as_completed(futures):
            result = fut.result()
            stats.add(sent=1)
            if result == "accepted":
                stats.add(accepted=1)
            elif result == "rejected":
                stats.add(rejected=1)
            else:
                stats.add(errored=1)

    elapsed = time.monotonic() - start
    actual_rate = stats.sent / elapsed if elapsed else 0

    print()
    print("=== Results ===")
    print(f"Sent:      {stats.sent}")
    print(f"Accepted:  {stats.accepted}")
    print(f"Rejected:  {stats.rejected}")
    print(f"Errored:   {stats.errored}")
    print(f"Elapsed:   {elapsed:.2f}s")
    print(f"Achieved:  {actual_rate:.1f} events/sec")

    return 0


if __name__ == "__main__":
    sys.exit(main())