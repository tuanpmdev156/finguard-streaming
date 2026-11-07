from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from faker import Faker
from db_connection import pg_cursor
import uuid
import random
import argparse
import json
import os
import time
from importlib import import_module
from dotenv import load_dotenv
load_dotenv()

@dataclass
class ReferenceCache:
    active_cards: list[dict]
    compromised_cards: list[dict]
    all_merchants: list[dict]
    merchants_by_country: dict[str,list[dict]]

def load_reference_cache():
    today = date.today()
    with pg_cursor() as cur:
        # Get card list by card holder
        cur.execute("""
            SELECT
                c.card_id,
                c.customer_id,
                c.credit_limit::FLOAT AS credit_limit,
                c.is_active,
                c.expiry_date,
                cu.home_country,
                cu.home_city
            FROM cards c
            JOIN customers cu ON c.customer_id = cu.customer_id
        """)
        card_cols = [desc[0] for desc in cur.description or ()]
        all_cards = [dict(zip(card_cols, card_row)) for card_row in cur.fetchall()]

        # Get merchant list
        cur.execute("""
            SELECT
                merchant_id,
                mcc_code,
                merchant_category,
                country,
                city,
                is_high_risk_mcc
            FROM merchants
        """)
        m_cols = [desc[0] for desc in cur.description or ()]
        all_merchants = [dict(zip(m_cols, m_row)) for m_row in cur.fetchall()]

        if not all_cards or not all_merchants:
            raise RuntimeError("Reference tables are empty!")

        # Valid cards
        active_cards = [
            c for c in all_cards if c["is_active"] and c["expiry_date"] >= today
        ]
        # Invalid cards (Locked or Expired)
        compromised_cards = [
            c for c in all_cards if not c["is_active"] or c["expiry_date"] < today
        ]

        merchants_by_country: dict[str, list[dict]] = defaultdict(list)
        for mer in all_merchants:
            merchants_by_country[mer["country"]].append(mer)

        print(
        f"Loaded cache: {len(active_cards)} active cards, "
        f"{len(compromised_cards)} compromised cards, "
        f"{len(all_merchants)} merchants across {len(merchants_by_country)} countries."
        )
        if not active_cards:
            print("[WARN] No active cards available. All scenarios will fall back to COMPROMISED_CARD.")
    
    return ReferenceCache(
        active_cards=active_cards,
        compromised_cards=compromised_cards,
        all_merchants=all_merchants,
        merchants_by_country=dict(merchants_by_country),
    )        

fake = Faker()
def build_event(card: dict,merchant: dict, amount: float, channel: str, scenario: str = "NORMAL"):
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "customer_id": card["customer_id"],
        "merchant_id": merchant["merchant_id"],
        "amount": round(amount, 2),
        "currency": "USD",
        "transaction_channel": channel,
        "transaction_country": merchant["country"],
        "transaction_city": merchant["city"],
        "ip_address": fake.ipv4_public() if channel == "Online" else None,
        "transaction_timestamp": datetime.now(timezone.utc).isoformat(),
        "injected_scenario": scenario,
    }

CHANNELS_WEIGHTS = {"POS": 50, "ATM": 15, "Online": 35}
SCENARIO_WEIGHTS = {
    "NORMAL": 87,
    "VELOCITY_ATTACK": 1,
    "HIGH_AMOUNT_SPIKE": 3,
    "GEO_MISMATCH": 3,
    "COMPROMISED_CARD": 3,
    "CORRUPT_RECORD": 1,
}

def pick_domestic_merchant(cache: ReferenceCache, country: str) -> dict:
    """Select a merchant in the same country as the cardholder (fallback to all_merchants if none exist).
    """
    pool = cache.merchants_by_country.get(country) or cache.all_merchants
    return random.choice(pool)

def pick_foreign_merchant(cache: ReferenceCache, home_country: str) -> dict:
    """Select a merchant in a different country from the cardholder to create a Geo Mismatch error."""
    foreign_pool = [m for m in cache.all_merchants if m["country"] != home_country]
    return random.choice(foreign_pool or cache.all_merchants)

def generate_batch_events(cache: ReferenceCache) -> list[dict]:
    """Generate a single transaction or a batch of transactions (if it's a VELOCITY_ATTACK).
    """
    scenario = random.choices(list(SCENARIO_WEIGHTS.keys()), weights=list(SCENARIO_WEIGHTS.values()), k=1)[0]
    channel = random.choices(list(CHANNELS_WEIGHTS.keys()), weights=list(CHANNELS_WEIGHTS.values()), k=1)[0]

    # Guard: avoid random.choice([]) when every card has been frozen/expired.
    # Fall back to COMPROMISED_CARD so the label stays honest; skip if no cards at all.
    if not cache.active_cards:
        if not cache.compromised_cards:
            return []
        scenario = "COMPROMISED_CARD"
    

    # 1. Normal valid transaction (~87%)
    if scenario == "NORMAL":
        card = random.choice(cache.active_cards)
        merchant = pick_domestic_merchant(cache, card["home_country"])
        amount = random.uniform(5.0, min(400.0, card["credit_limit"] * 0.3))
        return [build_event(card, merchant, amount, channel, "NORMAL")]
    
    # 2. Continuous card swiping 4-6 times in a few seconds (~3%)
    if scenario == "VELOCITY_ATTACK":
        card = random.choice(cache.active_cards)
        burst_size = random.randint(4, 6)
        return [
            build_event(
                card=card,
                merchant=pick_domestic_merchant(cache, card["home_country"]),
                amount=random.uniform(20.0, 150.0),
                channel=random.choice(["POS", "Online"]),
                scenario="VELOCITY_ATTACK",
            )
            for _ in range(burst_size)
        ]
        
    # 3. High amount spike > 90% of credit limit or > $5,000 (~3%) 
    if scenario == "HIGH_AMOUNT_SPIKE":
        card = random.choice(cache.active_cards)
        merchant = pick_domestic_merchant(cache, card["home_country"])
        amount = max(5000.01, card["credit_limit"] * random.uniform(0.92, 1.25))
        return [build_event(card, merchant, amount, channel, "HIGH_AMOUNT_SPIKE")]
    
    # 4. Physical transaction (POS/ATM) in a different country than the cardholder's residence (~3%)
    if scenario == "GEO_MISMATCH":
        card = random.choice(cache.active_cards)
        merchant = pick_foreign_merchant(cache, card["home_country"])
        physical_channel = random.choice(["POS", "ATM"])
        amount = random.uniform(50.0, 800.0)
        return [build_event(card, merchant, amount, physical_channel, "GEO_MISMATCH")]

    # 5. Use a card that has been locked or expired (~3%)
    if scenario == "COMPROMISED_CARD" and cache.compromised_cards:
        card = random.choice(cache.compromised_cards)
        merchant = pick_domestic_merchant(cache, card["home_country"])
        amount = random.uniform(25.0, 500.0)
        return [build_event(card, merchant, amount, channel, "COMPROMISED_CARD")]
    
    # 6. Corrupt records (amount <= 0) (~1%)
    card = random.choice(cache.active_cards)
    merchant = pick_domestic_merchant(cache, card["home_country"])
    bad_amount = random.choice([0.0, -15.5, -200.0])
    return [build_event(card, merchant, bad_amount, channel, "CORRUPT_RECORD")]


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "fin_transactions")


def parse_args() -> argparse.Namespace:
    """Parse CLI options for streaming rate (--tps) and event limit (--max-events)."""
    parser = argparse.ArgumentParser(description="FinGuard Real-Time Transaction Producer")
    parser.add_argument("--tps", type=float, default=2.0, help="Transactions per second (default: 2.0)")
    parser.add_argument("--max-events", type=int, default=0, help="Stop after N events (0 = infinite)")
    parser.add_argument(
        "--refresh-seconds", type=int, default=300,
        help="Reload card/merchant cache from PostgreSQL every N seconds (0 = never, default: 300)",
    )
    return parser.parse_args()


def refresh_reference_cache(current: ReferenceCache) -> ReferenceCache:
    """Reload reference cache; on any DB error keep the current cache instead of crashing."""
    try:
        return load_reference_cache()
    except Exception as exc:
        print(f"[WARN] Cache refresh failed, keeping previous cache: {exc}")
        return current


def delivery_report(err, _msg) -> None:
    """Kafka callback invoked asynchronously once a message is delivered or fails."""
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")


def send_event(producer, event: dict, index: int) -> None:
    """Serialize event to JSON, publish to Redpanda partitioned by card_id, and log."""
    producer.produce(
        topic=TOPIC,
        # Use card_id as partition key so all events of the same card land on the same partition
        key=event["card_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    # Trigger any pending delivery callbacks without blocking
    producer.poll(0)
    print(
        f"[{index:>5}] {event['injected_scenario']:<18} | "
        f"{event['transaction_channel']:<6} | "
        f"${event['amount']:>8.2f} | "
        f"card={event['card_id'][:8]}... | "
        f"country={event['transaction_country']}"
    )


def main() -> None:
    """Entry point: load reference cache, initialize Kafka producer, and stream events."""
    args = parse_args()
    cache = load_reference_cache()

    Producer = import_module("confluent_kafka").Producer
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "finguard-tx-producer",
        "acks": "all",
    })

    # Convert TPS (transactions/sec) to sleep seconds between normal events
    normal_delay = 1.0 / max(args.tps, 0.1)
    sent = 0
    last_refresh = time.monotonic()
    print(f"Streaming to '{TOPIC}' ({BOOTSTRAP_SERVERS}) at ~{args.tps} TPS. Press Ctrl+C to stop.")

    try:
        # Keep streaming forever if max_events == 0, otherwise stop once sent reaches max_events
        while not args.max_events or sent < args.max_events:
            # Periodically reload cards so frozen cards move to the compromised pool
            if args.refresh_seconds > 0 and time.monotonic() - last_refresh >= args.refresh_seconds:
                cache = refresh_reference_cache(cache)
                last_refresh = time.monotonic()

            # Generate 1 event (normal/single fraud) or 4-6 events (VELOCITY_ATTACK burst)
            batch = generate_batch_events(cache)

            # No cards available at all -> wait instead of busy-looping
            if not batch:
                print("[WARN] No cards available in cache, skipping event.")
                time.sleep(normal_delay)
                continue

            # Fire burst events rapidly (0.15s apart) to trigger Spark's velocity window;
            # otherwise wait according to the configured TPS rate
            delay = 0.15 if len(batch) > 1 else normal_delay

            for event in batch:
                sent += 1
                send_event(producer, event, sent)

                # Stop immediately inside a burst if we hit the --max-events limit
                if args.max_events and sent >= args.max_events:
                    break

                time.sleep(delay)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # Ensure all buffered messages in memory are delivered to Redpanda before exiting
        producer.flush(timeout=10)
        print(f"Flushed all messages. Total sent: {sent}")


if __name__ == "__main__":
    main()
