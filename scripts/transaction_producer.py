from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timezone
from faker import Faker
from datetime import date
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
    "VELOCITY_ATTACK": 3,
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


def delivery_report(err,_msg) -> None:
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")


def main() -> None:
    parser = argparse.ArgumentParser(description="FinGuard Real-Time Transaction Producer")
    parser.add_argument("--tps", type=float, default=2.0, help="Transactions per second (default: 2.0)")
    parser.add_argument("--max-events", type=int, default=0, help="Stop after N events (0 = infinite)")
    args = parser.parse_args()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "fin_transactions")

    cache = load_reference_cache()
    
    Producer = import_module("confluent_kafka").Producer
    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "finguard-tx-producer",
        "acks": "all",
    })

    sleep_interval = 1.0 / max(args.tps, 0.1)
    sent_count = 0

    print(f"Streaming to '{topic}' ({bootstrap_servers}) at ~{args.tps} TPS. Press Ctrl+C to stop.")

    try:
        while True:
            events = generate_batch_events(cache)
            for event in events:
                producer.produce(
                    topic=topic,
                    key=event["card_id"].encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                producer.poll(0)
                sent_count += 1

                print(
                    f"[{sent_count:>5}] {event['injected_scenario']:<18} | "
                    f"{event['transaction_channel']:<6} | "
                    f"${event['amount']:>8.2f} | "
                    f"card={event['card_id'][:8]}... | "
                    f"country={event['transaction_country']}"
                )

                if args.max_events and sent_count >= args.max_events:
                    return

                # VELOCITY_ATTACK (0.15s)
                time.sleep(0.15 if len(events) > 1 else sleep_interval)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush(timeout=10)
        print(f"Flushed all messages. Total sent: {sent_count}")


if __name__ == "__main__":
    main()