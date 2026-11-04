"""
Seed reference data (customers, cards, merchants) into PostgreSQL using Faker.

- Fixed SEED => same data on every run.
- Tables are truncated first => safe to re-run (idempotent).
- Some "bad" data is planted on purpose to test fraud rules later:
  blocked / expired cards, foreign merchants, high-risk MCCs.

Usage:
    uv run python scripts/postgres_seed_data.py
"""

import random
from datetime import date, timedelta

from faker import Faker

from db_connection import insert_rows, pg_cursor

SEED = 42
N_CUSTOMERS = 1000
N_MERCHANTS = 300

# Countries where customers live: country_code -> cities
COUNTRIES = {
    "US": ["New York", "Chicago", "Miami"],
    "GB": ["London", "Manchester"],
    "VN": ["Ha Noi", "Ho Chi Minh City", "Da Nang"],
    "SG": ["Singapore"],
    "DE": ["Berlin", "Munich"],
}
# Merchant-only countries => transactions here trigger the Geo Mismatch rule
FOREIGN_COUNTRIES = ["NG", "RU", "BR", "AE"]

# (mcc_code, category, is_high_risk)
MCC_CATALOG = [
    ("5411", "Grocery", False),
    ("5812", "Restaurant", False),
    ("5541", "Fuel", False),
    ("5732", "Electronics", False),
    ("4511", "Airline", False),
    ("6011", "ATM", False),
    ("7995", "Gambling", True),
    ("6051", "Crypto Exchange", True),
]

# Credit limit range (USD) per card type
CARD_TYPE_LIMIT = {"CREDIT": (2_000, 20_000), "DEBIT": (1_000, 10_000), "PREPAID": (200, 2_000)}


def new_customer(faker: Faker, random: random.Random) -> dict:
    customer_id = faker.uuid4()
    country = random.choice(list(COUNTRIES))
    return {
        "customer_id": customer_id,
        "full_name": faker.name(),
        # uuid prefix keeps the email unique (UNIQUE constraint)
        "email": f"{faker.user_name()}.{customer_id[:8]}@{faker.free_email_domain()}",
        "phone": faker.phone_number()[:30],
        "home_country": country,
        "home_city": random.choice(COUNTRIES[country]),
        "risk_profile": random.choices(["LOW", "MEDIUM", "HIGH"], weights=[70, 22, 8])[0],
    }


def new_card(faker: Faker, random: random.Random, customer_id: str) -> dict:
    card_type = random.choice(list(CARD_TYPE_LIMIT))
    low, high = CARD_TYPE_LIMIT[card_type]

    today = date.today()
    if random.random() < 0.06:  # ~6% expired
        expiry = today - timedelta(days=random.randint(30, 500))
    else:
        expiry = today + timedelta(days=random.randint(60, 1800))

    return {
        "card_id": faker.uuid4(),
        "customer_id": customer_id,
        "card_number_masked": f"****-****-****-{random.randint(0, 9999):04d}",
        "card_brand": random.choices(["Visa", "Mastercard", "Amex"], weights=[55, 35, 10])[0],
        "card_type": card_type,
        "credit_limit": round(random.uniform(low, high), -2),
        "is_active": random.random() > 0.05,  # ~5% blocked
        "expiry_date": expiry,
    }


def new_merchant(faker: Faker, random: random.Random) -> dict:
    mcc_code, category, is_high_risk = random.choice(MCC_CATALOG)
    if random.random() < 0.15:  # ~15% foreign merchants
        country, city = random.choice(FOREIGN_COUNTRIES), faker.city()
    else:
        country = random.choice(list(COUNTRIES))
        city = random.choice(COUNTRIES[country])
    return {
        "merchant_id": faker.uuid4(),
        "merchant_name": faker.company(),
        "mcc_code": mcc_code,
        "merchant_category": category,
        "country": country,
        "city": city,
        "is_high_risk_mcc": is_high_risk,
    }


def main() -> None:
    faker, random = Faker(), random.Random(SEED)
    faker.seed_instance(SEED)

    customers = [new_customer(faker, random) for _ in range(N_CUSTOMERS)]
    cards = [
        new_card(faker, random, c["customer_id"])
        for c in customers
        for _ in range(random.randint(1, 2))  # 1-2 cards per customer
    ]
    merchants = [new_merchant(faker, random) for _ in range(N_MERCHANTS)]

    with pg_cursor() as cur:
        cur.execute("TRUNCATE customers, cards, merchants CASCADE")
        insert_rows(cur, "customers", customers)
        insert_rows(cur, "cards", cards)
        insert_rows(cur, "merchants", merchants)

    print(f"Seeded {len(customers)} customers, {len(cards)} cards, {len(merchants)} merchants")


if __name__ == "__main__":
    main()
