"""
Simulate day-to-day changes on reference data (run after postgres_seed_data.py):
    1. New customers sign up (1 card each)
    2. Some active cards get blocked (lost / stolen)
    3. Some active cards get a credit limit change

No seed => every run produces different changes.

Usage:
    uv run python scripts/postgres_incremental_load.py
"""

import random

from faker import Faker

from db_connection import insert_rows, pg_cursor
from postgres_seed_data import new_card, new_customer

N_NEW_CUSTOMERS = 20
N_BLOCKED_CARDS = 10
N_LIMIT_CHANGES = 20


def main() -> None:
    fake, rand = Faker(), random.Random()

    with pg_cursor() as cur:
        # 1. New customers + their cards
        customers = [new_customer(fake, rand) for _ in range(N_NEW_CUSTOMERS)]
        cards = [new_card(fake, rand, c["customer_id"]) for c in customers]
        insert_rows(cur, "customers", customers)
        insert_rows(cur, "cards", cards)

        # 2. Block random active cards
        cur.execute(
            """
            UPDATE cards SET is_active = FALSE
            WHERE card_id IN (
                SELECT card_id FROM cards WHERE is_active ORDER BY random() LIMIT %s
            )
            """,
            (N_BLOCKED_CARDS,),
        )
        blocked = cur.rowcount

        # 3. Change credit limits by x0.5 .. x1.5 (rounded to 100, min 100)
        cur.execute(
            """
            UPDATE cards SET credit_limit = GREATEST(100, ROUND(credit_limit * (0.5 + random())::numeric, -2))
            WHERE card_id IN (
                SELECT card_id FROM cards WHERE is_active ORDER BY random() LIMIT %s
            )
            """,
            (N_LIMIT_CHANGES,),
        )
        limit_changes = cur.rowcount

    print(
        f"Inserted {len(customers)} customers / {len(cards)} cards, "
        f"blocked {blocked} cards, changed {limit_changes} credit limits"
    )


if __name__ == "__main__":
    main()
