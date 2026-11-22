"""Synthetic e-commerce data in the exact shape of the Olist dataset.

Why a generator when real Olist data exists?

* Volume: Olist has ~112k order lines. To see what partitioning and
  broadcast joins actually buy you in Spark (step 7) you need millions.
* Ground truth: the generator records every defect it injects in
  ``_defects.csv``. That turns the data-quality checks (step 5) into
  something you can *measure*: did the checks find all injected defects,
  and nothing else?
* Determinism: the same ``--seed`` always produces byte-identical files,
  so tests and benchmarks are repeatable.

Design notes worth reading before the code:

* Two random generators. ``rng`` builds the clean data, ``drng`` decides
  defects. Because defects never consume numbers from ``rng``, the clean
  part of the data is identical whatever ``defect_rate`` you choose.
* Snapshot semantics. The data is an extract taken at ``extracted_at``.
  An event that would happen after that moment has not happened yet, so
  the generator removes it and rolls the order status back (a parcel that
  would arrive next week is still "shipped" today).
* Not every null is a defect. Olist products sometimes have no category
  and canceled orders have no delivery date; the contract allows both.
  Only rows listed in ``_defects.csv`` are actually wrong.
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from itertools import accumulate
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .contracts import (
    CATEGORY_TRANSLATION,
    CUSTOMERS,
    ORDER_ITEMS,
    ORDER_PAYMENTS,
    ORDERS,
    PRODUCTS,
    SELLERS,
    TABLES,
    TIMESTAMP_FORMAT,
)

MANIFEST_FILE = "_manifest.json"
DEFECTS_FILE = "_defects.csv"

# (portuguese name, english name, relative popularity) - real Olist categories
CATEGORIES: Sequence[Tuple[str, str, float]] = (
    ("cama_mesa_banho", "bed_bath_table", 10.0),
    ("beleza_saude", "health_beauty", 8.6),
    ("esporte_lazer", "sports_leisure", 7.7),
    ("moveis_decoracao", "furniture_decor", 7.4),
    ("informatica_acessorios", "computers_accessories", 6.9),
    ("utilidades_domesticas", "housewares", 6.2),
    ("relogios_presentes", "watches_gifts", 5.3),
    ("telefonia", "telephony", 4.0),
    ("ferramentas_jardim", "garden_tools", 3.9),
    ("automotivo", "auto", 3.8),
    ("brinquedos", "toys", 3.7),
    ("cool_stuff", "cool_stuff", 3.4),
    ("perfumaria", "perfumery", 3.0),
    ("bebes", "baby", 2.7),
    ("eletronicos", "electronics", 2.5),
    ("papelaria", "stationery", 2.2),
    ("fashion_bolsas_e_acessorios", "fashion_bags_accessories", 1.8),
    ("pet_shop", "pet_shop", 1.7),
    ("moveis_escritorio", "office_furniture", 1.5),
    ("consoles_games", "consoles_games", 1.0),
)
# Real Olist categories that are missing from its translation table.
UNTRANSLATED_CATEGORIES = ("pc_gamer", "portateis_cozinha_e_preparadores_de_alimentos")

# (state, share of customers in %, cities, zip prefix range)
STATES: Sequence[Tuple[str, float, Tuple[str, ...], Tuple[int, int]]] = (
    ("SP", 42.0, ("sao paulo", "campinas", "guarulhos", "santo andre", "osasco"), (1000, 19999)),
    ("RJ", 12.9, ("rio de janeiro", "niteroi", "nova iguacu"), (20000, 28999)),
    ("MG", 11.7, ("belo horizonte", "uberlandia", "juiz de fora"), (30000, 39999)),
    ("RS", 5.5, ("porto alegre", "caxias do sul"), (90000, 99999)),
    ("PR", 5.1, ("curitiba", "londrina", "maringa"), (80000, 87999)),
    ("SC", 3.7, ("florianopolis", "joinville", "blumenau"), (88000, 89999)),
    ("BA", 3.4, ("salvador", "feira de santana"), (40000, 48999)),
    ("DF", 2.2, ("brasilia",), (70000, 72799)),
    ("ES", 2.0, ("vitoria", "vila velha"), (29000, 29999)),
    ("GO", 2.0, ("goiania", "anapolis"), (72800, 76799)),
    ("PE", 1.7, ("recife", "jaboatao dos guararapes"), (50000, 56999)),
    ("CE", 1.3, ("fortaleza",), (60000, 63999)),
    ("PA", 1.0, ("belem",), (66000, 68899)),
    ("MT", 0.9, ("cuiaba",), (78000, 78899)),
    ("MA", 0.7, ("sao luis",), (65000, 65999)),
    ("MS", 0.7, ("campo grande",), (79000, 79999)),
    ("PB", 0.5, ("joao pessoa",), (58000, 58999)),
    ("PI", 0.5, ("teresina",), (64000, 64999)),
    ("RN", 0.5, ("natal",), (59000, 59999)),
    ("AL", 0.4, ("maceio",), (57000, 57999)),
    ("SE", 0.3, ("aracaju",), (49000, 49999)),
    ("TO", 0.3, ("palmas",), (77000, 77999)),
    ("RO", 0.25, ("porto velho",), (76800, 76999)),
    ("AM", 0.15, ("manaus",), (69000, 69299)),
    ("AC", 0.08, ("rio branco",), (69900, 69999)),
    ("AP", 0.07, ("macapa",), (68900, 68999)),
    ("RR", 0.05, ("boa vista",), (69300, 69399)),
)

ORDER_STATUS_WEIGHTS = (
    ("delivered", 97.0),
    ("shipped", 1.1),
    ("canceled", 0.6),
    ("unavailable", 0.6),
    ("invoiced", 0.3),
    ("processing", 0.3),
    ("created", 0.05),
    ("approved", 0.05),
)
PAYMENT_TYPE_WEIGHTS = (("credit_card", 75.0), ("boleto", 19.5), ("voucher", 4.0), ("debit_card", 1.5))
INSTALLMENT_WEIGHTS = ((1, 50), (2, 12), (3, 10), (4, 7), (5, 5), (6, 4), (7, 2), (8, 4), (10, 6))
ITEMS_PER_ORDER_WEIGHTS = ((1, 90.0), (2, 7.5), (3, 1.5), (4, 0.6), (5, 0.25), (6, 0.15))

# (table, defect type) -> (what the pipeline should do, description)
DEFECT_SPECS: Dict[Tuple[str, str], Tuple[str, str]] = {
    ("customers", "duplicate_row"): ("dedupe", "Exact duplicate row, as produced by an overlapping export"),
    ("products", "category_missing_translation"): (
        "fix",
        "Category has no English translation (happens in real Olist too)",
    ),
    ("orders", "duplicate_row"): ("dedupe", "Exact duplicate row (at-least-once export / replay)"),
    ("orders", "null_customer_id"): ("quarantine", "Required foreign key customer_id is empty"),
    ("orders", "status_unnormalized"): ("fix", "Valid status with wrong case or whitespace, e.g. ' Delivered'"),
    ("orders", "invalid_status"): ("quarantine", "Status outside the accepted set"),
    ("orders", "delivered_before_purchase"): ("quarantine", "Delivered to customer before it was purchased"),
    ("orders", "future_purchase"): ("quarantine", "Event timestamps later than the extraction time"),
    ("order_items", "orphan_product"): ("quarantine", "product_id does not exist in products"),
    ("order_items", "non_positive_price"): ("quarantine", "price is zero or negative"),
    ("order_items", "price_comma_decimal"): ("fix", "Decimal comma instead of point, e.g. '129,90'"),
    ("order_payments", "duplicate_row"): ("dedupe", "Exact duplicate payment row"),
    ("order_payments", "invalid_payment_type"): ("quarantine", "'not_defined' is not a payment method"),
    ("order_payments", "orphan_order"): ("quarantine", "Payment for an order_id that does not exist"),
}


@dataclass
class GeneratorConfig:
    orders: int = 10_000
    seed: int = 42
    defect_rate: float = 0.01
    start: date = date(2017, 1, 1)
    end: date = date(2018, 8, 31)

    @property
    def extracted_at(self) -> datetime:
        """Moment the extract was taken: midnight after the last purchase day."""
        return datetime.combine(self.end + timedelta(days=1), time())


class _Weighted:
    """Fast repeated weighted choice (bisect on cumulative weights)."""

    def __init__(self, items: Sequence, weights: Sequence[float]):
        self.items = list(items)
        self.cum = list(accumulate(weights))

    def pick(self, rng: random.Random):
        return rng.choices(self.items, cum_weights=self.cum, k=1)[0]


def _hex_id(rng: random.Random) -> str:
    return f"{rng.getrandbits(128):032x}"


def _ts(value: Optional[datetime]) -> str:
    return value.strftime(TIMESTAMP_FORMAT) if value else ""


def _money(value: float) -> str:
    return f"{value:.2f}"


class _DefectLog:
    def __init__(self, path: Path):
        self._file = path.open("w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file, lineterminator="\n")
        self._writer.writerow(["table", "defect_type", "expected_action", "key"])
        self.counts: Dict[Tuple[str, str], int] = {k: 0 for k in DEFECT_SPECS}
        self.samples: Dict[Tuple[str, str], List[str]] = {k: [] for k in DEFECT_SPECS}

    def add(self, table: str, defect: str, key: str) -> None:
        spec = (table, defect)
        self._writer.writerow([table, defect, DEFECT_SPECS[spec][0], key])
        self.counts[spec] += 1
        if len(self.samples[spec]) < 3:
            self.samples[spec].append(key)

    def close(self) -> None:
        self._file.close()


class _TableWriter:
    def __init__(self, directory: Path, table):
        self._file = (directory / table.file_name).open("w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file, lineterminator="\n")
        self._writer.writerow(table.column_names)
        self.rows = 0

    def write(self, row: Sequence) -> None:
        self._writer.writerow(row)
        self.rows += 1

    def close(self) -> None:
        self._file.close()


def _pick_value_defect(drng: random.Random, rate: float, defect_types: Sequence[str]) -> Optional[str]:
    """Each defect type independently has probability ``rate``; at most one per row."""
    if rate <= 0:
        return None
    u = drng.random()
    idx = int(u / rate)
    return defect_types[idx] if idx < len(defect_types) else None


def generate(config: GeneratorConfig, out_dir: Path) -> dict:
    """Write the seven Olist CSV files plus ``_manifest.json`` and ``_defects.csv``.

    Returns the manifest as a dict.
    """
    if config.orders < 1:
        raise ValueError("orders must be >= 1")
    if not 0 <= config.defect_rate <= 0.05:
        raise ValueError("defect_rate must be between 0 and 0.05")

    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(config.seed)
    drng = random.Random(config.seed + 1_000_003)
    rate = config.defect_rate
    cutoff = config.extracted_at

    writers = {t.name: _TableWriter(out_dir, t) for t in TABLES}
    defects = _DefectLog(out_dir / DEFECTS_FILE)

    states = _Weighted(STATES, [s[1] for s in STATES])

    def location() -> Tuple[str, str, str]:
        state, _, cities, (lo, hi) = states.pick(rng)
        return f"{rng.randint(lo, hi):05d}", rng.choice(cities), state

    try:
        # --- reference data -------------------------------------------------
        for pt, en, _ in CATEGORIES:
            writers["category_translation"].write([pt, en])

        n_sellers = max(5, config.orders // 32)
        seller_ids = []
        for _ in range(n_sellers):
            sid = _hex_id(rng)
            seller_ids.append(sid)
            writers["sellers"].write([sid, *location()])
        # A few big sellers own most products (long tail, like real marketplaces).
        seller_pick = _Weighted(seller_ids, [1.0 / (i + 1) ** 0.8 for i in range(n_sellers)])

        categories = _Weighted([c[0] for c in CATEGORIES], [c[2] for c in CATEGORIES])
        n_products = max(10, config.orders // 3)
        products: List[Tuple[str, str, float, int]] = []  # id, seller, price, weight_g
        for _ in range(n_products):
            pid = _hex_id(rng)
            has_category = rng.random() >= 0.0185
            category = categories.pick(rng) if has_category else ""
            weight_g = min(40425, max(2, int(rng.lognormvariate(6.6, 1.2))))
            row = [
                pid,
                category,
                str(rng.randint(5, 76)) if has_category else "",
                str(min(3992, max(4, int(rng.lognormvariate(6.5, 0.7))))) if has_category else "",
                str(rng.choice((1, 1, 1, 1, 2, 2, 3, 4, 5, 6))) if has_category else "",
                str(weight_g),
                str(rng.randint(7, 105)),
                str(rng.randint(2, 105)),
                str(rng.randint(6, 118)),
            ]
            if category and drng.random() < rate:
                row[1] = drng.choice(UNTRANSLATED_CATEGORIES)
                defects.add("products", "category_missing_translation", pid)
            writers["products"].write(row)
            price = round(min(6735.0, max(0.85, rng.lognormvariate(4.4, 0.9))), 2)
            products.append((pid, seller_pick.pick(rng), price, weight_g))
        # Popularity follows a power law: a few products sell a lot (data skew!).
        product_pick = _Weighted(range(n_products), [1.0 / (i + 1) ** 0.9 for i in range(n_products)])

        statuses = _Weighted([s for s, _ in ORDER_STATUS_WEIGHTS], [w for _, w in ORDER_STATUS_WEIGHTS])
        pay_types = _Weighted([p for p, _ in PAYMENT_TYPE_WEIGHTS], [w for _, w in PAYMENT_TYPE_WEIGHTS])
        installments = _Weighted([i for i, _ in INSTALLMENT_WEIGHTS], [w for _, w in INSTALLMENT_WEIGHTS])
        items_per_order = _Weighted(
            [n for n, _ in ITEMS_PER_ORDER_WEIGHTS], [w for _, w in ITEMS_PER_ORDER_WEIGHTS]
        )

        start_dt = datetime.combine(config.start, time())
        span = int((cutoff - start_dt).total_seconds())
        people: List[Tuple[str, Tuple[str, str, str]]] = []

        # --- transactional data, one order at a time ---------------------------
        for _ in range(config.orders):
            # customer: Olist creates a new customer_id for every order
            if people and rng.random() < 0.03:
                unique_id, loc = rng.choice(people)
                if rng.random() < 0.1:  # the person moved since their last order
                    loc = location()
            else:
                unique_id, loc = _hex_id(rng), location()
                people.append((unique_id, loc))
            customer_id = _hex_id(rng)
            customer_row = [customer_id, unique_id, *loc]
            writers["customers"].write(customer_row)
            if drng.random() < rate:
                writers["customers"].write(customer_row)
                defects.add("customers", "duplicate_row", customer_id)

            # order timeline (more orders later in the period: the business grows)
            order_id = _hex_id(rng)
            status = statuses.pick(rng)
            purchase = start_dt + timedelta(seconds=int(rng.random() ** 0.8 * (span - 1)))
            approved = purchase + timedelta(minutes=min(10_080, int(rng.expovariate(1 / 600))))
            carrier = approved + timedelta(hours=rng.uniform(12, 144))
            delivered = carrier + timedelta(hours=rng.gammavariate(2.0, 96.0))
            estimated = datetime.combine(purchase.date() + timedelta(days=rng.randint(12, 45)), time())
            if status == "delivered":
                pass
            elif status == "shipped":
                delivered = None
            elif status == "canceled":
                approved = approved if rng.random() < 0.85 else None
                carrier = carrier if approved and rng.random() < 0.1 else None
                delivered = None
            elif status == "created":
                approved = carrier = delivered = None
            else:  # unavailable, invoiced, processing, approved
                carrier = delivered = None
            # snapshot semantics: nothing can have happened after the extract
            if delivered and delivered > cutoff:
                delivered = None
                status = "shipped" if status == "delivered" else status
            if carrier and carrier > cutoff:
                carrier = None
                status = "invoiced" if status == "shipped" else status
            if approved and approved > cutoff:
                approved = None
                status = "created"

            # order lines
            lines = []
            if status != "unavailable":  # in Olist, unavailable orders have no items
                product_idx = product_pick.pick(rng)
                for line_no in range(1, items_per_order.pick(rng) + 1):
                    if line_no > 1 and rng.random() >= 0.5:
                        product_idx = product_pick.pick(rng)
                    pid, sid, price, weight_g = products[product_idx]
                    freight = round(4.5 + weight_g / 1000 * 2.2 + rng.uniform(0, 9), 2)
                    limit = (approved or purchase) + timedelta(days=6, hours=rng.randint(0, 23))
                    lines.append([order_id, line_no, pid, sid, limit, price, freight])
                total = round(sum(line[5] + line[6] for line in lines), 2)
            else:
                total = round(min(6735.0, max(5.0, rng.lognormvariate(4.6, 0.9))), 2)

            # payments: mostly one payment; sometimes vouchers + a main method
            payments = []
            if total > 20 and rng.random() < 0.03:
                remaining = total
                for seq in range(1, rng.choice((2, 2, 2, 3))):
                    value = round(total * rng.uniform(0.05, 0.3), 2)
                    payments.append([order_id, seq, "voucher", 1, value])
                    remaining = round(remaining - value, 2)
                main_type = pay_types.pick(rng)
                inst = installments.pick(rng) if main_type == "credit_card" else 1
                payments.append([order_id, len(payments) + 1, main_type, inst, remaining])
            else:
                main_type = pay_types.pick(rng)
                inst = installments.pick(rng) if main_type == "credit_card" else 1
                payments.append([order_id, 1, main_type, inst, total])

            # --- defects (decided by drng only) -----------------------------
            order_defect = _pick_value_defect(
                drng,
                rate,
                ("null_customer_id", "status_unnormalized", "invalid_status",
                 "delivered_before_purchase", "future_purchase"),
            )
            out_customer, out_status = customer_id, status
            if order_defect == "null_customer_id":
                out_customer = ""
            elif order_defect == "status_unnormalized":
                out_status = drng.choice((status.upper(), " " + status.capitalize(), status + " "))
            elif order_defect == "invalid_status":
                out_status = "unknown"
            elif order_defect == "delivered_before_purchase":
                if delivered is None:
                    order_defect = None  # only meaningful for delivered orders
                else:
                    delivered = purchase - timedelta(hours=drng.randint(24, 240))
            elif order_defect == "future_purchase":
                # move the whole timeline so the purchase lands after the extract
                shift = (cutoff - purchase) + timedelta(days=drng.randint(1, 365), minutes=drng.randint(1, 1439))
                purchase, estimated = purchase + shift, estimated + shift
                approved = approved + shift if approved else None
                carrier = carrier + shift if carrier else None
                delivered = delivered + shift if delivered else None
            if order_defect:
                defects.add("orders", order_defect, order_id)

            order_row = [
                order_id, out_customer, out_status, _ts(purchase), _ts(approved),
                _ts(carrier), _ts(delivered), _ts(estimated),
            ]
            writers["orders"].write(order_row)
            if order_defect is None and drng.random() < rate:
                writers["orders"].write(order_row)
                defects.add("orders", "duplicate_row", order_id)

            for line in lines:
                key = f"{order_id}|{line[1]}"
                out = [line[0], str(line[1]), line[2], line[3], _ts(line[4]), _money(line[5]), _money(line[6])]
                line_defect = _pick_value_defect(
                    drng, rate, ("orphan_product", "non_positive_price", "price_comma_decimal")
                )
                if line_defect == "orphan_product":
                    out[2] = _hex_id(drng)
                elif line_defect == "non_positive_price":
                    out[5] = drng.choice(("0.00", "-" + out[5]))
                elif line_defect == "price_comma_decimal":
                    out[5] = out[5].replace(".", ",")
                if line_defect:
                    defects.add("order_items", line_defect, key)
                writers["order_items"].write(out)

            for pay in payments:
                key = f"{order_id}|{pay[1]}"
                out = [pay[0], str(pay[1]), pay[2], str(pay[3]), _money(pay[4])]
                if drng.random() < rate:
                    out[2] = "not_defined"
                    defects.add("order_payments", "invalid_payment_type", key)
                    writers["order_payments"].write(out)
                    continue
                writers["order_payments"].write(out)
                if drng.random() < rate:
                    writers["order_payments"].write(out)
                    defects.add("order_payments", "duplicate_row", key)

            if drng.random() < rate:  # a payment that belongs to no order
                ghost = _hex_id(drng)
                writers["order_payments"].write([ghost, "1", "credit_card", "1", _money(total)])
                defects.add("order_payments", "orphan_order", f"{ghost}|1")
    finally:
        for w in writers.values():
            w.close()
        defects.close()

    manifest = {
        "generator": "ecom_etl.generator",
        "config": {k: str(v) if isinstance(v, date) else v for k, v in asdict(config).items()},
        "extracted_at": _ts(cutoff),
        "row_counts": {name: w.rows for name, w in writers.items()},
        "defects": [
            {
                "table": table,
                "defect_type": defect,
                "expected_action": DEFECT_SPECS[(table, defect)][0],
                "description": DEFECT_SPECS[(table, defect)][1],
                "count": defects.counts[(table, defect)],
                "sample_keys": defects.samples[(table, defect)],
            }
            for (table, defect) in DEFECT_SPECS
        ],
    }
    manifest["total_defects"] = sum(d["count"] for d in manifest["defects"])
    (out_dir / MANIFEST_FILE).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def load_manifest(directory: Path) -> Optional[dict]:
    path = directory / MANIFEST_FILE
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
