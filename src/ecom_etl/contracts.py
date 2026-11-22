"""Source data contract for the seven Olist tables this project uses.

This module is the single place that describes every *source* table:
column names, logical types, nullability, allowed values, primary keys
and foreign keys. The synthetic generator, the profiler and (in later
steps) the Spark schemas and the data-quality checks all read from here,
so the "truth" about the source lives in exactly one file.

Column names are copied exactly from the Kaggle Olist dataset, including
its spelling mistakes (``product_name_lenght``). A contract describes the
data you *receive*, not the data you wish you had; renaming happens later,
in the silver layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

STRING = "string"
INT = "int"
FLOAT = "float"
TIMESTAMP = "timestamp"

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str = STRING
    nullable: bool = True
    accepted_values: Optional[Tuple[str, ...]] = None
    min_value: Optional[float] = None
    min_inclusive: bool = True
    # True for timestamps that record something that already happened
    # (a purchase, a delivery). Those can never be later than the moment
    # the data was extracted. Planned dates (estimated delivery) can.
    event_time: bool = False
    description: str = ""


@dataclass(frozen=True)
class ForeignKey:
    columns: Tuple[str, ...]
    ref_table: str
    ref_columns: Tuple[str, ...]


@dataclass(frozen=True)
class Table:
    name: str
    file_name: str
    columns: Tuple[Column, ...]
    primary_key: Tuple[str, ...]
    foreign_keys: Tuple[ForeignKey, ...] = ()
    description: str = ""

    @property
    def column_names(self) -> list:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"{self.name} has no column {name!r}")


ORDER_STATUSES = (
    "delivered",
    "shipped",
    "canceled",
    "unavailable",
    "invoiced",
    "processing",
    "created",
    "approved",
)

# "not_defined" appears in the real Olist data, but it is not a payment
# method, so the contract deliberately does not accept it.
PAYMENT_TYPES = ("credit_card", "boleto", "voucher", "debit_card")


CUSTOMERS = Table(
    name="customers",
    file_name="olist_customers_dataset.csv",
    description="One row per order-customer. customer_unique_id identifies the real person.",
    columns=(
        Column("customer_id", nullable=False, description="Key used by orders; new for every order"),
        Column("customer_unique_id", nullable=False, description="Stable id of the person"),
        Column("customer_zip_code_prefix", description="First 5 digits of the zip code"),
        Column("customer_city"),
        Column("customer_state"),
    ),
    primary_key=("customer_id",),
)

SELLERS = Table(
    name="sellers",
    file_name="olist_sellers_dataset.csv",
    columns=(
        Column("seller_id", nullable=False),
        Column("seller_zip_code_prefix"),
        Column("seller_city"),
        Column("seller_state"),
    ),
    primary_key=("seller_id",),
)

CATEGORY_TRANSLATION = Table(
    name="category_translation",
    file_name="product_category_name_translation.csv",
    columns=(
        Column("product_category_name", nullable=False),
        Column("product_category_name_english", nullable=False),
    ),
    primary_key=("product_category_name",),
)

PRODUCTS = Table(
    name="products",
    file_name="olist_products_dataset.csv",
    columns=(
        Column("product_id", nullable=False),
        Column("product_category_name", description="Portuguese name; can be null"),
        Column("product_name_lenght", INT, min_value=0),
        Column("product_description_lenght", INT, min_value=0),
        Column("product_photos_qty", INT, min_value=0),
        Column("product_weight_g", INT, min_value=0),
        Column("product_length_cm", INT, min_value=0),
        Column("product_height_cm", INT, min_value=0),
        Column("product_width_cm", INT, min_value=0),
    ),
    primary_key=("product_id",),
    foreign_keys=(
        ForeignKey(("product_category_name",), "category_translation", ("product_category_name",)),
    ),
)

ORDERS = Table(
    name="orders",
    file_name="olist_orders_dataset.csv",
    description="One row per order (the order header).",
    columns=(
        Column("order_id", nullable=False),
        Column("customer_id", nullable=False),
        Column("order_status", nullable=False, accepted_values=ORDER_STATUSES),
        Column("order_purchase_timestamp", TIMESTAMP, nullable=False, event_time=True),
        Column("order_approved_at", TIMESTAMP, event_time=True),
        Column("order_delivered_carrier_date", TIMESTAMP, event_time=True),
        Column("order_delivered_customer_date", TIMESTAMP, event_time=True),
        Column("order_estimated_delivery_date", TIMESTAMP, nullable=False),
    ),
    primary_key=("order_id",),
    foreign_keys=(ForeignKey(("customer_id",), "customers", ("customer_id",)),),
)

# The order lifecycle: each pair (later, earlier) must satisfy later >= earlier
# whenever both timestamps are present.
ORDER_TIMESTAMP_SEQUENCE = (
    ("order_approved_at", "order_purchase_timestamp"),
    ("order_delivered_carrier_date", "order_approved_at"),
    ("order_delivered_customer_date", "order_delivered_carrier_date"),
    ("order_delivered_customer_date", "order_purchase_timestamp"),
)

ORDER_ITEMS = Table(
    name="order_items",
    file_name="olist_order_items_dataset.csv",
    description="One row per order line. This is the grain of the future fact table.",
    columns=(
        Column("order_id", nullable=False),
        Column("order_item_id", INT, nullable=False, min_value=1, description="Line number inside the order"),
        Column("product_id", nullable=False),
        Column("seller_id", nullable=False),
        Column("shipping_limit_date", TIMESTAMP),
        Column("price", FLOAT, nullable=False, min_value=0, min_inclusive=False),
        Column("freight_value", FLOAT, nullable=False, min_value=0),
    ),
    primary_key=("order_id", "order_item_id"),
    foreign_keys=(
        ForeignKey(("order_id",), "orders", ("order_id",)),
        ForeignKey(("product_id",), "products", ("product_id",)),
        ForeignKey(("seller_id",), "sellers", ("seller_id",)),
    ),
)

ORDER_PAYMENTS = Table(
    name="order_payments",
    file_name="olist_order_payments_dataset.csv",
    description="One row per payment; an order can be paid in several parts.",
    columns=(
        Column("order_id", nullable=False),
        Column("payment_sequential", INT, nullable=False, min_value=1),
        Column("payment_type", nullable=False, accepted_values=PAYMENT_TYPES),
        Column("payment_installments", INT, nullable=False, min_value=1),
        Column("payment_value", FLOAT, nullable=False, min_value=0),
    ),
    primary_key=("order_id", "payment_sequential"),
    foreign_keys=(ForeignKey(("order_id",), "orders", ("order_id",)),),
)

# Order matters: parents before children, so loaders can respect foreign keys.
TABLES: Tuple[Table, ...] = (
    CUSTOMERS,
    SELLERS,
    CATEGORY_TRANSLATION,
    PRODUCTS,
    ORDERS,
    ORDER_ITEMS,
    ORDER_PAYMENTS,
)

TABLES_BY_NAME: Dict[str, Table] = {t.name: t for t in TABLES}
