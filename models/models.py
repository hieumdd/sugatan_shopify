from typing import Callable, TypedDict
import json


class ShopifyResource(TypedDict):
    table: str
    fields: list[str]
    api_ver: str
    endpoint: str
    transform: Callable[[list[dict]], list[dict]]
    schema: list[dict]


Orders: ShopifyResource = {
    "table": "Orders",
    "fields": [
        "app_id",
        "closed_at",
        "created_at",
        "currency",
        "customer",
        "email",
        "id",
        "line_items",
        "order_number",
        "processed_at",
        "refunds",
        "source_name",
        "subtotal_price",
        "total_tax",
        "total_shipping_price_set",
        "total_discounts",
        "total_price",
        "updated_at",
    ],
    "api_ver": "2021-10",
    "endpoint": "orders.json",
    "transform": lambda rows: [
        {
            "id": row.get("id"),
            "app_id": row.get("app_id"),
            "closed_at": row.get("closed_at"),
            "created_at": row.get("created_at"),
            "currency": row.get("currency"),
            "email": row.get("email"),
            "order_number": row.get("order_number"),
            "processed_at": row.get("processed_at"),
            "source_name": row.get("source_name"),
            "subtotal_price": row.get("subtotal_price"),
            "total_tax": row.get("total_tax"),
            "total_shipping_price_set": {
                "shop_money": {
                    "amount": row["total_shipping_price_set"]["shop_money"].get(
                        "amount"
                    ),
                    "currency_code": row["total_shipping_price_set"]["shop_money"].get(
                        "currency_code"
                    ),
                }
                if row["total_shipping_price_set"].get("shop_money")
                else {},
                "presentment_money": {
                    "amount": row["total_shipping_price_set"]["presentment_money"].get(
                        "amount"
                    ),
                    "currency_code": row["total_shipping_price_set"][
                        "presentment_money"
                    ].get("currency_code"),
                }
                if row["total_shipping_price_set"].get("presentment_money")
                else {},
            }
            if row.get("total_shipping_price_set")
            else {},
            "total_discounts": row.get("total_discounts"),
            "total_price": row.get("total_price"),
            "updated_at": row.get("updated_at"),
            "customer": json.dumps(row.get("customer")),
            "line_items": json.dumps(row.get("line_items")),
            "refunds": json.dumps(row.get("refunds")),
        }
        for row in rows
    ],
    "schema": [
        {"name": "id", "type": "INTEGER"},
        {"name": "app_id", "type": "INTEGER"},
        {"name": "closed_at", "type": "TIMESTAMP"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "currency", "type": "STRING"},
        {"name": "email", "type": "STRING"},
        {"name": "order_number", "type": "INTEGER"},
        {"name": "processed_at", "type": "TIMESTAMP"},
        {"name": "source_name", "type": "STRING"},
        {"name": "subtotal_price", "type": "FLOAT"},
        {"name": "total_tax", "type": "FLOAT"},
        {
            "name": "total_shipping_price_set",
            "type": "record",
            "fields": [
                {
                    "name": "shop_money",
                    "type": "record",
                    "fields": [
                        {"name": "amount", "type": "FLOAT"},
                        {"name": "currency_code", "type": "STRING"},
                    ],
                },
                {
                    "name": "presentment_money",
                    "type": "record",
                    "fields": [
                        {"name": "amount", "type": "FLOAT"},
                        {"name": "currency_code", "type": "STRING"},
                    ],
                },
            ],
        },
        {"name": "total_discounts", "type": "FLOAT"},
        {"name": "total_price", "type": "FLOAT"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "customer", "type": "STRING"},
        {"name": "line_items", "type": "STRING"},
        {"name": "refunds", "type": "STRING"},
    ],
}
