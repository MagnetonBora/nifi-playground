"""Customers REST API for Lab 5 (LookupRecord + RestLookupService).

  HTTP :8000  GET /customers/<id> -> customer JSON for c-1 ... c-20, 404 with empty body otherwise
  (c-9xxx ids in lab5-orders.ndjson are deliberately unknown)
"""
import json

from common import Quiet, serve_http

CUSTOMERS = {
    # field names match the orders_stage columns, so PutDatabaseRecord maps them directly
    "c-1": {"customer_id": "c-1", "customer_name": "Alice Example", "customer_tier": "gold", "country": "DE"},
    "c-2": {"customer_id": "c-2", "customer_name": "Bob Example", "customer_tier": "silver", "country": "FR"},
}
# c-3 ... c-20: fictional customers; (name, tier, country)
MORE = [
    ("Lena Hoffmann", "gold", "DE"),       ("Mathis Lefevre", "bronze", "FR"),
    ("Zofia Wisniewska", "silver", "PL"),  ("Taras Melnyk", "gold", "UA"),
    ("Grace Whitfield", "bronze", "US"),   ("Jonas Becker", "silver", "DE"),
    ("Chloe Garnier", "gold", "FR"),       ("Piotr Zielinski", "bronze", "PL"),
    ("Oksana Bondar", "silver", "UA"),     ("Marcus Delgado", "gold", "US"),
    ("Hanna Krause", "bronze", "DE"),      ("Hugo Marchand", "silver", "FR"),
    ("Agnieszka Lewandowska", "gold", "PL"), ("Dmytro Kravets", "bronze", "UA"),
    ("Naomi Brooks", "silver", "US"),      ("Felix Wagner", "gold", "DE"),
    ("Ines Rousseau", "bronze", "FR"),     ("Mykola Savchenko", "silver", "UA"),
]
for n, (name, tier, country) in enumerate(MORE, start=3):
    CUSTOMERS[f"c-{n}"] = {"customer_id": f"c-{n}", "customer_name": name, "customer_tier": tier, "country": country}


class Customers(Quiet):
    def do_GET(self):
        parts = self.path.strip("/").split("/")
        if len(parts) == 2 and parts[0] == "customers" and parts[1] in CUSTOMERS:
            self.reply(200, json.dumps(CUSTOMERS[parts[1]]).encode())
        else:
            self.reply(404)  # empty body -> RestLookupService returns "no match"


if __name__ == "__main__":
    serve_http(8000, Customers)
