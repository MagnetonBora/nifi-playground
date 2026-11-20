"""Customers REST API for Lab 4 (LookupRecord + RestLookupService).

  HTTP :8000  GET /customers/<id> -> customer JSON for c-1 and c-2, 404 with empty body otherwise
"""
import json

from common import Quiet, serve_http

CUSTOMERS = {
    # field names match the orders_stage columns, so PutDatabaseRecord maps them directly
    "c-1": {"customer_id": "c-1", "customer_name": "Alice Example", "customer_tier": "gold", "country": "DE"},
    "c-2": {"customer_id": "c-2", "customer_name": "Bob Example", "customer_tier": "silver", "country": "FR"},
}


class Customers(Quiet):
    def do_GET(self):
        parts = self.path.strip("/").split("/")
        if len(parts) == 2 and parts[0] == "customers" and parts[1] in CUSTOMERS:
            self.reply(200, json.dumps(CUSTOMERS[parts[1]]).encode())
        else:
            self.reply(404)  # empty body -> RestLookupService returns "no match"


if __name__ == "__main__":
    serve_http(8000, Customers)
