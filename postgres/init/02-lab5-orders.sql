-- Lab 5 (DB + REST + Kafka): target table for PutDatabaseRecord (UPSERT by order_id).
-- Applied automatically on a fresh database (docker-entrypoint-initdb.d) and by `make lab-setup`.
-- The CHECK constraint is intentional: order o-1002 (negative amount) must fail in the DB and go to the DLQ.
CREATE TABLE IF NOT EXISTS orders_stage (
    order_id       varchar(64)    PRIMARY KEY,
    customer_id    varchar(64),
    customer_name  varchar(256),
    customer_tier  varchar(32),
    amount         numeric(12, 2) NOT NULL CONSTRAINT orders_stage_amount_positive CHECK (amount > 0),
    currency       varchar(3)     NOT NULL,
    created_at     timestamptz,
    loaded_at      timestamptz    NOT NULL DEFAULT now()
);
