-- Lab 4 (database basics): source table for ExecuteSQLRecord / QueryDatabaseTableRecord and the
-- target table for PutDatabaseRecord. Applied automatically on a fresh database
-- (docker-entrypoint-initdb.d) and by `make lab-setup`; idempotent.
-- The data is deterministic: 1000 customers, every third one is gold (333 rows).
-- More rows for the incremental read: make lab4-insert n=10
CREATE TABLE IF NOT EXISTS customers (
    id          serial       PRIMARY KEY,
    name        varchar(128) NOT NULL,
    tier        varchar(16)  NOT NULL,
    country     varchar(2)   NOT NULL,
    updated_at  timestamptz  NOT NULL DEFAULT now()
);

INSERT INTO customers (name, tier, country)
SELECT 'Customer ' || g,
       (ARRAY['gold', 'silver', 'bronze'])[1 + g % 3],
       (ARRAY['DE', 'FR', 'PL', 'UA', 'US'])[1 + g % 5]
FROM generate_series(1, 1000) AS g
WHERE NOT EXISTS (SELECT 1 FROM customers);

CREATE TABLE IF NOT EXISTS customers_gold (
    id         integer      PRIMARY KEY,
    name       varchar(128) NOT NULL,
    tier       varchar(16)  NOT NULL,
    country    varchar(2)   NOT NULL,
    copied_at  timestamptz  NOT NULL DEFAULT now()
);
