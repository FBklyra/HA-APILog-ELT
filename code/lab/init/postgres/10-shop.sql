-- SPDX-License-Identifier: Apache-2.0
-- ALELT lab: sample schema + fake data (PostgreSQL source). Runs once on first start of alelt-lab-postgres.
CREATE SCHEMA shop;
CREATE TABLE shop.customers (id integer PRIMARY KEY, name varchar(80) NOT NULL, email varchar(120), city varchar(60),
                             created_at timestamptz NOT NULL DEFAULT now(), vip boolean NOT NULL DEFAULT false);
CREATE TABLE shop.orders (id bigint PRIMARY KEY, customer_id integer NOT NULL REFERENCES shop.customers(id),
                          status varchar(16) NOT NULL, total numeric(12,2) NOT NULL, ordered_at timestamptz NOT NULL,
                          note text);
CREATE TABLE shop.order_items (order_id bigint NOT NULL REFERENCES shop.orders(id), line_no smallint NOT NULL,
                               sku varchar(32) NOT NULL, qty integer NOT NULL, price numeric(10,2) NOT NULL,
                               PRIMARY KEY (order_id, line_no));
ALTER TABLE shop.customers REPLICA IDENTITY FULL;
ALTER TABLE shop.orders REPLICA IDENTITY FULL;
ALTER TABLE shop.order_items REPLICA IDENTITY FULL;
INSERT INTO shop.customers (id, name, email, city, vip)
SELECT g, 'Customer ' || g, 'customer' || g || '@example.test',
       (ARRAY['Montreal','Toronto','Lima','Madrid','Recife','Lyon'])[1 + g % 6], g % 10 = 0
FROM generate_series(1, 200) g;
INSERT INTO shop.orders (id, customer_id, status, total, ordered_at, note)
SELECT g, 1 + g % 200, (ARRAY['NEW','PAID','SHIPPED','CANCELLED'])[1 + g % 4], round((g * 7.31)::numeric % 900, 2),
       timestamptz '2026-01-01 00:00Z' + g * interval '37 minutes', CASE WHEN g % 5 = 0 THEN 'gift wrap' END
FROM generate_series(1, 1000) g;
INSERT INTO shop.order_items (order_id, line_no, sku, qty, price)
SELECT o, l, 'SKU-' || lpad(((o * l) % 500)::text, 4, '0'), 1 + (o + l) % 5, round(((o * l) % 200 + 0.99)::numeric, 2)
FROM generate_series(1, 1000) o, generate_series(1, 3) l;
-- Capture role + publication (what the connector prerequisite check asks a DBA to create).
CREATE ROLE alelt_capture WITH LOGIN REPLICATION PASSWORD 'lab_local_only';
GRANT USAGE ON SCHEMA shop TO alelt_capture;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO alelt_capture;
CREATE PUBLICATION alelt_lab FOR TABLE shop.customers, shop.orders, shop.order_items;
