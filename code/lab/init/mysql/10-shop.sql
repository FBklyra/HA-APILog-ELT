-- SPDX-License-Identifier: Apache-2.0
-- ALELT lab: sample schema + fake data (MySQL 9 and MariaDB sources). Runs once on first start.
CREATE DATABASE IF NOT EXISTS shop;
USE shop;
CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(80) NOT NULL, email VARCHAR(120), city VARCHAR(60),
                        created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), vip TINYINT(1) NOT NULL DEFAULT 0);
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id INT NOT NULL, status VARCHAR(16) NOT NULL,
                     total DECIMAL(12,2) NOT NULL, ordered_at DATETIME(6) NOT NULL, note TEXT,
                     FOREIGN KEY (customer_id) REFERENCES customers(id));
CREATE TABLE order_items (order_id BIGINT NOT NULL, line_no SMALLINT NOT NULL, sku VARCHAR(32) NOT NULL,
                          qty INT NOT NULL, price DECIMAL(10,2) NOT NULL, PRIMARY KEY (order_id, line_no),
                          FOREIGN KEY (order_id) REFERENCES orders(id));
INSERT INTO customers (id, name, email, city, vip)
WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 200)
SELECT n, CONCAT('Customer ', n), CONCAT('customer', n, '@example.test'),
       ELT(1 + n % 6, 'Montreal', 'Toronto', 'Lima', 'Madrid', 'Recife', 'Lyon'), n % 10 = 0 FROM g;
INSERT INTO orders (id, customer_id, status, total, ordered_at, note)
WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 1000)
SELECT n, 1 + n % 200, ELT(1 + n % 4, 'NEW', 'PAID', 'SHIPPED', 'CANCELLED'), ROUND(MOD(n * 7.31, 900), 2),
       TIMESTAMPADD(MINUTE, n * 37, '2026-01-01 00:00:00'), IF(n % 5 = 0, 'gift wrap', NULL) FROM g;
INSERT INTO order_items (order_id, line_no, sku, qty, price)
WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 1000)
SELECT g.n, l.k, CONCAT('SKU-', LPAD(MOD(g.n * l.k, 500), 4, '0')), 1 + MOD(g.n + l.k, 5), ROUND(MOD(g.n * l.k, 200) + 0.99, 2)
FROM g CROSS JOIN (SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 3) l;
-- Capture user (what the connector prerequisite check asks a DBA to create).
CREATE USER IF NOT EXISTS 'alelt_capture'@'%' IDENTIFIED BY 'lab_local_only';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'alelt_capture'@'%';
