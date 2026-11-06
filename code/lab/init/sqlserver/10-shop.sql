-- SPDX-License-Identifier: Apache-2.0
-- ALELT lab: sample schema + fake data + CDC (SQL Server source). Run by `lab up sqlserver` (idempotent).
IF DB_ID('labdb') IS NULL CREATE DATABASE labdb;
GO
USE labdb;
GO
IF SCHEMA_ID('shop') IS NULL EXEC('CREATE SCHEMA shop');
GO
IF OBJECT_ID('shop.customers') IS NULL
BEGIN
  CREATE TABLE shop.customers (id INT PRIMARY KEY, name NVARCHAR(80) NOT NULL, email NVARCHAR(120), city NVARCHAR(60),
                               created_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(), vip BIT NOT NULL DEFAULT 0);
  CREATE TABLE shop.orders (id BIGINT PRIMARY KEY, customer_id INT NOT NULL REFERENCES shop.customers(id),
                            status VARCHAR(16) NOT NULL, total DECIMAL(12,2) NOT NULL, ordered_at DATETIME2(6) NOT NULL,
                            note NVARCHAR(MAX));
END
GO
IF NOT EXISTS (SELECT 1 FROM shop.customers)
BEGIN
  WITH g AS (SELECT TOP (200) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) n FROM sys.all_objects)
  INSERT INTO shop.customers (id, name, email, city, vip)
  SELECT n, CONCAT('Customer ', n), CONCAT('customer', n, '@example.test'),
         CHOOSE(1 + n % 6, 'Montreal', 'Toronto', 'Lima', 'Madrid', 'Recife', 'Lyon'), IIF(n % 10 = 0, 1, 0) FROM g;
  WITH g AS (SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) n FROM sys.all_objects a CROSS JOIN sys.all_objects b)
  INSERT INTO shop.orders (id, customer_id, status, total, ordered_at, note)
  SELECT n, 1 + n % 200, CHOOSE(1 + n % 4, 'NEW', 'PAID', 'SHIPPED', 'CANCELLED'), CAST((n * 7.31) % 900 AS DECIMAL(12,2)),
         DATEADD(MINUTE, n * 37, '2026-01-01'), IIF(n % 5 = 0, 'gift wrap', NULL) FROM g;
END
GO
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'labdb' AND is_cdc_enabled = 1) EXEC sys.sp_cdc_enable_db;
GO
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('shop.customers'))
  EXEC sys.sp_cdc_enable_table @source_schema = 'shop', @source_name = 'customers', @role_name = NULL;
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('shop.orders'))
  EXEC sys.sp_cdc_enable_table @source_schema = 'shop', @source_name = 'orders', @role_name = NULL;
GO
