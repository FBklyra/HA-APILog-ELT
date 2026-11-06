-- SPDX-License-Identifier: Apache-2.0
-- ALELT lab: ARCHIVELOG + supplemental logging (lab database only) + sample schema SHOP in FREEPDB1.
-- Runs once, as SYSDBA, after the database is created (Oracle Free image "setup" scripts).
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE USER shop IDENTIFIED BY lab_local_only QUOTA UNLIMITED ON users;
GRANT CREATE SESSION, CREATE TABLE TO shop;
CREATE TABLE shop.customers (id NUMBER(10) PRIMARY KEY, name VARCHAR2(80) NOT NULL, email VARCHAR2(120),
                             city VARCHAR2(60), created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL);
ALTER TABLE shop.customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
INSERT INTO shop.customers (id, name, email, city)
SELECT LEVEL, 'Customer ' || LEVEL, 'customer' || LEVEL || '@example.test',
       DECODE(MOD(LEVEL, 3), 0, 'Montreal', 1, 'Lima', 'Recife') FROM dual CONNECT BY LEVEL <= 200;
COMMIT;
