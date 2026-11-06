#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# ALELT lab: sample schema SHOP with DATA CAPTURE CHANGES in LABDB.
# The Db2 Community image runs /var/custom/*.sh once after the instance is set up.
cat > /tmp/alelt_shop.sql <<'SQL'
CREATE SCHEMA SHOP;
CREATE TABLE SHOP.CUSTOMERS (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(80) NOT NULL, EMAIL VARCHAR(120),
  CITY VARCHAR(60), CREATED_AT TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP) DATA CAPTURE CHANGES;
INSERT INTO SHOP.CUSTOMERS (ID, NAME, EMAIL, CITY)
  WITH G(N) AS (VALUES 1 UNION ALL SELECT N + 1 FROM G WHERE N < 200)
  SELECT N, 'Customer ' || TRIM(CHAR(N)), 'customer' || TRIM(CHAR(N)) || '@example.test', 'Montreal' FROM G;
COMMIT;
SQL
chmod 644 /tmp/alelt_shop.sql
su - db2inst1 -c "db2 connect to LABDB && db2 -tvf /tmp/alelt_shop.sql; db2 terminate"
