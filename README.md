# HA-APILog-ELT
Self-hosted, high-availability log-based CDC/ELT engine. Reads database transaction logs through public vendor APIs (Db2, PostgreSQL, MySQL, Oracle, SQL Server) and loads warehouses (Snowflake and others) with fenced failover and checkpoint-in-transaction exactly-once design.
