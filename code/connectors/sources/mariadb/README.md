# MariaDB source

Thin variant: `alelt_src_mariadb.connector:MariaDBSource` re-exports the MariaDB flavor of the shared binlog core in
`connectors/sources/mysql` (see its README for settings, positions, types, prerequisites and tests).

MariaDB specifics handled by the core: GTID list positions (`domain-server-seq`), MariaDB GTID / GTID_LIST /
ANNOTATE_ROWS events, row metadata optional (names and types from the catalog), JSON as LONGTEXT + `json_valid`,
exact snapshot position (`binlog_snapshot_file/position` + `BINLOG_GTID_POS`), privilege `BINLOG MONITOR`.

PYTHONPATH: `connectors/sources/mariadb` (falls back to the sibling `connectors/sources/mysql` in the repo layout;
in the image put both on the path).
