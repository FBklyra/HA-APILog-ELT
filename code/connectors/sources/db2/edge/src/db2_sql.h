/* SPDX-License-Identifier: Apache-2.0
 * The ONLY SQL the Db2 plugin can execute (R1). Every statement is a
 * read-only SELECT using parameter markers for all values. Catalog/monitor
 * statements end in WITH UR (SYSCAT/SYSIBMADM metadata); table data is read
 * only by the builders of db2_build.c, which end in WITH CS (F-04: no dirty
 * reads; needs CUR_COMMIT=ON so readers never wait on writers).
 * tests/test_db2.c enforces this on every build.
 */
#ifndef ALELT_DB2_SQL_H
#define ALELT_DB2_SQL_H

typedef enum db2_qid {
    Q_SCHEMAS = 0,   /* all user schemas                         */
    Q_TABLES,        /* tables of one schema                     */
    Q_COLUMNS,       /* columns of the tables of one schema      */
    Q_DB_CODEPAGE,   /* database code page                       */
    Q_LOG_UTIL,      /* transaction log utilisation (governor)   */
    Q_MON_DB,        /* database-level monitor counters          */
    Q_LOG_CFG,       /* log-related database configuration       */
    Q_SNAP_TABLE,    /* snapshot: table exists and is a table    */
    Q_META_COLS,     /* snapshot/verify: columns, key seq, types */
    Q_UNIQUE_KEYS,   /* unique-index columns of one schema (no-PK key fallback) */
    Q_PK_TABLES,     /* tables of one schema that have a primary key            */
    Q_UNIQUE_TAB,    /* unique-index columns of one table (no-PK key fallback)  */
    Q_PARTS,         /* data partitions of one schema (partition object ids)    */
    Q_SERVER_HOST,   /* host name of the server this connection reached (AHS-08) */
    Q__COUNT
} db2_qid;

extern const char *const db2_sql[Q__COUNT];

/* Fixed fragments the builders (db2_build.c) concatenate with quoted
 * identifiers and parameter markers. */
#define DB2_SNAP_SELECT "SELECT "
#define DB2_SNAP_FROM " FROM "
#define DB2_SNAP_WHERE " WHERE "
#define DB2_SNAP_ORDER " ORDER BY "
#define DB2_SNAP_FETCH " FETCH FIRST %u ROWS ONLY WITH CS"
#define DB2_ISO_CS " WITH CS"
#define DB2_COUNT_ALL "COUNT(*)"

#endif
