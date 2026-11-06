/* SPDX-License-Identifier: Apache-2.0
 * Command whitelist parser (R5): everything not explicitly allowed is rejected.
 */
#include "command.h"
#include "alelttest.h"

#define ID "\"command_id\":\"0b6f6c1e-8a61-4d7a-9f1e-2a3b4c5d6e7f\",\"issued_at\":\"2026-11-03T12:00:00Z\""

static int parse(const char *s, alelt_cmd *c)
{
    char err[256];
    return alelt_cmd_parse(s, strlen(s), c, err, sizeof(err));
}

static void t_accept(void)
{
    static alelt_cmd c;
    CHECK(parse("{" ID ",\"name\":\"PAUSE\",\"args\":{}}", &c) == 0 && c.name == ALELT_CMD_PAUSE);
    CHECK(parse("{" ID ",\"name\":\"RESUME\"}", &c) == 0 && c.name == ALELT_CMD_RESUME);
    CHECK(parse("{" ID ",\"name\":\"START_CAPTURE\",\"args\":{\"from_pos\":{\"engine\":\"db2\",\"text\":\"12:00000000000000AB\"}}}", &c) == 0);
    CHECK(c.has_from_pos && strcmp(c.from_pos, "12:00000000000000AB") == 0);
    CHECK(parse("{" ID ",\"name\":\"START_CAPTURE\",\"args\":{}}", &c) == 0 && !c.has_from_pos);
    CHECK(parse("{" ID ",\"name\":\"SET_LIMITS\",\"args\":{\"cpu_pct\":25,\"batch_kb\":512}}", &c) == 0);
    CHECK(c.limits.cpu_pct == 25 && c.limits.batch_kb == 512 && c.limits.mem_mb == -1);
    CHECK(parse("{" ID ",\"name\":\"SNAPSHOT_CATALOG\",\"args\":{\"schemas\":[\"APPSCHEMA\",\"LOG$\"]}}", &c) == 0 && c.nschemas == 2);
    CHECK(parse("{" ID ",\"name\":\"SNAPSHOT_TABLE\",\"args\":{\"table\":{\"schema\":\"APPSCHEMA\",\"name\":\"CUSTOMER\"},"
                "\"key_from\":[\"C0001\",5],\"chunk_rows\":100}}", &c) == 0);
    CHECK(c.nkey_from == 2 && strcmp(c.key_from[1], "5") == 0 && c.chunk_rows == 100 && c.chunk_from == 0);
    CHECK(parse("{" ID ",\"name\":\"SNAPSHOT_TABLE\",\"args\":{\"table\":{\"schema\":\"APPSCHEMA\",\"name\":\"CUSTOMER\"},"
                "\"snapshot_id\":\"0b6f6c1e-8a61-4d7a-9f1e-2a3b4c5d6e70\",\"key_from\":[\"C0040\"],\"chunk_from\":3}}", &c) == 0);
    CHECK(c.chunk_from == 3 && c.nkey_from == 1); /* resumed snapshot (AGENT-COMMANDS: key_from + chunk_from) */
    CHECK(parse("{" ID ",\"name\":\"SNAPSHOT_TABLE\",\"args\":{\"table\":{\"schema\":\"A\",\"name\":\"B\"},\"chunk_from\":-1}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"PROBE\",\"args\":{\"name\":\"db_health\"}}", &c) == 0);
    CHECK(parse("{" ID ",\"name\":\"SHUTDOWN\",\"args\":null}", &c) == 0);
    /* VERIFY_TABLE: accepted, re-serialized from the whitelisted fields only */
    CHECK(parse("{" ID ",\"name\":\"VERIFY_TABLE\",\"args\":{\"table\":{\"schema\":\"ERPSIM\",\"name\":\"ORDERS\"},"
                "\"verify_id\":\"0b6f6c1e-8a61-4d7a-9f1e-2a3b4c5d6e70\",\"checks\":[\"row_count\",\"aggregates\"],"
                "\"columns\":[\"ID\",\"AMT\"],\"ranges\":[{\"after\":null,\"to\":[100]},{\"after\":[100],\"to\":null}]}}", &c) == 0);
    CHECK(c.name == ALELT_CMD_VERIFY_TABLE && strcmp(c.tab_schema, "ERPSIM") == 0 && c.verify_len > 0);
    CHECK(strstr(c.verify_args, "\"to\":[\"100\"]") != NULL);   /* numbers carried as text */
    CHECK(strstr(c.verify_args, "\"max_keys\":10000") != NULL); /* defaults made explicit */
    CHECK(strstr(c.verify_args, "\"timeout_s\":300") != NULL);
    CHECK(parse("{" ID ",\"name\":\"VERIFY_TABLE\",\"args\":{\"table\":{\"schema\":\"ERPSIM\",\"name\":\"ORDERS\"},"
                "\"verify_id\":\"0b6f6c1e-8a61-4d7a-9f1e-2a3b4c5d6e70\",\"checks\":[\"rows\"],"
                "\"key_values\":[[1],[\"2\"]],\"max_keys\":5,\"timeout_s\":30}}", &c) == 0);
    CHECK(strstr(c.verify_args, "\"key_values\":[[\"1\"],[\"2\"]]") != NULL);
}

static void t_reject(void)
{
    static alelt_cmd c;
    CHECK(parse("{" ID ",\"name\":\"EXEC_SQL\",\"args\":{\"sql\":\"DELETE FROM T\"}}", &c) != 0);
    CHECK(c.command_id[0] != 0); /* id still captured for the audit trail */
    CHECK(parse("{" ID ",\"name\":\"pause\"}", &c) != 0);                       /* case-sensitive */
    CHECK(parse("{" ID ",\"name\":\"PAUSE\",\"args\":{\"x\":1}}", &c) != 0);    /* args on argless */
    CHECK(parse("{" ID ",\"name\":\"PAUSE\",\"extra\":1}", &c) != 0);           /* unknown field */
    CHECK(parse("{\"command_id\":\"nope\",\"issued_at\":\"2026-11-03\",\"name\":\"PAUSE\"}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"PROBE\",\"args\":{\"name\":\"shell\"}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"PROBE\",\"args\":{\"name\":\"db_health\",\"sql\":\"x\"}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"SET_LIMITS\",\"args\":{\"cpu_pct\":0}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"SET_LIMITS\",\"args\":{\"cpu_pct\":\"50\"}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"SET_LIMITS\",\"args\":{}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"SNAPSHOT_TABLE\",\"args\":{\"table\":{\"schema\":\"APPSCHEMA\",\"name\":\"X\\\"; DROP\"}}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"SNAPSHOT_TABLE\",\"args\":{\"table\":\"APPSCHEMA.CUSTOMER\"}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"SNAPSHOT_TABLE\",\"args\":{\"table\":{\"schema\":\"A\",\"name\":\"B\"},\"key_from\":[{\"x\":1}]}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"SNAPSHOT_CATALOG\",\"args\":{\"schemas\":[\"A B\"]}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"START_CAPTURE\",\"args\":{\"from_pos\":{\"text\":\"a b\"}}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"START_CAPTURE\",\"args\":{\"from_lri\":\"1:0\"}}", &c) != 0);
#define VT "{" ID ",\"name\":\"VERIFY_TABLE\",\"args\":{\"table\":{\"schema\":\"S\",\"name\":\"T\"},\"verify_id\":\"0b6f6c1e-8a61-4d7a-9f1e-2a3b4c5d6e70\","
    CHECK(parse(VT "\"checks\":[\"row_count\"],\"sql\":\"SELECT 1\"}}", &c) != 0);         /* unknown arg */
    CHECK(parse(VT "\"checks\":[\"delete\"]}}", &c) != 0);                                 /* not whitelisted */
    CHECK(parse(VT "\"checks\":[\"keys\",\"keys\"]}}", &c) != 0);                         /* duplicate */
    CHECK(parse(VT "\"checks\":[]}}", &c) != 0);
    CHECK(parse(VT "\"checks\":[\"rows\"]}}", &c) != 0);                                   /* rows without keys */
    CHECK(parse(VT "\"checks\":[\"keys\"],\"key_values\":[[1]]}}", &c) != 0);            /* keys without rows */
    CHECK(parse(VT "\"checks\":[\"aggregates\"],\"columns\":[\"A;DROP\"]}}", &c) != 0); /* bad identifier */
    CHECK(parse(VT "\"checks\":[\"keys\"],\"ranges\":[{\"after\":[{\"x\":1}]}]}}", &c) != 0);
    CHECK(parse(VT "\"checks\":[\"keys\"],\"ranges\":[{\"from\":[1]}]}}", &c) != 0);
    CHECK(parse(VT "\"checks\":[\"keys\"],\"max_keys\":100000}}", &c) != 0);
    CHECK(parse(VT "\"checks\":[\"keys\"],\"timeout_s\":0}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"VERIFY_TABLE\",\"args\":{\"table\":{\"schema\":\"S\",\"name\":\"T\"},\"verify_id\":\"x\",\"checks\":[\"keys\"]}}", &c) != 0);
    CHECK(parse("[]", &c) != 0);
    CHECK(parse("{" ID "}", &c) != 0);
}

/* Cheap deterministic fuzz: random mutations must never crash and never yield
 * an accepted command whose name is outside the whitelist. */
/* AHS-05: START_CAPTURE{from_pos, discard_spool:true} = the audited recovery for a spool blocked by 409 */
static void t_discard_spool(void)
{
    static alelt_cmd c;
    CHECK(parse("{" ID ",\"name\":\"START_CAPTURE\",\"args\":{\"from_pos\":\"12:00000000000000AB\",\"discard_spool\":true}}", &c) == 0);
    CHECK(c.has_from_pos && c.discard_spool == 1);
    CHECK(parse("{" ID ",\"name\":\"START_CAPTURE\",\"args\":{\"from_pos\":\"12:AB\",\"discard_spool\":false}}", &c) == 0 &&
          c.discard_spool == 0);
    CHECK(parse("{" ID ",\"name\":\"START_CAPTURE\",\"args\":{\"discard_spool\":true}}", &c) != 0);      /* needs from_pos */
    CHECK(parse("{" ID ",\"name\":\"START_CAPTURE\",\"args\":{\"from_pos\":\"1:A\",\"discard_spool\":1}}", &c) != 0);
    CHECK(parse("{" ID ",\"name\":\"START_CAPTURE\",\"args\":{\"from_pos\":\"1:A\",\"discard_spool\":\"yes\"}}", &c) != 0);
}

static void t_mutation_fuzz(void)
{
    static const char *seed = "{" ID ",\"name\":\"SNAPSHOT_TABLE\",\"args\":{\"table\":{\"schema\":\"APPSCHEMA\",\"name\":\"CUSTOMER\"},\"key_from\":[\"a\"],\"chunk_rows\":10}}";
    static alelt_cmd c;
    char buf[512];
    unsigned x = 12345;
    int i, ok = 1;
    size_t n = strlen(seed);
    for (i = 0; i < 20000; i++) {
        size_t k, m;
        memcpy(buf, seed, n + 1);
        for (k = 0; k < 4; k++) {
            x = x * 1103515245u + 12345u;
            m = (x >> 8) % n;
            buf[m] = (char)((x >> 16) & 0xFF);
        }
        if (parse(buf, &c) == 0 && (c.name < ALELT_CMD_FIRST || c.name > ALELT_CMD_LAST)) ok = 0;
    }
    CHECK(ok);
}

void test_cmd_suite(void)
{
    printf("commands (R5 whitelist)\n");
    RUN(t_accept);
    RUN(t_reject);
    RUN(t_discard_spool);
    RUN(t_mutation_fuzz);
}
