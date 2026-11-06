/* SPDX-License-Identifier: Apache-2.0
 * Whitelisted hub -> agent commands (R5). Mirrors alelt_common.models.Command.
 */
#ifndef ALELT_COMMAND_H
#define ALELT_COMMAND_H

#include <stddef.h>

#include "config.h"
#include "alelt_plugin.h"

#define ALELT_MAX_SCHEMAS 32
#define ALELT_CMD_MAX_JSON (64 * 1024)

typedef enum alelt_cmd_name {
    ALELT_CMD_NONE = 0,
    ALELT_CMD_START_CAPTURE,
    ALELT_CMD_PAUSE,
    ALELT_CMD_RESUME,
    ALELT_CMD_SET_LIMITS,
    ALELT_CMD_SNAPSHOT_CATALOG,
    ALELT_CMD_SNAPSHOT_TABLE,
    ALELT_CMD_PROBE,
    ALELT_CMD_SHUTDOWN,
    ALELT_CMD_VERIFY_TABLE
} alelt_cmd_name;
#define ALELT_CMD_FIRST ALELT_CMD_START_CAPTURE
#define ALELT_CMD_LAST ALELT_CMD_VERIFY_TABLE /* keep = last enumerator (fuzz invariant, AHS-17) */

/* VERIFY_TABLE caps (= alelt_common.models VERIFY_*) */
#define ALELT_VERIFY_ARGS_MAX (64 * 1024)
#define ALELT_VERIFY_MAX_RANGES 256
#define ALELT_VERIFY_MAX_KEYS 10000
#define ALELT_VERIFY_MAX_KEY_VALUES 1000
#define ALELT_VERIFY_MAX_COLUMNS 1012
#define ALELT_VERIFY_MAX_TIMEOUT_S 600

typedef struct alelt_cmd {
    char command_id[37];
    alelt_cmd_name name;
    char issued_at[40];

    /* START_CAPTURE */
    int has_from_pos;
    char from_pos[ALELT_POS_MAX];
    int discard_spool; /* AHS-05: operator recovery, needs from_pos; drops every unacknowledged batch */

    /* SET_LIMITS: -1 = not supplied */
    alelt_limits limits;

    /* SNAPSHOT_CATALOG */
    int nschemas;
    char schemas[ALELT_MAX_SCHEMAS][ALELT_IDENT_MAX];

    /* SNAPSHOT_TABLE */
    char snapshot_id[37];
    char tab_schema[ALELT_IDENT_MAX];
    char tab_name[ALELT_IDENT_MAX];
    int chunk_rows; /* 0 = default */
    int chunk_from; /* first chunk number of a resumed snapshot (0 = from the top) */
    int nkey_from, nkey_to;
    char key_from[ALELT_MAX_KEYCOLS][ALELT_KEYVAL_MAX];
    char key_to[ALELT_MAX_KEYCOLS][ALELT_KEYVAL_MAX];

    /* PROBE */
    char probe[32];

    /* VERIFY_TABLE (table in tab_schema/tab_name): validated args re-serialized
     * from the whitelisted fields only (canonical JSON, NUL-terminated). */
    char verify_id[37];
    size_t verify_len;
    char verify_args[ALELT_VERIFY_ARGS_MAX];
} alelt_cmd;

const char *alelt_cmd_str(alelt_cmd_name n);
int alelt_probe_name_valid(const char *name);

/* Parse one Command JSON object. 0 = accepted; otherwise err explains the
 * rejection and out->command_id is filled when it was readable (for audit). */
int alelt_cmd_parse(const char *json, size_t len, alelt_cmd *out, char *err, size_t errcap);
struct fj_val;
int alelt_cmd_from_val(const struct fj_val *v, alelt_cmd *out, char *err, size_t errcap);

#endif
