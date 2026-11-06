/*
 * libaleltdecode - Db2 LUW log record framing and row-image decoding (ALELT).
 *
 * Behaviour mirrors the Python reference (connectors/sources/db2/alelt_src_db2/framing.py, rowimage.py);
 * both are checked against the same golden corpus (tests/golden/db2). Layouts: ../DESIGN.md.
 *
 * All functions are pure: no allocation, no globals, no I/O. Every read is bounds-checked against the
 * length passed in. Return value is an aleltd_err code; outputs are only valid on ALELTD_OK.
 *
 * Copyright Freddy Beltran. Licensed under the Apache License, Version 2.0.
 */
#ifndef ALELTDECODE_H
#define ALELTDECODE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32) && defined(ALELTDECODE_BUILD_DLL)
#define ALELTD_API __declspec(dllexport)
#elif defined(_WIN32) && defined(ALELTDECODE_USE_DLL)
#define ALELTD_API __declspec(dllimport)
#else
#define ALELTD_API
#endif

#define ALELTD_ABI_VERSION 2

/* Error codes. 1..14 are identical to alelt_src_db2.rowimage.Err (11 = Python-only codepage stage). */
typedef enum aleltd_err {
    ALELTD_OK = 0,
    ALELTD_E_SHORT = 1,            /* buffer too short for header / fixed section          */
    ALELTD_E_SUBREC_TYPE = 2,      /* record type not a user record (IBM rule, see rowimage.c) */
    ALELTD_E_NOT_USER_DATA = 3,    /* not a formatted user data record                      */
    ALELTD_E_BAD_FIXED_LEN = 4,
    ALELTD_E_BAD_NULL_FLAG = 5,
    ALELTD_E_BAD_BCD = 6,
    ALELTD_E_BAD_VAR_REF = 7,      /* (offset,len) of a var-length column outside record    */
    ALELTD_E_UNSUPPORTED_TYPE = 8,
    ALELTD_E_BAD_BOOL = 9,
    ALELTD_E_ARG = 10,             /* NULL pointer / invalid argument                       */
    ALELTD_E_VALUE_COMPRESSED = 12, /* inner record type 0x02: value-compressed format      */
    ALELTD_E_POINTER = 13,         /* outer 0x02 bit without 0x04: overflow pointer, no row */
    ALELTD_E_FIXED_LAYOUT = 14,    /* columns do not consume the fixed section exactly      */
    ALELTD_E_FRAMING = 20,         /* buffer cannot be framed at *pos                       */
    ALELTD_E_LAYOUT = 21,          /* DMS record layout not as expected                     */
    ALELTD_END = 100               /* aleltd_next_record: no more records                      */
} aleltd_err;

/* Cell kinds, identical to alelt_src_db2.rowimage.Kind. */
typedef enum aleltd_kind {
    ALELTD_K_NULL = 0,
    ALELTD_K_ABSENT = 1,
    ALELTD_K_INT = 2,
    ALELTD_K_FLOAT = 3,
    ALELTD_K_TEXT = 4,
    ALELTD_K_BYTES = 5,
    ALELTD_K_BOOL = 6,
    ALELTD_K_LOB_UNAVAILABLE = 7   /* value not in the image; i = LOB descriptor byte or ALELTD_UNAVAIL_* */
} aleltd_kind;

#define ALELTD_UNAVAIL_VAR_OUT_OF_ROW (-2)  /* varying-length field offset with the 0x8000 bit       */
#define ALELTD_UNAVAIL_DECFLOAT (-3)        /* row encoding not in IBM docs: re-read by key        */
#define ALELTD_UNAVAIL_XML (-4)             /* in-row XML descriptor only                          */
#define ALELTD_UNAVAIL_LONG (-5)            /* LONG VARCHAR / LONG VARGRAPHIC descriptor only       */

/* Column types (built-in Db2 types; distinct types resolved to their source type by the caller). */
typedef enum aleltd_type {
    ALELTD_T_UNSUPPORTED = 0,      /* unknown type => ALELTD_E_UNSUPPORTED_TYPE                */
    ALELTD_T_SMALLINT = 1,
    ALELTD_T_INTEGER = 2,
    ALELTD_T_BIGINT = 3,
    ALELTD_T_REAL = 4,
    ALELTD_T_DOUBLE = 5,
    ALELTD_T_DECIMAL = 6,          /* length = precision, scale = scale                    */
    ALELTD_T_DATE = 7,
    ALELTD_T_TIME = 8,
    ALELTD_T_TIMESTAMP = 9,        /* scale = fractional precision 0..12                   */
    ALELTD_T_BOOLEAN = 10,
    ALELTD_T_CHAR = 11,            /* length = bytes                                       */
    ALELTD_T_GRAPHIC = 12,         /* length = characters (2 bytes each)                   */
    ALELTD_T_BINARY = 13,
    ALELTD_T_VARCHAR = 14,
    ALELTD_T_VARGRAPHIC = 15,
    ALELTD_T_VARBINARY = 16,
    ALELTD_T_CLOB = 17,
    ALELTD_T_BLOB = 18,
    ALELTD_T_DBCLOB = 19,
    ALELTD_T_DECFLOAT = 20,        /* length 8 or 16; cell = LOB_UNAVAILABLE(-3)            */
    ALELTD_T_XML = 21,             /* 4 B (offset,len) descriptor; cell = LOB_UNAVAILABLE(-4) */
    ALELTD_T_LONGVAR = 22          /* LONG VARCHAR / LONG VARGRAPHIC, 4 B; LOB_UNAVAILABLE(-5) */
} aleltd_type;

/* ------------------------------------------------------------------ framing */

#define ALELTD_FILTER_HDR_SIZE 56u     /* db2ReadLogFilterData */
#define ALELTD_LOG_HDR_SIZE 40u

#define ALELTD_RT_NORMAL 0x004Eu
#define ALELTD_RT_CLR 0x0043u
#define ALELTD_RT_COMMIT 0x0084u
#define ALELTD_RT_PENDING_COMMIT 0x008Au
#define ALELTD_RT_ABORT 0x0041u

typedef struct aleltd_record {
    size_t offset;              /* offset of the 56 B filter header in the buffer       */
    size_t length;              /* 56 + size                                            */
    uint64_t lri_part1;
    uint64_t lri_part2;
    uint32_t real_len;
    int32_t sqlcode;
    uint32_t size;              /* log record size incl. 40 B header                    */
    uint16_t rtype;
    uint16_t flag;
    uint64_t lsn;
    uint64_t lfs;
    uint64_t back_lso;
    uint8_t tid[6];
    uint32_t header_size;       /* 40; CLR 56 or 64                                     */
    size_t body_offset;         /* offset of the body (after header_size) in the buffer */
    size_t body_length;
} aleltd_record;

/* Read the record at *pos; on ALELTD_OK advances *pos. ALELTD_END when *pos == len. */
ALELTD_API int aleltd_next_record(const uint8_t *buf, size_t len, size_t *pos, aleltd_record *out);

typedef struct aleltd_dms_header {
    uint8_t component;          /* 1 = DMS                                              */
    uint8_t function;           /* 161 DEL, 162 INS, 163 UPD, 110/111/112 undo, ...    */
    uint16_t tbspaceid;
    uint16_t tableid;
} aleltd_dms_header;

typedef struct aleltd_dms_images {
    int has_before;
    int has_after;
    size_t before_offset;       /* relative to body; whole sub-record (4 B header first) */
    size_t before_length;
    size_t after_offset;
    size_t after_length;
} aleltd_dms_images;

ALELTD_API int aleltd_dms_header_read(const uint8_t *body, size_t len, aleltd_dms_header *out);
ALELTD_API int aleltd_dms_images_read(const uint8_t *body, size_t len, const aleltd_dms_header *h,
                                aleltd_dms_images *out);

/* ------------------------------------------------------------------ row images */

typedef struct aleltd_column {
    int32_t type;               /* aleltd_type */
    int32_t length;
    int32_t scale;
    int32_t nullable;           /* 0/1 */
} aleltd_column;

#define ALELTD_TEXT_MAX 48

typedef struct aleltd_cell {
    int32_t kind;               /* aleltd_kind */
    int64_t i;                  /* INT, BOOL (0/1), LOB_UNAVAILABLE (descriptor byte or -1) */
    double f;                   /* FLOAT */
    uint32_t offset;            /* BYTES: offset relative to the sub-record start        */
    uint32_t length;            /* BYTES: length                                         */
    char text[ALELTD_TEXT_MAX];    /* TEXT: NUL-terminated ASCII                            */
} aleltd_cell;

/* Decode one sub-record into ncols cells. On error *err_col (if non-NULL) = failing column index
 * (or SIZE_MAX for record-level errors). */
ALELTD_API int aleltd_decode_row(const uint8_t *sub, size_t len, const aleltd_column *cols, size_t ncols,
                           aleltd_cell *cells, size_t *err_col);

/* Fixed-section width of a column (without null byte); -1 if unknown. */
ALELTD_API int32_t aleltd_fixed_width(int32_t type, int32_t length, int32_t scale);

ALELTD_API int aleltd_abi_version(void);

#ifdef __cplusplus
}
#endif

#endif /* ALELTDECODE_H */
