/* Formatted user data record (row image) decoding. Mirrors rowimage.py decode_cells(). */
#include "aleltdecode.h"
#include "aleltd_internal.h"

#include <string.h>

#define SUBREC_HDR 4u
#define USERDATA_HDR 4u
#define LOB_INLINE 0x69u
#define LOB_EMPTY 0x80u
#define VAR_OUT_OF_ROW 0x8000u
#define OUTER_OVERFLOW 0x04u
#define OUTER_POINTER 0x02u
#define MAX_DEC_PRECISION 31
#define MAX_TS_SCALE 12

int32_t aleltd_fixed_width(int32_t type, int32_t length, int32_t scale)
{
    if (length < 0 || scale < 0) {
        return -1;
    }
    switch (type) {
    case ALELTD_T_SMALLINT: return 2;
    case ALELTD_T_INTEGER: return 4;
    case ALELTD_T_BIGINT: return 8;
    case ALELTD_T_REAL: return 4;
    case ALELTD_T_DOUBLE: return 8;
    case ALELTD_T_BOOLEAN: return 1;
    case ALELTD_T_DECIMAL: return length / 2 + 1;
    case ALELTD_T_DATE: return 4;
    case ALELTD_T_TIME: return 3;
    case ALELTD_T_TIMESTAMP: return 7 + (scale + 1) / 2;
    case ALELTD_T_CHAR:
    case ALELTD_T_BINARY: return length;
    case ALELTD_T_GRAPHIC: return (length > INT32_MAX / 2) ? -1 : 2 * length;
    case ALELTD_T_VARCHAR:
    case ALELTD_T_VARGRAPHIC:
    case ALELTD_T_VARBINARY:
    case ALELTD_T_CLOB:
    case ALELTD_T_BLOB:
    case ALELTD_T_DBCLOB:
    case ALELTD_T_XML:
    case ALELTD_T_LONGVAR: return 4;
    case ALELTD_T_DECFLOAT: return (length == 8) ? 8 : 16;
    default: return -1;
    }
}

static int supported(const aleltd_column *c)
{
    if (c->type == ALELTD_T_DECIMAL && c->length > MAX_DEC_PRECISION) {
        return 0;
    }
    if (c->type == ALELTD_T_TIMESTAMP && c->scale > MAX_TS_SCALE) {
        return 0;
    }
    return c->type != ALELTD_T_UNSUPPORTED && aleltd_fixed_width(c->type, c->length, c->scale) >= 0;
}

/* n BCD digit nibbles (high nibble first) -> ASCII; dst must hold n + 1. */
static int bcd_digits(const uint8_t *b, size_t n, char *dst)
{
    size_t i;

    for (i = 0; i < n; i++) {
        unsigned v = (i & 1u) ? (b[i >> 1] & 0x0Fu) : (unsigned)(b[i >> 1] >> 4);
        if (v > 9u) {
            return ALELTD_E_BAD_BCD;
        }
        dst[i] = (char)('0' + v);
    }
    dst[n] = '\0';
    return ALELTD_OK;
}

static int decimal_text(const uint8_t *b, size_t w, int32_t scale, char *out)
{
    char d[2 * 16 + 1];
    size_t nd = 2u * w - 1u;
    size_t ip;
    size_t i;
    size_t o = 0;
    unsigned sign = b[w - 1u] & 0x0Fu;
    int rc;

    if (sign < 0x0Au) {
        return ALELTD_E_BAD_BCD;
    }
    if (nd >= sizeof(d)) {
        return ALELTD_E_UNSUPPORTED_TYPE;
    }
    rc = bcd_digits(b, nd, d);
    if (rc != ALELTD_OK) {
        return rc;
    }
    if ((size_t)scale > nd) {
        return ALELTD_E_BAD_BCD;
    }
    ip = nd - (size_t)scale;
    if (sign == 0x0Bu || sign == 0x0Du) {
        out[o++] = '-';
    }
    for (i = 0; i < ip && d[i] == '0'; i++) {
    }
    if (i == ip) {
        out[o++] = '0';
    }
    for (; i < ip; i++) {
        out[o++] = d[i];
    }
    if (scale > 0) {
        out[o++] = '.';
        for (i = ip; i < nd; i++) {
            out[o++] = d[i];
        }
    }
    out[o] = '\0';
    return ALELTD_OK;
}

static void put(char *out, size_t *o, const char *src, size_t n)
{
    memcpy(out + *o, src, n);
    *o += n;
}

static int datetime_text(int32_t type, const uint8_t *b, int32_t scale, char *out)
{
    char d[14 + MAX_TS_SCALE + 1];
    size_t o = 0;
    size_t n = (type == ALELTD_T_DATE) ? 8u : (type == ALELTD_T_TIME) ? 6u : 14u + (size_t)scale;
    int rc = bcd_digits(b, n, d);

    if (rc != ALELTD_OK) {
        return rc;
    }
    if (type == ALELTD_T_TIME) {
        put(out, &o, d, 2); out[o++] = ':'; put(out, &o, d + 2, 2); out[o++] = ':'; put(out, &o, d + 4, 2);
    } else {
        put(out, &o, d, 4); out[o++] = '-'; put(out, &o, d + 4, 2); out[o++] = '-'; put(out, &o, d + 6, 2);
        if (type == ALELTD_T_TIMESTAMP) {
            out[o++] = 'T';
            put(out, &o, d + 8, 2); out[o++] = ':'; put(out, &o, d + 10, 2); out[o++] = ':';
            put(out, &o, d + 12, 2);
            if (scale > 0) {
                out[o++] = '.';
                put(out, &o, d + 14, (size_t)scale);
            }
        }
    }
    out[o] = '\0';
    return ALELTD_OK;
}

/* Sign-extend a bits-wide two's complement value without implementation-defined conversions. */
static int64_t sext(uint64_t u, unsigned bits)
{
    if (bits < 64u) {
        uint64_t sign = (uint64_t)1 << (bits - 1u);
        return (u & sign) ? (int64_t)u - (int64_t)(sign << 1) : (int64_t)u;
    }
    return (u > (uint64_t)INT64_MAX) ? -(int64_t)(~u) - 1 : (int64_t)u;
}

static int var_cell(const aleltd_column *c, const uint8_t *f, const uint8_t *data, size_t dlen, size_t fixed_len,
                    aleltd_cell *cell)
{
    size_t off = aleltd_le16(f);
    size_t ln = aleltd_le16(f + 2);
    const uint8_t *v;
    int is_var = (c->type == ALELTD_T_VARCHAR || c->type == ALELTD_T_VARGRAPHIC || c->type == ALELTD_T_VARBINARY);

    if (c->type == ALELTD_T_XML) {             /* in-row XML descriptor only: re-read by key */
        cell->kind = ALELTD_K_LOB_UNAVAILABLE;
        cell->i = ALELTD_UNAVAIL_XML;
        return ALELTD_OK;
    }
    if (c->type == ALELTD_T_LONGVAR) {         /* long field descriptor only: re-read by key */
        cell->kind = ALELTD_K_LOB_UNAVAILABLE;
        cell->i = ALELTD_UNAVAIL_LONG;
        return ALELTD_OK;
    }
    if (is_var && (off & VAR_OUT_OF_ROW) != 0u) {   /* IBM docs: 0x8000 offset bit = stored out of row */
        cell->kind = ALELTD_K_LOB_UNAVAILABLE;
        cell->i = ALELTD_UNAVAIL_VAR_OUT_OF_ROW;
        return ALELTD_OK;
    }
    if (ln != 0u && (off < fixed_len || off + ln > dlen)) {   /* var data lies after the fixed section */
        return ALELTD_E_BAD_VAR_REF;
    }
    v = (ln != 0u) ? data + off : data;
    cell->kind = ALELTD_K_BYTES;
    if (is_var) {
        if (ln != 0u) {
            cell->offset = (uint32_t)(SUBREC_HDR + USERDATA_HDR + off);
            cell->length = (uint32_t)ln;
        }
        return ALELTD_OK;
    }
    /* LOB descriptor: 0x69 00 00 00 + inline data; 0x80 = empty; anything else = out of row */
    if (ln == 1u && v[0] == LOB_EMPTY) {
        return ALELTD_OK;
    }
    if (ln >= 4u && v[0] == LOB_INLINE && v[1] == 0u && v[2] == 0u && v[3] == 0u) {
        cell->offset = (uint32_t)(SUBREC_HDR + USERDATA_HDR + off + 4u);
        cell->length = (uint32_t)(ln - 4u);
        return ALELTD_OK;
    }
    cell->kind = ALELTD_K_LOB_UNAVAILABLE;
    cell->i = (ln != 0u) ? (int64_t)v[0] : -1;
    return ALELTD_OK;
}

static int value_cell(const aleltd_column *c, const uint8_t *f, size_t w, const uint8_t *data, size_t dlen,
                      size_t fixed_len, size_t fpos, aleltd_cell *cell)
{
    uint32_t u32;
    uint64_t u64;
    float fl;
    double db;

    switch (c->type) {
    case ALELTD_T_SMALLINT:
        cell->kind = ALELTD_K_INT;
        cell->i = sext(aleltd_le16(f), 16u);
        return ALELTD_OK;
    case ALELTD_T_INTEGER:
        cell->kind = ALELTD_K_INT;
        cell->i = sext(aleltd_le32(f), 32u);
        return ALELTD_OK;
    case ALELTD_T_BIGINT:
        cell->kind = ALELTD_K_INT;
        cell->i = sext(aleltd_le64(f), 64u);
        return ALELTD_OK;
    case ALELTD_T_REAL:
        u32 = aleltd_le32(f);
        memcpy(&fl, &u32, sizeof(fl));
        cell->kind = ALELTD_K_FLOAT;
        cell->f = (double)fl;
        return ALELTD_OK;
    case ALELTD_T_DOUBLE:
        u64 = aleltd_le64(f);
        memcpy(&db, &u64, sizeof(db));
        cell->kind = ALELTD_K_FLOAT;
        cell->f = db;
        return ALELTD_OK;
    case ALELTD_T_BOOLEAN:
        if (f[0] > 1u) {
            return ALELTD_E_BAD_BOOL;
        }
        cell->kind = ALELTD_K_BOOL;
        cell->i = f[0];
        return ALELTD_OK;
    case ALELTD_T_DECIMAL:
        cell->kind = ALELTD_K_TEXT;
        return decimal_text(f, w, c->scale, cell->text);
    case ALELTD_T_DATE:
    case ALELTD_T_TIME:
    case ALELTD_T_TIMESTAMP:
        cell->kind = ALELTD_K_TEXT;
        return datetime_text(c->type, f, c->scale, cell->text);
    case ALELTD_T_DECFLOAT:                    /* row encoding not in IBM docs: re-read by key */
        cell->kind = ALELTD_K_LOB_UNAVAILABLE;
        cell->i = ALELTD_UNAVAIL_DECFLOAT;
        return ALELTD_OK;
    case ALELTD_T_CHAR:
    case ALELTD_T_GRAPHIC:
    case ALELTD_T_BINARY:
        cell->kind = ALELTD_K_BYTES;
        cell->offset = (uint32_t)(SUBREC_HDR + USERDATA_HDR + fpos);
        cell->length = (uint32_t)w;
        return ALELTD_OK;
    default:
        return var_cell(c, f, data, dlen, fixed_len, cell);
    }
}

int aleltd_decode_row(const uint8_t *sub, size_t len, const aleltd_column *cols, size_t ncols,
                   aleltd_cell *cells, size_t *err_col)
{
    const uint8_t *data;
    size_t dlen;
    size_t fixed_len;
    size_t pos = 0;
    size_t i;
    int absent = 0;
    int rc;

    if (err_col != NULL) {
        *err_col = SIZE_MAX;
    }
    if (sub == NULL || (ncols != 0u && (cols == NULL || cells == NULL))) {
        return ALELTD_E_ARG;
    }
    if (len < SUBREC_HDR + USERDATA_HDR) {
        return ALELTD_E_SHORT;
    }
    /* IBM "Data manager log records": user record = outer type 0 or 0x10, or the 0x04 bit set with an inner type
     * having bit 0x01 (formatted) or 0x02 (value-compressed); 0x02 without 0x04 = pointer (row data elsewhere). */
    if (sub[0] != 0u && sub[0] != 0x10u && (sub[0] & OUTER_OVERFLOW) == 0u) {
        return ((sub[0] & OUTER_POINTER) != 0u) ? ALELTD_E_POINTER : ALELTD_E_SUBREC_TYPE;
    }
    if ((sub[4] & 0x01u) == 0u) {
        return ((sub[4] & 0x02u) != 0u) ? ALELTD_E_VALUE_COMPRESSED : ALELTD_E_NOT_USER_DATA;
    }
    fixed_len = aleltd_le16(sub + 6);
    data = sub + SUBREC_HDR + USERDATA_HDR;
    dlen = len - (SUBREC_HDR + USERDATA_HDR);
    if (fixed_len > dlen) {
        return ALELTD_E_BAD_FIXED_LEN;
    }
    for (i = 0; i < ncols; i++) {
        const aleltd_column *c = &cols[i];
        aleltd_cell *cell = &cells[i];
        size_t w;
        size_t need;

        memset(cell, 0, sizeof(*cell));
        if (err_col != NULL) {
            *err_col = i;
        }
        if (!supported(c)) {
            return ALELTD_E_UNSUPPORTED_TYPE;
        }
        w = (size_t)aleltd_fixed_width(c->type, c->length, c->scale);
        need = w + (c->nullable ? 1u : 0u);
        if (absent || need > fixed_len - pos) {
            absent = 1;
            cell->kind = ALELTD_K_ABSENT;
            continue;
        }
        if (c->nullable) {
            uint8_t nf = data[pos + w];
            if (nf > 1u) {
                return ALELTD_E_BAD_NULL_FLAG;
            }
            if (nf == 1u) {
                cell->kind = ALELTD_K_NULL;
                pos += need;
                continue;
            }
        }
        rc = value_cell(c, data + pos, w, data, dlen, fixed_len, pos, cell);
        if (rc != ALELTD_OK) {
            return rc;
        }
        pos += need;
    }
    if (err_col != NULL) {
        *err_col = SIZE_MAX;
    }
    if (!absent && pos != fixed_len) {      /* catalog columns must consume the fixed section exactly */
        return ALELTD_E_FIXED_LAYOUT;
    }
    return ALELTD_OK;
}
