/* db2ReadLog (filter ON) buffer framing and DMS record image location. Mirrors framing.py. */
#include "aleltdecode.h"
#include "aleltd_internal.h"

#include <string.h>

#define CLR_FLAG_EXTRA 0x0002u
#define DMS_HDR_SIZE 6u
#define DMS_IMG_OFF 14u        /* u16 internal, u16 len, u16 free, RID 6, u16 offset */
#define DMS_IMG_OFF_EMPTY 20u  /* *-to-empty-page variants: 3 more u16 */

enum {
    F_UNDO_INSERT = 110, F_UNDO_DELETE = 111, F_UNDO_UPDATE = 112, F_UNDO_INSERT_EMPTY = 131,
    F_DELETE = 161, F_INSERT = 162, F_UPDATE = 163,
    F_DELETE_EMPTY = 164, F_INSERT_EMPTY = 165, F_UNDO_DELETE_EMPTY = 166
};

int aleltd_abi_version(void)
{
    return ALELTD_ABI_VERSION;
}

int aleltd_next_record(const uint8_t *buf, size_t len, size_t *pos, aleltd_record *out)
{
    size_t off;
    size_t rec_off;
    const uint8_t *r;

    if (buf == NULL || pos == NULL || out == NULL || *pos > len) {
        return ALELTD_E_ARG;
    }
    off = *pos;
    if (off == len) {
        return ALELTD_END;
    }
    if (len - off < (size_t)ALELTD_FILTER_HDR_SIZE + ALELTD_LOG_HDR_SIZE) {
        return ALELTD_E_FRAMING;
    }
    if (aleltd_le64(buf + off) != 1u) {
        return ALELTD_E_FRAMING;
    }
    memset(out, 0, sizeof(*out));
    out->offset = off;
    out->lri_part1 = aleltd_le64(buf + off + 8);
    out->lri_part2 = aleltd_le64(buf + off + 16);
    out->real_len = aleltd_le32(buf + off + 48);
    out->sqlcode = (int32_t)aleltd_le32(buf + off + 52);

    rec_off = off + ALELTD_FILTER_HDR_SIZE;
    r = buf + rec_off;
    out->size = aleltd_le32(r);
    out->rtype = aleltd_le16(r + 4);
    out->flag = aleltd_le16(r + 6);
    out->lsn = aleltd_le64(r + 8);
    out->lfs = aleltd_le64(r + 16);
    out->back_lso = aleltd_le64(r + 24);
    memcpy(out->tid, r + 32, sizeof(out->tid));
    if (out->size < ALELTD_LOG_HDR_SIZE || (size_t)out->size > len - rec_off) {
        return ALELTD_E_FRAMING;
    }
    out->header_size = ALELTD_LOG_HDR_SIZE;
    if (out->rtype == ALELTD_RT_CLR) {
        out->header_size += 16u + ((out->flag & CLR_FLAG_EXTRA) ? 8u : 0u);
    }
    if (out->header_size > out->size) {
        return ALELTD_E_FRAMING;
    }
    out->length = (size_t)ALELTD_FILTER_HDR_SIZE + out->size;
    out->body_offset = rec_off + out->header_size;
    out->body_length = (size_t)out->size - out->header_size;
    *pos = rec_off + out->size;
    return ALELTD_OK;
}

int aleltd_dms_header_read(const uint8_t *body, size_t len, aleltd_dms_header *out)
{
    if (body == NULL || out == NULL) {
        return ALELTD_E_ARG;
    }
    if (len < DMS_HDR_SIZE) {
        return ALELTD_E_SHORT;
    }
    out->component = body[0];
    out->function = body[1];
    out->tbspaceid = aleltd_le16(body + 2);
    out->tableid = aleltd_le16(body + 4);
    return ALELTD_OK;
}

/* Sub-record at off; its own 4-byte header carries the length (bytes 2..3). */
static int subrecord(const uint8_t *b, size_t len, size_t off, size_t *sub_len)
{
    size_t ln;

    if (off > len || len - off < 4u) {
        return ALELTD_E_LAYOUT;
    }
    ln = aleltd_le16(b + off + 2);
    if (ln < 4u || ln > len - off) {
        return ALELTD_E_LAYOUT;
    }
    *sub_len = ln;
    return ALELTD_OK;
}

int aleltd_dms_images_read(const uint8_t *body, size_t len, const aleltd_dms_header *h, aleltd_dms_images *out)
{
    size_t off;
    size_t ln = 0;
    size_t ln2 = 0;
    size_t nxt;
    int rc;

    if (body == NULL || h == NULL || out == NULL) {
        return ALELTD_E_ARG;
    }
    memset(out, 0, sizeof(*out));
    switch (h->function) {
    case F_INSERT:
    case F_DELETE:
    case F_UNDO_DELETE:
    case F_UNDO_UPDATE:
    case F_INSERT_EMPTY:
    case F_DELETE_EMPTY:
    case F_UNDO_DELETE_EMPTY:
        off = DMS_HDR_SIZE + ((h->function >= F_DELETE_EMPTY) ? DMS_IMG_OFF_EMPTY : DMS_IMG_OFF);
        rc = subrecord(body, len, off, &ln);
        if (rc != ALELTD_OK) {
            return rc;
        }
        if (h->function == F_DELETE || h->function == F_DELETE_EMPTY) {
            out->has_before = 1;
            out->before_offset = off;
            out->before_length = ln;
        } else {
            out->has_after = 1;
            out->after_offset = off;
            out->after_length = ln;
        }
        return ALELTD_OK;
    case F_UPDATE:
        off = DMS_HDR_SIZE + DMS_IMG_OFF;
        rc = subrecord(body, len, off, &ln);
        if (rc != ALELTD_OK) {
            return rc;
        }
        nxt = off + ln;
        if (nxt > len || len - nxt < DMS_HDR_SIZE || body[nxt + 1] != F_UPDATE) {
            return ALELTD_E_LAYOUT;
        }
        rc = subrecord(body, len, nxt + DMS_HDR_SIZE + DMS_IMG_OFF, &ln2);
        if (rc != ALELTD_OK) {
            return rc;
        }
        out->has_before = 1;
        out->before_offset = off;
        out->before_length = ln;
        out->has_after = 1;
        out->after_offset = nxt + DMS_HDR_SIZE + DMS_IMG_OFF;
        out->after_length = ln2;
        return ALELTD_OK;
    case F_UNDO_INSERT:
    case F_UNDO_INSERT_EMPTY:
        return ALELTD_OK;
    default:
        return ALELTD_E_LAYOUT;
    }
}
