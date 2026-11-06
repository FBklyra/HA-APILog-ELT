/* Minimal self-checking unit test for libaleltdecode. */
#include "aleltdecode.h"

#include <stdio.h>
#include <string.h>

static int failures = 0;

#define CHECK(cond)                                                                  \
    do {                                                                             \
        if (!(cond)) {                                                               \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                              \
        }                                                                            \
    } while (0)

/* ALELTTEST.DRIVER insert (D100, TOR1, Alex, Martin, ON-A1234, 82000, 4.8, 06:00:00) taken from the corpus. */
static const uint8_t k_record[] = {
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3c, 0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x8b, 0x14, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd7, 0x13, 0x33, 0x05, 0x00, 0x00, 0x00, 0x00,
    0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x87, 0x00, 0x00, 0x00, 0x4e, 0x00, 0x02, 0x00,
    0x8b, 0x14, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3c, 0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0xd5, 0x12, 0x33, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x98, 0x00, 0x00,
    0x01, 0xa2, 0x02, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x4b, 0x00, 0xc3, 0x1a, 0x04, 0x00, 0x00, 0x00,
    0x00, 0x00, 0xb2, 0x19, 0x00, 0x00, 0x4b, 0x00, 0x01, 0x00, 0x2d, 0x00, 0x2d, 0x00, 0x04, 0x00,
    0x54, 0x4f, 0x52, 0x31, 0x31, 0x00, 0x04, 0x00, 0x00, 0x35, 0x00, 0x06, 0x00, 0x00, 0x3b, 0x00,
    0x08, 0x00, 0x00, 0x50, 0x40, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x33, 0x33, 0x33,
    0x33, 0x33, 0x13, 0x40, 0x00, 0x06, 0x00, 0x00, 0x00, 0x44, 0x31, 0x30, 0x30, 0x41, 0x6c, 0x65,
    0x78, 0x4d, 0x61, 0x72, 0x74, 0x69, 0x6e, 0x4f, 0x4e, 0x2d, 0x41, 0x31, 0x32, 0x33, 0x34};

static const aleltd_column k_driver[] = {
    {ALELTD_T_VARCHAR, 10, 0, 0}, {ALELTD_T_CHAR, 4, 0, 0},     {ALELTD_T_VARCHAR, 30, 0, 1},
    {ALELTD_T_VARCHAR, 30, 0, 1}, {ALELTD_T_VARCHAR, 20, 0, 1}, {ALELTD_T_BIGINT, 8, 0, 1},
    {ALELTD_T_DOUBLE, 8, 0, 1},   {ALELTD_T_TIME, 3, 0, 1}};

#define NCOLS (sizeof(k_driver) / sizeof(k_driver[0]))

static int bytes_eq(const uint8_t *sub, const aleltd_cell *c, const char *s)
{
    size_t n = strlen(s);
    return c->kind == ALELTD_K_BYTES && c->length == n && memcmp(sub + c->offset, s, n) == 0;
}

static void test_record(void)
{
    aleltd_record r;
    aleltd_dms_header h;
    aleltd_dms_images im;
    aleltd_cell cells[NCOLS];
    size_t pos = 0;
    size_t err_col = 0;
    const uint8_t *body;
    const uint8_t *sub;

    CHECK(aleltd_next_record(k_record, sizeof(k_record), &pos, &r) == ALELTD_OK);
    CHECK(r.rtype == ALELTD_RT_NORMAL && r.size == 0x87u && r.lri_part1 == 0x163cu && pos == sizeof(k_record));
    CHECK(aleltd_next_record(k_record, sizeof(k_record), &pos, &r) == ALELTD_END);
    body = k_record + r.body_offset;
    CHECK(aleltd_dms_header_read(body, r.body_length, &h) == ALELTD_OK);
    CHECK(h.component == 1u && h.function == 162u && h.tbspaceid == 2u && h.tableid == 11u);
    CHECK(aleltd_dms_images_read(body, r.body_length, &h, &im) == ALELTD_OK);
    CHECK(im.has_after && !im.has_before && im.after_length == 75u);
    sub = body + im.after_offset;
    CHECK(aleltd_decode_row(sub, im.after_length, k_driver, NCOLS, cells, &err_col) == ALELTD_OK);
    CHECK(bytes_eq(sub, &cells[0], "D100") && bytes_eq(sub, &cells[1], "TOR1"));
    CHECK(bytes_eq(sub, &cells[2], "Alex") && bytes_eq(sub, &cells[3], "Martin"));
    CHECK(bytes_eq(sub, &cells[4], "ON-A1234"));
    CHECK(cells[5].kind == ALELTD_K_INT && cells[5].i == 82000);
    CHECK(cells[6].kind == ALELTD_K_FLOAT && cells[6].f == 4.8);
    CHECK(cells[7].kind == ALELTD_K_TEXT && strcmp(cells[7].text, "06:00:00") == 0);
}

static void test_truncation(void)
{
    size_t n;

    for (n = 0; n < sizeof(k_record); n++) {
        aleltd_record r;
        size_t pos = 0;
        int rc = aleltd_next_record(k_record, n, &pos, &r);
        CHECK(n == 0 ? rc == ALELTD_END : rc == ALELTD_E_FRAMING);
    }
}

static void test_values(void)
{
    /* sub-record: header (0, 0, len16), user data header (1, 0, fixed16), fixed section */
    static const uint8_t fixed[] = {0x00, 0x12, 0x3d, 0x00, 0x80, 0x00, 0x20, 0x26,
                                    0x01, 0x02, 0x03, 0x04, 0x05, 0x67, 0x80};
    const aleltd_column cols[4] = {{ALELTD_T_DECIMAL, 5, 2, 0}, {ALELTD_T_SMALLINT, 2, 0, 1},
                                {ALELTD_T_TIMESTAMP, 10, 3, 0}, {ALELTD_T_INTEGER, 4, 0, 1}};
    aleltd_cell cells[4];
    uint8_t sub[64];
    size_t len = 8u + sizeof(fixed);

    memset(sub, 0, sizeof(sub));
    sub[2] = (uint8_t)len;
    sub[4] = 1;
    sub[6] = (uint8_t)sizeof(fixed);
    memcpy(sub + 8, fixed, sizeof(fixed));
    CHECK(aleltd_decode_row(sub, len, cols, 4, cells, NULL) == ALELTD_OK);
    CHECK(cells[0].kind == ALELTD_K_TEXT && strcmp(cells[0].text, "-1.23") == 0);
    CHECK(cells[1].kind == ALELTD_K_INT && cells[1].i == -32768);
    CHECK(cells[2].kind == ALELTD_K_TEXT && strcmp(cells[2].text, "2026-01-02T03:04:05.678") == 0);
    CHECK(cells[3].kind == ALELTD_K_ABSENT); /* row written before ADD COLUMN */
    sub[8 + 5] = 2;                       /* bad null flag of SMALLINT */
    CHECK(aleltd_decode_row(sub, len, cols, 4, cells, NULL) == ALELTD_E_BAD_NULL_FLAG);
    sub[8 + 5] = 0;
    sub[8 + 1] = 0xA2;                    /* bad BCD digit */
    CHECK(aleltd_decode_row(sub, len, cols, 4, cells, NULL) == ALELTD_E_BAD_BCD);
    CHECK(aleltd_decode_row(NULL, len, cols, 4, cells, NULL) == ALELTD_E_ARG);
}

int main(void)
{
    CHECK(aleltd_abi_version() == ALELTD_ABI_VERSION);
    test_record();
    test_truncation();
    test_values();
    if (failures != 0) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    puts("aleltdecode tests passed");
    return 0;
}
