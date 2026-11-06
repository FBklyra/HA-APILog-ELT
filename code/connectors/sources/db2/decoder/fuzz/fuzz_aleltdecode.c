/* libFuzzer target (clang / clang-cl with -fsanitize=fuzzer,address): frame + decode arbitrary bytes. */
#include "aleltdecode.h"

#include <stddef.h>
#include <stdint.h>

static const aleltd_column k_cols[] = {
    {ALELTD_T_CHAR, 10, 0, 0},      {ALELTD_T_VARCHAR, 60, 0, 1},    {ALELTD_T_DECIMAL, 13, 2, 1},
    {ALELTD_T_TIMESTAMP, 10, 6, 1}, {ALELTD_T_CLOB, 65536, 0, 1},    {ALELTD_T_BIGINT, 8, 0, 1},
    {ALELTD_T_DOUBLE, 8, 0, 1},     {ALELTD_T_DATE, 4, 0, 1},        {ALELTD_T_TIME, 3, 0, 1},
    {ALELTD_T_BOOLEAN, 1, 0, 1},    {ALELTD_T_TIMESTAMP, 13, 12, 1}, {ALELTD_T_GRAPHIC, 3, 0, 1}};

#define NCOLS (sizeof(k_cols) / sizeof(k_cols[0]))

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    aleltd_record r;
    aleltd_dms_header h;
    aleltd_dms_images im;
    aleltd_cell cells[NCOLS];
    size_t pos = 0;

    (void)aleltd_decode_row(data, size, k_cols, NCOLS, cells, NULL);
    while (aleltd_next_record(data, size, &pos, &r) == ALELTD_OK) {
        const uint8_t *body = data + r.body_offset;
        if (aleltd_dms_header_read(body, r.body_length, &h) != ALELTD_OK ||
            aleltd_dms_images_read(body, r.body_length, &h, &im) != ALELTD_OK) {
            continue;
        }
        if (im.has_before) {
            (void)aleltd_decode_row(body + im.before_offset, im.before_length, k_cols, NCOLS, cells, NULL);
        }
        if (im.has_after) {
            (void)aleltd_decode_row(body + im.after_offset, im.after_length, k_cols, NCOLS, cells, NULL);
        }
    }
    return 0;
}
