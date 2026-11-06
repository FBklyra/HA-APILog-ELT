/* Internal helpers: explicit little-endian reads (no unaligned access, host-endian independent). */
#ifndef ALELTD_INTERNAL_H
#define ALELTD_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

static inline uint16_t aleltd_le16(const uint8_t *p)
{
    return (uint16_t)((uint16_t)p[0] | (uint16_t)((uint16_t)p[1] << 8));
}

static inline uint32_t aleltd_le32(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static inline uint64_t aleltd_le64(const uint8_t *p)
{
    return (uint64_t)aleltd_le32(p) | ((uint64_t)aleltd_le32(p + 4) << 32);
}

#endif /* ALELTD_INTERNAL_H */
