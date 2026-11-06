import { describe, expect, it } from 'vitest';
import { bytes, daysUntil, duration, toCsv } from './format';

describe('format', () => {
  it('bytes', () => {
    expect(bytes(512)).toBe('512 B');
    expect(bytes(2 * 1024 ** 3)).toBe('2 GB');
  });
  it('duration', () => {
    expect(duration(null)).toBe('—');
    expect(duration(42)).toBe('42 s');
    expect(duration(310)).toBe('5 min 10 s');
    expect(duration(7200)).toBe('2 h 0 min');
  });
  it('daysUntil', () => {
    expect(daysUntil('2026-12-03T00:00:00Z', new Date('2026-11-03T00:00:00Z'))).toBe(30);
  });
  it('toCsv quotes per RFC 4180 and serialises objects', () => {
    const csv = toCsv([{ a: 'x,y', b: 'he said "hi"', c: { k: 1 } }, { a: null }], ['a', 'b', 'c']);
    expect(csv).toBe('a,b,c\r\n"x,y","he said ""hi""","{""k"":1}"\r\n,,\r\n');
  });
});
