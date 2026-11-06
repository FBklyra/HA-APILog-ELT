import { describe, expect, it } from 'vitest';
import type { HostMapEntry } from '../api/contract';
import { hostMapComplete, hostMapRows, mappingOf, targetsToCheck, validTestHost } from './hostMap';
import { positionExample, positionHintKey, startCaptureArgs } from './startCapture';

const entry = (host: string): HostMapEntry => ({ host, connections: ['c1'], sides: ['source'], prod_denied: true, test_host: null, test_denied: false });
const hosts = [entry('db2prod.corp.local'), entry('acct.snowflakecomputing.com')];

describe('host map step (REQ-24 / X-117)', () => {
  it('accepts host names and addresses only (hub rule)', () => {
    expect(validTestHost('db2test.corp.local')).toBe(true);
    expect(validTestHost('10.0.0.5')).toBe(true);
    for (const bad of ['', '  ', 'https://x', 'a b', 'u@h', 'h/p', 'h?q', 'h#f']) expect(validTestHost(bad), bad).toBe(false);
  });
  it('apply stays disabled until every host is mapped and checked clean', () => {
    let rows = hostMapRows(hosts, {}, {});
    expect(rows.map((r) => r.state)).toEqual(['missing', 'missing']);
    expect(hostMapComplete(rows)).toBe(false);
    const drafts = { 'db2prod.corp.local': ' db2test.corp.local ', 'acct.snowflakecomputing.com': 'db2prod.corp.local' };
    expect(targetsToCheck(hosts, drafts, {})).toEqual(['db2test.corp.local', 'db2prod.corp.local']);
    rows = hostMapRows(hosts, drafts, {});
    expect(rows.map((r) => r.state)).toEqual(['checking', 'checking']);
    rows = hostMapRows(hosts, drafts, { 'db2test.corp.local': null, 'db2prod.corp.local': 'db2prod.corp.local' });
    expect(rows.map((r) => r.state)).toEqual(['ok', 'denied']);
    expect(rows[1]?.deniedBy).toBe('db2prod.corp.local');
    expect(hostMapComplete(rows)).toBe(false);
    rows = hostMapRows(hosts, { ...drafts, 'acct.snowflakecomputing.com': 'test-acct' }, { 'db2test.corp.local': null, 'test-acct': null });
    expect(hostMapComplete(rows)).toBe(true);
    expect(mappingOf(rows)).toEqual({ 'db2prod.corp.local': 'db2test.corp.local', 'acct.snowflakecomputing.com': 'test-acct' });
  });
  it('invalid targets are never sent to the guard check', () => {
    expect(targetsToCheck(hosts, { 'db2prod.corp.local': 'jdbc://x' }, {})).toEqual([]);
    expect(hostMapRows(hosts, { 'db2prod.corp.local': 'jdbc://x' }, {})[0]?.state).toBe('invalid');
  });
  it('no hosts = nothing to map', () => {
    expect(hostMapComplete(hostMapRows([], {}, {}))).toBe(true);
  });
});

describe('START_CAPTURE args (REQ-10 / REQ-24)', () => {
  it('resume is the default form; explicit position must be non-empty', () => {
    expect(startCaptureArgs('resume', 'ignored')).toEqual({ resume: true });
    expect(startCaptureArgs('from_pos', ' 1:00000000000A3F21 ')).toEqual({ from_pos: '1:00000000000A3F21' });
    expect(startCaptureArgs('from_pos', '   ')).toBeNull();
  });
  it('format hint by engine, example = durable position text (never parsed)', () => {
    expect(positionHintKey('DB2')).toBe('agent.start.hint.db2');
    expect(positionHintKey('postgres')).toBe('agent.start.hint.generic');
    expect(positionExample({ engine: 'postgres', text: '0/16B3748' })).toBe('0/16B3748');
    expect(positionExample(null)).toBe('—');
  });
});
