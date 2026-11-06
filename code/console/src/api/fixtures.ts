import type {
  Agent, AuditEvent, Batch, ColumnConfig, ImportRun, LadderResult, QuarantineItem, ReconRun, Retention, Revision,
  SchemaChange, SwitchRequest, TableCertification, TableDetail, UsersAndRoles, Wave, HubHealth, CertCriterion,
  TopologyStatus,
} from './contract';
import { CERT_CRITERIA } from './contract';
import { createImportMock, type ImportMockState } from './mockImport';

/** Deterministic fixtures modelled on the local ERPTEST database (schema APPSCHEMA). */
const NOW = Date.parse('2026-11-03T15:00:00Z');
export const iso = (offsetMin: number): string => new Date(NOW + offsetMin * 60_000).toISOString();
const pos = (part2: number) => ({ engine: 'db2', text: `1:${part2.toString(16).toUpperCase().padStart(16, '0')}` });

const col = (name: string, ordinal: number, engine_type: string, canonical_type: ColumnConfig['canonical_type'],
  o: Partial<ColumnConfig> = {}): ColumnConfig => ({
  name, ordinal, engine_type, canonical_type, length: null, scale: null, nullable: true, key_seq: null,
  enabled: true, hashed: false, ...o,
});

export interface MockDb {
  tables: TableDetail[];
  batches: Batch[];
  quarantine: QuarantineItem[];
  agents: Agent[];
  revisions: Revision[];
  imports: ImportRun[];
  schemaChanges: SchemaChange[];
  recon: ReconRun[];
  certification: TableCertification[];
  waves: Wave[];
  switchRequests: SwitchRequest[];
  hub: HubHealth;
  topology: TopologyStatus;
  retention: Retention;
  users: UsersAndRoles;
  audit: AuditEvent[];
  ladder: LadderResult[];
  importer: ImportMockState;
  seq: number;
}

function table(id: string, name: string, state: TableDetail['state'], columns: ColumnConfig[],
  o: Partial<TableDetail> = {}): TableDetail {
  return {
    id, table: { schema: 'APPSCHEMA', name }, dest_name: name, enabled: true, sync_mode: 'LOG', state,
    lag_seconds: 42, rows_per_day: 1200, active_source: 'LEGACY', certification: 'IN_PROGRESS',
    capture_enabled: true, revision: 3, columns, schema_history: [], recon: [], checkpoint: pos(0xa3f21), ...o,
  };
}

const criteria = (fail: CertCriterion[] = [], pending: CertCriterion[] = []) =>
  CERT_CRITERIA.map((c) => ({
    criterion: c,
    status: fail.includes(c) ? ('FAIL' as const) : pending.includes(c) ? ('PENDING' as const) : ('PASS' as const),
    value: c === 'LATENCY_P95' ? '74 s ≤ 120 s' : c === 'CLEAN_DAYS_7' ? '7/7' : 'match',
    evidence_url: null,
  }));

export function createMockDb(): MockDb {
  const tables: TableDetail[] = [
    table('t-client', 'CUSTOMER', 'IN_SYNC', [
      col('CUSTOMER_ID', 0, 'CHAR', 'STRING', { length: 10, nullable: false, key_seq: 1 }),
      col('NAME', 1, 'VARCHAR', 'STRING', { length: 60, nullable: false }),
      col('ADDRESS_1', 2, 'VARCHAR', 'STRING', { length: 80 }),
      col('CITY', 3, 'VARCHAR', 'STRING', { length: 40 }),
      col('PROVINCE', 4, 'CHAR', 'STRING', { length: 2 }),
      col('POSTAL_CODE', 5, 'VARCHAR', 'STRING', { length: 10 }),
      col('CREDIT_MAX', 6, 'DECIMAL', 'DECIMAL', { length: 13, scale: 2 }),
      col('IS_ACTIVE', 7, 'CHAR', 'STRING', { length: 1 }),
      col('CREATED_ON', 8, 'TIMESTAMP', 'LOCALDATETIME'),
      col('NOTES', 9, 'CLOB', 'STRING', { length: 65536 }),
    ], { certification: 'CERTIFIED', active_source: 'ALELT', rows_per_day: 300 }),
    table('t-orders', 'ORDERS', 'IN_SYNC', [
      col('ORDER_ID', 0, 'INTEGER', 'INT', { nullable: false, key_seq: 1 }),
      col('ORDER_NO', 1, 'VARCHAR', 'STRING', { length: 20, nullable: false }),
      col('CUSTOMER', 2, 'CHAR', 'STRING', { length: 10, nullable: false }),
      col('ORIGIN', 3, 'VARCHAR', 'STRING', { length: 40 }),
      col('DESTINATION', 4, 'VARCHAR', 'STRING', { length: 40 }),
      col('PICKUP_DUE', 5, 'DATE', 'LOCALDATE'),
      col('DELIVERY_DUE', 6, 'TIMESTAMP', 'LOCALDATETIME'),
      col('WEIGHT', 7, 'DECIMAL', 'DECIMAL', { length: 11, scale: 2 }),
      col('PIECES', 8, 'SMALLINT', 'SHORT'),
      col('CHARGES', 9, 'DECIMAL', 'DECIMAL', { length: 13, scale: 2 }),
      col('STATUS', 10, 'VARCHAR', 'STRING', { length: 10 }),
      col('MODIFIED_ON', 11, 'TIMESTAMP', 'LOCALDATETIME', { nullable: false }),
    ], { rows_per_day: 8400 }),
    table('t-driver', 'DRIVER', 'CATCHING_UP', [
      col('STAFF_ID', 0, 'VARCHAR', 'STRING', { length: 10, nullable: false, key_seq: 1 }),
      col('TERMINAL', 1, 'CHAR', 'STRING', { length: 4, nullable: false, key_seq: 2 }),
      col('FIRST_NAME', 2, 'VARCHAR', 'STRING', { length: 30 }),
      col('LAST_NAME', 3, 'VARCHAR', 'STRING', { length: 30 }),
      col('LICENSE_NO', 4, 'VARCHAR', 'STRING', { length: 20, hashed: true }),
      col('MILES_YTD', 5, 'BIGINT', 'LONG'),
      col('RATING', 6, 'DOUBLE', 'DOUBLE'),
      col('SHIFT_START', 7, 'TIME', 'LOCALTIME'),
    ], { lag_seconds: 310, rows_per_day: 90, certification: 'NOT_STARTED' }),
    table('t-order_status', 'ORDER_STATUS', 'SNAPSHOTTING', [
      col('ORDER_ID', 0, 'INTEGER', 'INT', { nullable: false }),
      col('STATUS_CODE', 1, 'VARCHAR', 'STRING', { length: 10, nullable: false }),
      col('CHANGED', 2, 'TIMESTAMP', 'LOCALDATETIME', { nullable: false }),
      col('INS_USER', 3, 'VARCHAR', 'STRING', { length: 128 }),
      col('COMMENTS', 4, 'VARCHAR', 'STRING', { length: 200 }),
    ], { lag_seconds: null, rows_per_day: 15000, certification: 'NOT_STARTED' }),
    table('t-nocapture', 'NO_CAPTURE', 'DISABLED', [
      col('ID', 0, 'INTEGER', 'INT', { nullable: false, key_seq: 1 }),
      col('VAL', 1, 'VARCHAR', 'STRING', { length: 20 }),
    ], { enabled: false, capture_enabled: false, lag_seconds: null, rows_per_day: 0, certification: 'NOT_STARTED', checkpoint: null }),
  ];

  const schemaChanges: SchemaChange[] = [
    { id: 'sc-1', table: { schema: 'APPSCHEMA', name: 'ORDERS' }, kind: 'ADD_COLUMN', detail: 'ADD COLUMN ASSET_ID VARCHAR(10)',
      detected_at: iso(-90), position: pos(0xa3e00), policy: 'REQUIRE_APPROVAL', status: 'PENDING' },
    { id: 'sc-2', table: { schema: 'APPSCHEMA', name: 'CUSTOMER' }, kind: 'ALTER_TYPE', detail: 'NAME VARCHAR(60) → VARCHAR(80)',
      detected_at: iso(-60 * 24 * 3), position: pos(0x9a000), policy: 'AUTO_APPLY', status: 'APPLIED' },
  ];
  tables[1]!.schema_history = [schemaChanges[0]!];
  tables[0]!.schema_history = [schemaChanges[1]!];

  const recon: ReconRun[] = [
    { id: 'rr-1', check_kind: 'ROW_COUNT', started_at: iso(-30), finished_at: iso(-29), results: [
      { id: 'r-1', run_id: 'rr-1', table_id: 't-client', table: { schema: 'APPSCHEMA', name: 'CUSTOMER' }, check_kind: 'ROW_COUNT',
        outcome: 'MATCH', detail: '5 = 5', at: iso(-29), drill: null },
      { id: 'r-2', run_id: 'rr-1', table_id: 't-driver', table: { schema: 'APPSCHEMA', name: 'DRIVER' }, check_kind: 'ROW_COUNT',
        outcome: 'MISMATCH', detail: 'source 12, destination 11', at: iso(-29),
        drill: { key: { STAFF_ID: 'D300', TERMINAL: 'VAN1' }, column: null, position: pos(0xa3f10) } },
    ] },
  ];
  tables[0]!.recon = [recon[0]!.results[0]!];
  tables[2]!.recon = [recon[0]!.results[1]!];

  return {
    tables,
    schemaChanges,
    recon,
    batches: [0, 1, 2, 3].map((i) => ({
      batch_id: `0000000${i}-5a1e-4c1a-9b7e-00000000000${i}`, agent_id: 'erpsrv01-agent', connection_id: 'erp-prod',
      from_pos: pos(0xa3000 + i * 0x100), to_pos: pos(0xa30ff + i * 0x100), rec_count: 120 + i, raw_bytes: 48_000 + i * 900,
      sha256: `${'ab'.repeat(31)}0${i}`, status: i === 0 ? ('RECEIVED' as const) : ('LOADED' as const),
      received_at: iso(-i * 2), loaded_at: i === 0 ? null : iso(-i * 2 + 1),
    })),
    quarantine: [
      { id: 'q-1', position: pos(0xa2ff0), table: { schema: 'APPSCHEMA', name: 'CUSTOMER' }, reason: 'UNKNOWN canonical type for column NOTES (inline LOB descriptor)',
        raw_ref: { batch_id: '00000003-5a1e-4c1a-9b7e-000000000003', offset: 4096, length: 312 },
        raw_hex: '38010000 4E000000 ...', at: iso(-120), status: 'OPEN' },
    ],
    agents: [{
      id: 'erpsrv01-agent', connection_id: 'erp-prod', engine: 'db2', hostname: 'ERPSRV01', database: 'ERPTEST', version: '0.1.0',
      state: 'CAPTURING', last_heartbeat_at: iso(-0.2), last_read_pos: pos(0xa3f40), durable_pos: pos(0xa3f21),
      spool_bytes: 12 * 1024 ** 2, spool_capacity_bytes: 2 * 1024 ** 3, cpu_pct: 3.4, mem_mb: 88,
      limits: { cpu_pct: 25, mem_mb: 512, batch_kb: 1024, max_rate_kbps: 0, poll_ms: 1000 },
      cert_thumbprint: '3F2A…9C1D', cert_expires_at: iso(60 * 24 * 21),
      governor_events: [{ at: iso(-300), kind: 'THROTTLE', detail: 'CPU 27% > cap 25% for 10 s' }],
    }],
    revisions: [
      { id: 1, author: 'alelt_import', created_at: iso(-60 * 24 * 10), reason: 'Initial import of existing schema', source: 'alelt_import', current: false },
      { id: 2, author: 'operator@example.test', created_at: iso(-60 * 24 * 5), reason: 'Enable DRIVER', source: 'console', current: false },
      { id: 3, author: 'operator@example.test', created_at: iso(-60 * 24), reason: 'Hash LICENSE_NO', source: 'console', current: true },
    ],
    imports: [],
    certification: tables.filter((t) => t.enabled).map((t) => ({
      table_id: t.id, table: t.table, status: t.certification, active_source: t.active_source,
      clean_days: t.certification === 'CERTIFIED' ? 9 : t.id === 't-orders' ? 7 : 2,
      criteria: t.certification === 'CERTIFIED' || t.id === 't-orders' ? criteria() : criteria(['ROW_COUNT_PARITY'], ['DBT_SHADOW_DIFF', 'CLEAN_DAYS_7']),
      signoffs: t.certification === 'CERTIFIED' ? [{ id: 's-1', user: 'operator@example.test', signed_at: iso(-60 * 24 * 2), comment: 'All §10 criteria green' }] : [],
    })),
    waves: [{ id: 'w-1', name: 'Wave 1 — low churn', table_ids: ['t-client', 't-orders'], planned_at: iso(60 * 24 * 2), status: 'PLANNED' }],
    switchRequests: [],
    hub: {
      services: [
        { name: 'hub-api', status: 'OK', detail: 'v0.1.0' },
        { name: 'worker-decode', status: 'OK', detail: '2 replicas' },
        { name: 'worker-load', status: 'OK', detail: '2 replicas' },
        { name: 'postgres', status: 'OK', detail: '16.4' },
        { name: 'destination', status: 'OK', detail: 'snowflake sandbox' },
      ],
      queues: [
        { name: 'decode', depth: 1, oldest_age_seconds: 4 },
        { name: 'load', depth: 0, oldest_age_seconds: 0 },
        { name: 'snapshot', depth: 3, oldest_age_seconds: 610 },
        { name: 'verify', depth: 0, oldest_age_seconds: 0 },
      ],
      spool_used_bytes: 3.2 * 1024 ** 3, spool_capacity_bytes: 200 * 1024 ** 3,
      destination: { engine: 'snowflake', account: 'example-sandbox', warehouse: 'ALELT_WH', credits_used_today: 1.8, credits_used_month: 21.4, credit_quota_month: 120 },
      credentials: [
        { name: 'snowflake-loader-keypair', kind: 'key_pair', rotated_at: iso(-60 * 24 * 40), expires_at: iso(60 * 24 * 50) },
        { name: 'hub-server-cert', kind: 'server_cert', rotated_at: iso(-60 * 24 * 100), expires_at: iso(60 * 24 * 265) },
        { name: 'agent-client-cert ERPSRV01', kind: 'client_cert', rotated_at: iso(-60 * 24 * 344), expires_at: iso(60 * 24 * 21) },
      ],
    },
    topology: {
      mode: 'standalone',
      layers: [
        { layer: 'api', ha: false, kind: '1 replica', detail: 'alelt api on hub-vm01', advice: 'Run 2+ API replicas behind a load balancer.' },
        { layer: 'workers', ha: true, kind: '2 replicas', detail: 'jobs claimed once; one worker per stream (advisory lock)', advice: null },
        { layer: 'metadata', ha: false, kind: 'sqlite', detail: 'sqlite:////data/alelt.db', advice: 'Move to PostgreSQL (managed or Patroni) for HA.' },
        { layer: 'spool', ha: false, kind: 'local', detail: 'file:///var/lib/alelt/spool', advice: 'Use an S3-compatible object store (s3://bucket/prefix).' },
        { layer: 'agents', ha: true, kind: 'active/passive', detail: 'erptest: 2 agents, lease epoch 3', advice: null },
        { layer: 'destination', ha: true, kind: 'snowflake', detail: 'checkpoint in the same transaction makes failover safe', advice: null },
      ],
      nodes: [
        { node_id: 'hub-vm01:api:1', role: 'api', host: 'hub-vm01', started_at: iso(-60 * 24), last_seen: iso(0), alive: true },
        { node_id: 'hub-vm01:worker:1', role: 'worker', host: 'hub-vm01', started_at: iso(-60 * 24), last_seen: iso(0), alive: true },
        { node_id: 'hub-vm01:worker:2', role: 'worker', host: 'hub-vm01', started_at: iso(-60 * 24), last_seen: iso(-30), alive: false },
      ],
      connections: [
        {
          connection_id: 'erptest', agent_mode: 'active_passive', holder: 'edge-01', epoch: 3,
          lease_id: '7d0b6f8e-2f7e-4c4b-9a43-3f0f3f8d9a01', expires_at: iso(0.4),
          agents: [
            { agent_id: 'edge-01', hostname: 'ERPSRV01', state: 'CAPTURING', last_heartbeat: iso(0), role: 'active' },
            { agent_id: 'edge-02', hostname: 'ERPSRV02', state: 'STANDBY', last_heartbeat: iso(0), role: 'standby' },
          ],
        },
      ],
      operation: null,
      install_options: [
        { mode: 'standalone', title: 'Single host (non-HA)', when: 'Evaluation, small installs, one VM; restart = minutes of capture pause (agents spool, nothing lost).',
          command: 'cd deploy/local && docker compose up -d', doc: 'deploy/local/README.md' },
        { mode: 'ha', title: 'High availability (2+ hosts / Kubernetes)', when: 'Production: no single point of failure; needs PostgreSQL HA, an S3-compatible store and a load balancer.',
          command: 'helm install alelt deploy/helm/alelt -f values.yaml', doc: 'deploy/HA.md' },
      ],
    },
    retention: {
      spool_cap_bytes: 200 * 1024 ** 3, raw_archive_days: 60, wal_archiving_ok: true,
      postgres_backups: [
        { at: iso(-60 * 13), kind: 'FULL', size_bytes: 820 * 1024 ** 2, status: 'OK' },
        { at: iso(-5), kind: 'WAL', size_bytes: 16 * 1024 ** 2, status: 'OK' },
      ],
    },
    users: {
      group_mappings: [
        { group_id: '11111111-1111-1111-1111-111111111111', group_name: 'ALELT-Viewers', role: 'Viewer' },
        { group_id: '22222222-2222-2222-2222-222222222222', group_name: 'ALELT-DataOperators', role: 'DataOperator' },
        { group_id: '33333333-3333-3333-3333-333333333333', group_name: 'ALELT-PlatformAdmins', role: 'PlatformAdmin' },
        { group_id: '44444444-4444-4444-4444-444444444444', group_name: 'ALELT-Auditors', role: 'Auditor' },
      ],
      users: [
        { id: 'u-1', email: 'operator@example.test', name: 'Data Operator', roles: ['DataOperator'], last_seen: iso(-10) },
        { id: 'u-2', email: 'approver@example.test', name: 'Second Approver', roles: ['DataOperator'], last_seen: iso(-60) },
        { id: 'u-3', email: 'platform@example.test', name: 'Platform Admin', roles: ['PlatformAdmin'], last_seen: iso(-5) },
      ],
    },
    audit: [
      { id: 'a-1', at: iso(-60 * 24), actor: 'operator@example.test', role: 'DataOperator', action: 'table.config.update',
        target: 'APPSCHEMA.DRIVER', detail: { revision: 3, reason: 'Hash LICENSE_NO' }, correlation_id: 'console-seed-1' },
      { id: 'a-2', at: iso(-300), actor: 'system', role: 'system', action: 'agent.governor.throttle',
        target: 'erpsrv01-agent', detail: { cpu_pct: 27 }, correlation_id: null },
    ],
    ladder: [
      ['L0', 'Agent service lifecycle'], ['L1', 'db2ReadLog QUERY + catalog'], ['L2', 'Raw blocks for scripted DML'],
      ['L3', 'Decoder byte-exact'], ['L4', 'End-to-end to destination sandbox'], ['L5', 'Type inventory + fuzz'],
      ['L6', 'Lab load + chaos'], ['L7', 'Production shadow, one table'], ['L8', 'All tables, certification'],
      ['L9', 'Switch + rollback drills'], ['L10', 'Legacy pipeline decommission'],
    ].map(([level, name], i) => ({
      schema_version: 1 as const, level: level as LadderResult['level'], name: name!, environment: i < 6 ? ('dev' as const) : i === 6 ? ('lab' as const) : ('prod' as const),
      status: i < 2 ? ('PASS' as const) : ('NOT_RUN' as const), started_at: i < 2 ? iso(-200) : null, finished_at: i < 2 ? iso(-199) : null,
      host: i < 2 ? 'dev-laptop' : '', checks: [], artifacts: [], exit_criterion: '',
    })),
    importer: createImportMock(),
    seq: 100,
  };
}
