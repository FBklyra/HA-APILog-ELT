/**
 * ALELT hub console API contract — `/api/v1` (mirrors hub/alelt_common/models.py).
 * Human-readable twin: console/API_CONTRACT.md. Keep both in sync; bump CONTRACT_VERSION on breaking change.
 *
 * Conventions
 * - JSON, snake_case fields, timestamps ISO-8601 UTC strings (`...Z`), ids are strings (UUID unless noted).
 * - Auth: `Authorization: Bearer <Entra ID access token>`. Dev only: `X-ALELT-Dev-Role: <Role>` (hub honours it only when ALELT_DEV_AUTH=1).
 * - Every request may carry `X-ALELT-Correlation-Id`; the hub echoes it and logs it on the audit event.
 * - Every mutating call requires `reason` and writes an `audit_event`. Destructive calls also require `confirm_text`.
 * - Errors: HTTP 4xx/5xx with body `ApiError`. 403 = role lacks permission; 409 = state conflict (e.g. two-person rule).
 * - Secrets are never returned: credential endpoints expose metadata (name, rotated_at, expires_at) only.
 */

import type { DiagnoseEndpoints } from './diagnose';

export const CONTRACT_VERSION = 1;
export const API_BASE = '/api/v1';

// ------------------------------------------------------------------ shared primitives
export type Iso8601 = string;
export type Id = string;

export const ROLES = ['Viewer', 'DataOperator', 'PlatformAdmin', 'Auditor'] as const;
export type Role = (typeof ROLES)[number];

/** alelt_common.models.Position — opaque; Db2 type-1 LRI text '<part1>:<part2 hex16>'. Never parse in the console. */
export interface Position {
  engine: string;
  text: string;
}

/** alelt_common.models.TableRef (wire field is `schema`). */
export interface TableRef {
  schema: string;
  name: string;
}

export const CANONICAL_TYPES = [
  'BOOLEAN', 'SHORT', 'INT', 'LONG', 'DECIMAL', 'FLOAT', 'DOUBLE', 'LOCALDATE', 'LOCALTIME',
  'LOCALDATETIME', 'INSTANT', 'STRING', 'BINARY', 'JSON', 'XML', 'UNKNOWN',
] as const;
export type CanonicalType = (typeof CANONICAL_TYPES)[number];

export interface Page<T> {
  items: T[];
  total: number;
  next_cursor: string | null;
}

export interface ApiError {
  error: { code: string; message: string; correlation_id: string | null };
}

/** Body fields common to every mutating request. */
export interface Mutation {
  reason: string;
}
/** Destructive mutations: `confirm_text` must equal the server-computed expected text (see expectedConfirmText). */
export interface DestructiveMutation extends Mutation {
  confirm_text: string;
}

// ------------------------------------------------------------------ identity — GET /me
export interface Me {
  oid: string;
  name: string;
  email: string;
  roles: Role[];
  /** true when authenticated through dev role header (never true in production). */
  dev: boolean;
}

// ------------------------------------------------------------------ output profile (system columns)
/**
 * Destination system-column naming. Core profiles: `alelt` (default, _ALELT_SYNCED/_ALELT_DELETED) and `golden_spec`
 * (names taken from the golden spec of the existing landing schema). Extensions may register further profile names,
 * so the console treats the profile as an opaque string and displays the resolved `system_columns` from the hub.
 */
export type OutputProfile = string;
export const DEFAULT_SYSTEM_COLUMNS = { synced: '_ALELT_SYNCED', deleted: '_ALELT_DELETED' } as const;
export interface SystemColumns {
  synced: string;
  deleted: string;
}

// ------------------------------------------------------------------ overview — GET /overview
export type Health = 'OK' | 'DEGRADED' | 'DOWN' | 'UNKNOWN';
export type Severity = 'info' | 'warning' | 'critical';

export interface Alert {
  id: Id;
  name: string; // Prometheus alert name, e.g. AleltCaptureLagHigh
  severity: Severity;
  summary: string;
  since: Iso8601;
}

export interface Overview {
  pipeline_health: Health;
  capture_lag_seconds: number;
  capture_lag_lri_distance: number | null;
  last_batch: { batch_id: Id; received_at: Iso8601; to_pos: Position } | null;
  latency_p95_seconds: number | null;   // recorded commit -> apply p95 (24 h); null = no evidence
  latency_target_seconds: number;
  open_alerts: Alert[];
  /** 0..1 — share of in-scope tables whose latest parity checks all match. */
  parity_score: number;
  tables_total: number;
  tables_in_sync: number;
  output_profile: OutputProfile;
  system_columns: SystemColumns;
  ladder: LadderResult[];
}

// ------------------------------------------------------------------ tables — GET /tables, GET/PUT /tables/{id}
export const TABLE_STATES = ['NEW', 'SNAPSHOTTING', 'CATCHING_UP', 'IN_SYNC', 'PAUSED', 'ERROR', 'DISABLED'] as const;
export type TableState = (typeof TABLE_STATES)[number];
export type SyncMode = 'LOG' | 'SNAPSHOT_ONLY' | 'HISTORY';
/** LEGACY = the pipeline ALELT replaces (stays live until decommission); ALELT = this platform. */
export type ActiveSource = 'LEGACY' | 'ALELT';
export type CertificationStatus = 'NOT_STARTED' | 'IN_PROGRESS' | 'CERTIFIED' | 'REVOKED';

export interface TableSummary {
  id: Id;
  table: TableRef;
  dest_name: string;
  enabled: boolean;
  sync_mode: SyncMode;
  state: TableState;
  lag_seconds: number | null;
  rows_per_day: number;
  active_source: ActiveSource;
  certification: CertificationStatus;
  capture_enabled: boolean; // Db2 DATA CAPTURE CHANGES present
  /** Table lifecycle (ADR-012): NEW, INITIAL_LOAD, CATCH_UP, STREAMING, RESYNC, DISABLED; null before the lifecycle. */
  phase?: string | null;
  /** Out-of-row values still being completed by key (ADR-013); blocks certification while > 0. */
  pending_completions?: number;
}

export interface ColumnConfig {
  name: string;
  ordinal: number;
  engine_type: string;
  canonical_type: CanonicalType;
  length: number | null;
  scale: number | null;
  nullable: boolean;
  key_seq: number | null;
  enabled: boolean;
  hashed: boolean;
}

export interface TableDetail extends TableSummary {
  revision: number;
  columns: ColumnConfig[];
  schema_history: SchemaChange[];
  recon: ReconResult[];
  checkpoint: Position | null;
}

/** PUT /tables/{id}/config — creates a new config revision. */
export interface TableConfigUpdate extends Mutation {
  enabled?: boolean;
  sync_mode?: SyncMode;
  columns?: { name: string; enabled: boolean; hashed: boolean }[];
}

/** POST /tables/{id}/resync (destructive: confirm_text = table FQN). */
export type ResyncRequest = DestructiveMutation;
/** POST /tables/{id}/reprocess — re-decode from raw archive (no Db2 access). */
export interface ReprocessRequest extends DestructiveMutation {
  from_pos: string;
  to_pos: string;
}

// ------------------------------------------------------------------ batches & quarantine
export type BatchStatus = 'RECEIVED' | 'DECODED' | 'LOADED' | 'FAILED' | 'DUPLICATE';
/** GET /batches?status=&cursor= */
export interface Batch {
  batch_id: Id;
  agent_id: string;
  connection_id: string;
  from_pos: Position;
  to_pos: Position;
  rec_count: number;
  raw_bytes: number;
  sha256: string;
  status: BatchStatus;
  received_at: Iso8601;
  loaded_at: Iso8601 | null;
}

export interface RawRef {
  batch_id: Id;
  offset: number;
  length: number;
}
/** GET /quarantine?status=  — mirrors alelt_common.QuarantineRecord + lifecycle. */
export interface QuarantineItem {
  id: Id;
  position: Position | null;
  table: TableRef | null;
  reason: string;
  raw_ref: RawRef | null;
  raw_hex: string | null;
  at: Iso8601;
  status: 'OPEN' | 'REPROCESSED' | 'IGNORED';
}

// ------------------------------------------------------------------ agents — GET /agents, POST /agents/{id}/commands
/** alelt_common.models.CommandName — whitelist (R5). */
export const COMMAND_NAMES = [
  'START_CAPTURE', 'PAUSE', 'RESUME', 'SET_LIMITS', 'SNAPSHOT_CATALOG', 'SNAPSHOT_TABLE', 'PROBE', 'SHUTDOWN',
] as const;
export type CommandName = (typeof COMMAND_NAMES)[number];
export const PROBES = ['current_position', 'db_health', 'log_config'] as const;

export type AgentState = 'IDLE' | 'CAPTURING' | 'PAUSED' | 'PAUSED_BY_GOVERNOR' | 'SNAPSHOTTING' | 'SPOOL_FULL' | 'ERROR' | 'OFFLINE';

/** alelt_common.models.Limits */
export interface Limits {
  cpu_pct: number;
  mem_mb: number;
  batch_kb: number;
  max_rate_kbps: number;
  poll_ms: number;
}

export interface GovernorEvent {
  at: Iso8601;
  kind: 'THROTTLE' | 'PAUSE' | 'RESUME';
  detail: string;
}

export interface Agent {
  id: string;
  connection_id: string;
  engine: string;
  hostname: string;
  database: string;
  version: string;
  state: AgentState;
  last_heartbeat_at: Iso8601 | null;
  last_read_pos: Position | null;
  durable_pos: Position | null;
  spool_bytes: number;
  spool_capacity_bytes: number;
  cpu_pct: number;
  mem_mb: number;
  limits: Limits;
  cert_thumbprint: string;
  cert_expires_at: Iso8601;
  governor_events: GovernorEvent[];
}

/**
 * START_CAPTURE args (REQ-10/REQ-24): `{resume: true}` = agent resumes from its durable position (local spool or hub,
 * else the log end), or `{from_pos}` = explicit opaque position text (same format as `Agent.durable_pos.text`).
 * The hub refuses an empty `from_pos` (400 INVALID_ARGS).
 */
export type StartCaptureArgs = { resume: true } | { from_pos: string };

/** POST /agents/{id}/commands. PAUSE/SHUTDOWN are destructive (confirm_text = agent id). SET_LIMITS args = Partial<Limits>. START_CAPTURE args = StartCaptureArgs. */
export interface AgentCommandRequest extends Mutation {
  name: CommandName;
  args: Record<string, unknown>;
  confirm_text?: string;
}
export interface CommandAccepted {
  command_id: Id;
  status: 'QUEUED' | 'SENT' | 'ACKED' | 'REJECTED';
}

// ------------------------------------------------------------------ revisions — GET /revisions, /revisions/{id}/diff, POST /revisions/{id}/rollback, GET /revisions/{id}/export
export interface DiffEntry {
  path: string; // e.g. tables.APPSCHEMA.CUSTOMER.columns.NOTES.hashed
  op: 'add' | 'remove' | 'change';
  before: unknown;
  after: unknown;
}
export interface Revision {
  id: number;
  author: string;
  created_at: Iso8601;
  reason: string;
  source: 'console' | 'alelt_import' | 'git' | 'rollback';
  current: boolean;
}

// ------------------------------------------------------------------ schema import — POST /import/runs, POST /import/runs/{id}/accept
export interface ImportRun {
  id: Id;
  started_at: Iso8601;
  finished_at: Iso8601 | null;
  status: 'RUNNING' | 'DONE' | 'FAILED' | 'ACCEPTED';
  source_ref: string;
  diff: DiffEntry[];
  message: string | null;
}

// ------------------------------------------------------------------ schema changes — GET /schema-changes, POST /schema-changes/{id}/decision
export type SchemaChangeKind = 'ADD_COLUMN' | 'DROP_COLUMN' | 'ALTER_TYPE' | 'RENAME_COLUMN' | 'CAPTURE_DISABLED' | 'NEW_TABLE';
export interface SchemaChange {
  id: Id;
  table: TableRef;
  kind: SchemaChangeKind;
  detail: string;
  detected_at: Iso8601;
  position: Position | null;
  policy: 'AUTO_APPLY' | 'REQUIRE_APPROVAL' | 'BLOCK';
  status: 'PENDING' | 'APPROVED' | 'REJECTED' | 'APPLIED';
}
export interface SchemaDecision extends Mutation {
  decision: 'APPROVE' | 'REJECT';
}

// ------------------------------------------------------------------ reconciliation — GET /recon, POST /recon/runs
export type ReconCheck = 'ROW_COUNT' | 'KEY_SET' | 'COLUMN_CHECKSUM' | 'DELETES' | 'LEGACY_PARITY';
export interface ReconResult {
  id: Id;
  run_id: Id;
  table_id: Id;
  table: TableRef;
  check_kind: ReconCheck;
  outcome: 'MATCH' | 'MISMATCH' | 'ERROR';
  detail: string;
  at: Iso8601;
  /** Drill-down to the first offending key/column/position, when known. */
  drill: { key: Record<string, unknown> | null; column: string | null; position: Position | null } | null;
}
export interface ReconRun {
  id: Id;
  check_kind: ReconCheck;
  started_at: Iso8601;
  finished_at: Iso8601 | null;
  results: ReconResult[];
}
export interface ReconRunRequest extends Mutation {
  table_ids: Id[];
  check_kind: ReconCheck;
}

// ------------------------------------------------------------------ certification & cutover — GET /certification, /certification/waves
/** DESIGN.md §8.3 criteria. */
export const CERT_CRITERIA = [
  'ROW_COUNT_PARITY', 'PK_SET_PARITY', 'COLUMN_CHECKSUM_PARITY', 'DELETE_PARITY', 'LATENCY_P95', 'DBT_SHADOW_DIFF', 'CLEAN_DAYS_7',
] as const;
export type CertCriterion = (typeof CERT_CRITERIA)[number];

export interface CriterionResult {
  criterion: CertCriterion;
  status: 'PASS' | 'FAIL' | 'PENDING';
  value: string;
  evidence_url: string | null;
}
export interface Signoff {
  id: Id;
  user: string;
  signed_at: Iso8601;
  comment: string;
}
export interface TableCertification {
  table_id: Id;
  table: TableRef;
  status: CertificationStatus;
  clean_days: number;
  criteria: CriterionResult[];
  signoffs: Signoff[];
  active_source: ActiveSource;
}
/** POST /certification/{table_id}/signoff — allowed only when every criterion is PASS. */
export type SignoffRequest = Mutation;

export interface Wave {
  id: Id;
  name: string;
  table_ids: Id[];
  planned_at: Iso8601 | null;
  status: 'PLANNED' | 'PENDING_APPROVAL' | 'SWITCHED' | 'ROLLED_BACK';
}
/** POST /certification/waves */
export interface WaveCreate extends Mutation {
  name: string;
  table_ids: Id[];
  planned_at: Iso8601 | null;
}

// ------------------------------------------------------------------ switch (two-person rule) — POST /switch, GET /switch/requests, POST /switch/requests/{id}/approve|reject
export type SwitchScope = { table_id: Id } | { wave_id: Id };
/** Request a switch (target ALELT) or rollback (target LEGACY). confirm_text = expectedConfirmText('switch', scopeLabel). */
export interface SwitchRequestCreate extends DestructiveMutation {
  scope: SwitchScope;
  target: ActiveSource;
}
export interface SwitchRequest {
  id: Id;
  scope: SwitchScope;
  scope_label: string; // table FQN or wave name
  target: ActiveSource;
  requested_by: string;
  requested_at: Iso8601;
  reason: string;
  status: 'PENDING_APPROVAL' | 'EXECUTED' | 'REJECTED' | 'EXPIRED' | 'FAILED';
  approved_by: string | null;
  executed_at: Iso8601 | null;
  expires_at: Iso8601;
}
/** Approver must differ from requester (hub enforces; 409 otherwise). */
export type SwitchApproval = DestructiveMutation;

// ------------------------------------------------------------------ hub & destination — GET /hub/health
export interface HubHealth {
  services: { name: string; status: Health; detail: string }[];
  queues: { name: string; depth: number; oldest_age_seconds: number }[];
  spool_used_bytes: number;
  spool_capacity_bytes: number;
  destination: {
    engine: string;
    account: string;
    warehouse: string;
    /** null = not observable from the hub (never a fake 0). */
    credits_used_today: number | null;
    credit_quota_month: number | null;
    credits_used_month: number | null;
  };
  /** Metadata only — never secret material. */
  credentials: { name: string; kind: 'key_pair' | 'client_cert' | 'server_cert' | 'oidc_secret'; rotated_at: Iso8601; expires_at: Iso8601 | null }[];
}

// ------------------------------------------------------------------ retention — GET/PUT /retention
export interface Retention {
  spool_cap_bytes: number;
  raw_archive_days: number; // ADR-007: 30..90
  /** null = not observable from the hub (backups are run outside it); never a fake empty list. */
  postgres_backups: { at: Iso8601; kind: 'FULL' | 'WAL'; size_bytes: number; status: 'OK' | 'FAILED' }[] | null;
  /** From pg_stat_archiver on a PostgreSQL metadata DB; null = unknown (SQLite, no privilege). */
  wal_archiving_ok: boolean | null;
}
export interface RetentionUpdate extends Mutation {
  spool_cap_bytes?: number;
  raw_archive_days?: number;
}

// ------------------------------------------------------------------ users & roles — GET /users, PUT /users/group-mappings
export interface GroupMapping {
  group_id: string; // Entra ID object id
  group_name: string;
  role: Role;
}
export interface AppUser {
  id: Id;
  email: string;
  name: string;
  roles: Role[];
  last_seen: Iso8601 | null;
}
export interface UsersAndRoles {
  group_mappings: GroupMapping[];
  users: AppUser[];
}
export interface GroupMappingsUpdate extends Mutation {
  group_mappings: GroupMapping[];
}

// ------------------------------------------------------------------ audit — GET /audit?from=&to=&actor=&action=, GET /audit/export?format=csv|json
export interface AuditEvent {
  id: Id;
  at: Iso8601;
  actor: string;
  role: Role | 'agent' | 'system';
  action: string; // e.g. table.config.update, agent.command.PAUSE, switch.approve
  target: string;
  detail: Record<string, unknown>;
  correlation_id: string | null;
}

// ------------------------------------------------------------------ test ladder — GET /ladder (tests/ladder/Lx/result.json)
export type LadderLevel = 'L0' | 'L1' | 'L2' | 'L3' | 'L4' | 'L5' | 'L6' | 'L7' | 'L8' | 'L9' | 'L10';
export interface LadderResult {
  schema_version: 1;
  level: LadderLevel;
  name: string;
  status: 'PASS' | 'FAIL' | 'SKIP' | 'NOT_RUN';
  environment: 'dev' | 'lab' | 'prod';
  started_at: Iso8601 | null;
  finished_at: Iso8601 | null;
  host: string;
  checks: { name: string; status: 'PASS' | 'FAIL' | 'SKIP'; detail: string }[];
  artifacts: string[];
  exit_criterion: string;
  // hub ladder index (test-strategy 4.2): stale evidence is never shown as a green level
  formal?: boolean;
  stale?: boolean;
  evidence?: boolean;
  age_hours?: number | null;
  file?: string;
}

// ------------------------------------------------------------------ migration importer (hub/alelt_hub/importer.py, hub/alelt_import/models.py)
export type Environment = 'TEST' | 'PROD';
export interface CredentialField { name: string; label: string; secret: boolean; required: boolean; multiline: boolean; help: string }
/** Display names come from the hub: the console never hard-codes a legacy platform. */
export interface PluginInfo {
  name: string; display_name: string; description: string; credential_fields: CredentialField[];
  read_only: boolean; accepts_files: boolean; file_hint: string; live: boolean;
}
export interface PlanWarning { code: string; severity: 'info' | 'warning' | 'blocker'; message: string; ref: string | null }
export interface Schedule { frequency_minutes: number | null; daily_time: string | null; timezone: string | null; paused: boolean; schedule_type: string | null }
export interface PlanColumn { enabled: boolean; hashed: boolean; is_primary_key: boolean }
export interface PlanTable { enabled: boolean; name_in_destination: string; sync_mode: string; columns: Record<string, PlanColumn> }
export interface PlanSchema { enabled: boolean; name_in_destination: string; tables: Record<string, PlanTable> }
/** config/tables.yaml document. */
export interface TablesDoc {
  connection_id: string; source_engine: string; destination_engine: string; output_profile: string;
  schema_change_handling: string; source_name: string; legacy_schema: string | null;
  snapshot_window?: { start: string; end: string; tz: string }; schemas: Record<string, PlanSchema>;
}
export interface PlannedConnection {
  legacy_id: string; legacy_name: string; connection_id: string; include: boolean; supported: boolean;
  source_engine: string | null; destination_engine: string | null; capture_mode: string | null;
  source: { engine: string | null; service: string; settings: Record<string, unknown>; secrets: Record<string, string> };
  destination: { engine: string | null; legacy_id: string | null; name: string | null; settings: Record<string, unknown>; secrets: Record<string, string> };
  tables: TablesDoc; latency_target_s: number; schedule: Schedule; paused: boolean; legacy_schema: string | null;
  enabled_tables: number; total_tables: number;
}
export interface PlannedTrigger { legacy_id: string; kind: 'webhook' | 'dbt_job'; name: string; include: boolean; target: string | null; events: string[]; after_connections: string[]; settings: Record<string, unknown> }
export interface PlannedRole { email: string; name: string; roles: Role[]; legacy_role: string | null; include: boolean }
export interface PlannedSecret { key: string; label: string; owner: string; field: string; where: 'hub' | 'agent'; used_by: string[] }
export interface MigrationPlan {
  plugin: string; platform: string; generated_at: Iso8601; connections: PlannedConnection[]; triggers: PlannedTrigger[];
  roles: PlannedRole[]; secrets: PlannedSecret[]; warnings: PlanWarning[]; summary: Record<string, number>;
}
export interface ColumnEdit { enabled?: boolean; hashed?: boolean }
export interface TableEdit { enabled?: boolean; columns?: Record<string, ColumnEdit> }
export interface ConnectionEdit { include?: boolean; connection_id?: string; latency_target_s?: number; output_profile?: string; tables?: Record<string, TableEdit> }
export interface PlanEdits { connections: Record<string, ConnectionEdit>; triggers: Record<string, boolean>; roles: Record<string, boolean> }
export type ImportStatus = 'DISCOVERED' | 'FAILED' | 'GOLDEN_ONLY' | 'PENDING_APPROVAL' | 'APPLIED';
export interface ImportSession {
  id: Id; plugin: string; platform: string; mode: 'live' | 'files'; status: ImportStatus;
  started_at: Iso8601; finished_at: Iso8601 | null; started_by: string; files: { name: string; kind: string }[];
  message: string | null; inventory: Record<string, unknown> | null; plan: MigrationPlan | null; warnings: PlanWarning[];
  diff: DiffEntry[]; needs_approval: boolean; golden: { tables: string[]; columns: number } | null;
  requested_by: string | null; approved_by: string | null; applied_revision: number | null; result: ApplyResult | null;
}
export interface DiscoverRequest extends Mutation { plugin: string; credentials: Record<string, string> }
export interface DiscoverFilesRequest extends Mutation { plugin?: string; files: { name: string; data_base64: string }[] }
export interface SecretInput { value?: string; ref?: string }
export interface ApplyRequest extends Mutation {
  environment: Environment; secrets: Record<string, SecretInput>; start_shadow?: boolean;
  source_overrides?: Record<string, Record<string, unknown>>; destination_overrides?: Record<string, Record<string, unknown>>;
  /** prod host -> test host, merged over the stored mapping (PUT /import/runs/{id}/host-map). */
  host_map?: Record<string, string>;
}
/**
 * GET /import/runs/{id}/host-map (X-117): every host / account found in the selected connections (source and
 * destination settings) must be mapped to its test counterpart before apply (409 HOST_MAP_REQUIRED); a target on
 * the production deny list is refused (409 PROD_TARGET). Hosts are lower case.
 */
export interface HostMapEntry {
  host: string; connections: string[]; sides: ('source' | 'destination')[];
  prod_denied: boolean; test_host: string | null; test_denied: boolean;
}
export interface HostMap { hosts: HostMapEntry[]; complete: boolean }
export interface HostMapUpdate { mappings: Record<string, string>; reason?: string }
export interface ApplyResult {
  revision: number;
  connections: { connection_id: string; status: string; config_file: string | null; missing_secrets: string[]; golden?: { covered: string[]; missing: string[] } }[];
  triggers: number; roles_granted: string[]; secrets_saved: string[];
}
export interface ApplyResponse { id: Id; status: ImportStatus; result: ApplyResult | null; requested_by: string | null; applied_revision: number | null }

// ------------------------------------------------------------------ pipeline wizard + production guard
export interface WizardField { name: string; label: string; secret?: boolean; required?: boolean; default?: string | number }
export interface WizardSource { engine: string; display_name: string; capture_mode: 'edge' | 'remote'; available: boolean; fields: WizardField[] }
export interface CheckItem { id: string; label: string; status: 'PASS' | 'FAIL' | 'UNKNOWN' | 'INFO'; detail: string; check_sql?: string | null; fix_sql?: string | null; tables?: string[] }
export interface TestStep { step: string; ok: boolean; detail: string; pending?: boolean; elapsed_ms?: number }
export interface SourceTestRequest { engine: string; connection_id?: string; environment: Environment; settings: Record<string, unknown>; password?: string }
export interface SourceTestResult { ok: boolean; steps: TestStep[]; checks: CheckItem[]; denied: boolean; catalog_tables?: number | null }
export interface DestTestRequest { engine: 'postgres' | 'snowflake'; environment: Environment; settings: Record<string, unknown>; bootstrap: boolean; reason?: string }
export interface DestTestResult { ok: boolean; steps: TestStep[]; denied: boolean; environment?: Environment; elapsed_ms?: number }
export interface DefaultTable {
  fqn: string; schema: string; name: string; has_pk: boolean | null; capture_enabled: boolean | null; columns: number;
  selected: boolean; reason: 'keyed' | 'keyless_surrogate' | 'capture_disabled' | 'noise_name' | 'not_in_golden' | 'imported';
  churn_per_day: number | null; high_churn: boolean; in_golden: boolean;
}
export interface WizardDefaults {
  connection_id: string; source_name: string; tables: DefaultTable[];
  latency: { latency_target_s: number; why: 'high_churn' | 'legacy_schedule' | 'default'; tables: string[] };
  output_profile: string; schemas: string[]; catalog_available: boolean; golden_tables: string[];
  legacy_schema: string | null; legacy_id: string | null; schedule: Schedule | null; checks: CheckItem[];
}
export interface PipelineCreate {
  reason?: string; connection_id: string; environment: Environment; source_engine: string; source: Record<string, unknown>;
  password?: SecretInput; destination_engine: 'postgres' | 'snowflake'; destination: Record<string, unknown>;
  tables?: string[]; latency_target_s?: number; output_profile?: string; session_id?: string; legacy_id?: string;
  agent_kind?: 'service' | 'devedge'; hub_url?: string;
}
export interface AgentPackage {
  token: string; token_hint: string; agent_id: string; expires_at: Iso8601; hub_url: string;
  aleltagent_json: Record<string, unknown>; devedge_json: Record<string, unknown>;
}
export interface PipelineCreated {
  connection_id: string; status: string; config_file: string | null; revision: number; tables: string[];
  latency_target_s: number; output_profile: string; agent: AgentPackage; kind: string;
  contract: { covered: string[]; missing: string[] } | null;
}
export type ProgressStepId = 'agent' | 'prerequisites' | 'initial_load' | 'cdc' | 'contract' | 'parity';
export interface ProgressStep { id: ProgressStepId; status: 'DONE' | 'RUNNING' | 'PENDING' | 'FAIL'; detail: string; checks?: CheckItem[]; covered?: string[]; missing?: string[]; last_batch_at?: Iso8601 | null }
export interface Progress {
  connection_id: string; environment: Environment; status: string; active_source: ActiveSource;
  steps: ProgressStep[]; tables: { fqn: string; snapshot_rows: number; snapshot_done: boolean; parity: string | null }[];
}
export interface Pipeline {
  connection_id: string; environment: Environment; origin: string; status: string; active_source: ActiveSource;
  source_engine: string; destination_engine: string; latency_target_s: number; revision: number | null;
  legacy_ref: string | null; tables: number; source_host: string | null; source_database: string | null;
  created_at: Iso8601; created_by: string;
}
export interface DenyList { entries: { entry: string; source: 'settings' | 'console' }[]; effective: string[] }
export interface DenyListUpdate extends Mutation { entries: string[] }

// ------------------------------------------------------------------ topology & HA (X-50)
export type TopologyMode = 'standalone' | 'ha';
export type TopologyLayerId = 'api' | 'workers' | 'metadata' | 'spool' | 'agents' | 'destination';
export interface TopologyLayer { layer: TopologyLayerId; ha: boolean; kind: string; detail: string; advice: string | null }
export interface HubNode { node_id: string; role: 'api' | 'worker'; host: string; started_at: Iso8601; last_seen: Iso8601; alive: boolean }
export interface CaptureAgentRole {
  agent_id: string; hostname: string | null; state: string | null; last_heartbeat: Iso8601 | null;
  role: 'active' | 'standby' | 'standalone';
}
export interface CaptureConnection {
  connection_id: string; agent_mode: 'standalone' | 'active_passive'; holder: string | null; epoch: number;
  lease_id: string | null; expires_at: Iso8601 | null; agents: CaptureAgentRole[];
}
export type TopologyStepStatus = 'todo' | 'running' | 'done' | 'skipped' | 'failed';
export interface TopologyOperation {
  id: Id; to: TopologyMode; status: 'running' | 'waiting' | 'done' | 'failed' | 'aborted'; started_at: Iso8601; finished_at: Iso8601 | null;
  steps: { id: string; status: TopologyStepStatus; at: Iso8601 | null; detail: string }[];
}
export interface InstallOption { mode: TopologyMode; title: string; when: string; command: string; doc: string }
export interface TopologyStatus {
  mode: TopologyMode | 'mixed'; layers: TopologyLayer[]; nodes: HubNode[]; connections: CaptureConnection[];
  operation: TopologyOperation | null; install_options: InstallOption[];
}
export interface TopologyPlanRequest { to: TopologyMode; db_url?: string; spool_url?: string }
export interface TopologyPlan {
  from: string; to: string; steps: { id: string; title: string; detail: string; status: string }[];
  blockers: string[]; warnings: string[]; command: string;
}
export interface LeaseReleased { released: boolean; holder: string | null; epoch: number }

// ------------------------------------------------------------------ endpoint map (single source for the client)
export interface Endpoints extends DiagnoseEndpoints {
  'GET /me': { res: Me };
  'GET /overview': { res: Overview };
  'GET /tables': { res: Page<TableSummary> };
  'GET /tables/{id}': { res: TableDetail };
  'PUT /tables/{id}/config': { req: TableConfigUpdate; res: TableDetail };
  'POST /tables/{id}/resync': { req: ResyncRequest; res: { job_id: Id } };
  'POST /tables/{id}/reprocess': { req: ReprocessRequest; res: { job_id: Id } };
  'GET /batches': { res: Page<Batch> };
  'GET /quarantine': { res: Page<QuarantineItem> };
  'GET /agents': { res: Agent[] };
  'POST /agents/{id}/commands': { req: AgentCommandRequest; res: CommandAccepted };
  'GET /revisions': { res: Revision[] };
  'GET /revisions/{id}/diff': { res: DiffEntry[] };
  'POST /revisions/{id}/rollback': { req: DestructiveMutation; res: Revision };
  'GET /revisions/{id}/export': { res: string }; // text/yaml
  'POST /import/runs': { req: Mutation; res: ImportRun };
  'GET /import/runs': { res: ImportRun[] };
  'POST /import/runs/{id}/accept': { req: Mutation; res: Revision };
  'GET /schema-changes': { res: SchemaChange[] };
  'POST /schema-changes/{id}/decision': { req: SchemaDecision; res: SchemaChange };
  'GET /recon': { res: ReconRun[] };
  'POST /recon/runs': { req: ReconRunRequest; res: ReconRun };
  'GET /certification': { res: TableCertification[] };
  'POST /certification/{id}/signoff': { req: SignoffRequest; res: TableCertification };
  'GET /certification/waves': { res: Wave[] };
  'POST /certification/waves': { req: WaveCreate; res: Wave };
  'POST /switch': { req: SwitchRequestCreate; res: SwitchRequest };
  'GET /switch/requests': { res: SwitchRequest[] };
  'POST /switch/requests/{id}/approve': { req: SwitchApproval; res: SwitchRequest };
  'POST /switch/requests/{id}/reject': { req: Mutation; res: SwitchRequest };
  'GET /hub/health': { res: HubHealth };
  'GET /topology': { res: TopologyStatus };
  'POST /topology/plan': { req: TopologyPlanRequest; res: TopologyPlan };
  'POST /topology/connections/{id}/release-lease': { req: Mutation; res: LeaseReleased };
  'GET /retention': { res: Retention };
  'PUT /retention': { req: RetentionUpdate; res: Retention };
  'GET /users': { res: UsersAndRoles };
  'PUT /users/group-mappings': { req: GroupMappingsUpdate; res: UsersAndRoles };
  'GET /audit': { res: Page<AuditEvent> };
  'GET /audit/export': { res: string }; // ?format=csv|json -> file body
  'GET /ladder': { res: LadderResult[] };
  'GET /import/plugins': { res: PluginInfo[] };
  'POST /import/discover': { req: DiscoverRequest; res: ImportSession };
  'POST /import/discover/files': { req: DiscoverFilesRequest; res: ImportSession };
  'GET /import/runs/{id}': { res: ImportSession };
  'PUT /import/runs/{id}/plan': { req: { edits: PlanEdits; reason?: string }; res: ImportSession };
  'POST /import/runs/{id}/apply': { req: ApplyRequest; res: ApplyResponse };
  'GET /import/runs/{id}/host-map': { res: HostMap };
  'PUT /import/runs/{id}/host-map': { req: HostMapUpdate; res: HostMap };
  'GET /wizard/sources': { res: WizardSource[] };
  'POST /wizard/source/test': { req: SourceTestRequest; res: SourceTestResult };
  'POST /wizard/destination/test': { req: DestTestRequest; res: DestTestResult };
  'GET /wizard/defaults': { res: WizardDefaults }; // ?connection_id&session_id&legacy_id&engine
  'POST /wizard/pipelines': { req: PipelineCreate; res: PipelineCreated };
  'POST /wizard/pipelines/{id}/agent': { req: { reason: string; hub_url?: string }; res: AgentPackage };
  'GET /wizard/pipelines/{id}/progress': { res: Progress };
  'GET /pipelines': { res: Pipeline[] };
  'GET /pipelines/{id}/tables.yaml': { res: string }; // text/yaml
  'GET /guard/denylist': { res: DenyList };
  'PUT /guard/denylist': { req: DenyListUpdate; res: DenyList };
  'POST /guard/check': { req: { targets: string[] }; res: { target: string; denied_by: string | null }[] };
}
export type EndpointKey = keyof Endpoints;

// ------------------------------------------------------------------ confirmation text (shared rule, hub computes the same)
export type ConfirmAction = 'resync' | 'reprocess' | 'pause' | 'shutdown' | 'switch' | 'rollback' | 'revision_rollback';
/** Text the user must type to confirm a destructive action. Hub validates with the same rule. */
export function expectedConfirmText(action: ConfirmAction, target: string): string {
  return `${action.toUpperCase()} ${target}`;
}
