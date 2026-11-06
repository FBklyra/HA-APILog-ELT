/**
 * Incidents & self-diagnosis API (hub/alelt_hub/diagnose_api.py). Merged into `Endpoints` (contract.ts).
 * Values are always redacted by the hub; raw source values only via POST /incidents/{id}/source-values (incident.reveal).
 */
import type { Id, Iso8601, Mutation, Severity } from './contract';

export const CLASSIFICATIONS = ['PLATFORM_DEFECT', 'SOURCE_DATA', 'CONFIGURATION', 'INFRASTRUCTURE', 'DESTINATION', 'UNKNOWN'] as const;
export type Classification = (typeof CLASSIFICATIONS)[number];
export type IncidentStatus = 'OPEN' | 'DIAGNOSED' | 'RESOLVED';
export type ActionKind = 'RETRY_JOB' | 'REDECODE_RANGE' | 'RESYNC_TABLE' | 'SNAPSHOT_CATALOG' | 'RESUME_AGENT' | 'SOURCE_DATA_POLICY';
export const HANDLING_OPTIONS = ['QUARANTINE_SKIP', 'COERCE', 'ACCEPT_AS_IS'] as const;
export type HandlingOption = (typeof HANDLING_OPTIONS)[number];

export interface SourceDataIssue {
  issue: 'INVALID_ENCODING' | 'INVALID_DATE' | 'FUTURE_TIMESTAMP' | 'NULL_IN_NOT_NULL' | 'OUT_OF_RANGE' | 'DUPLICATE_KEYLESS' | 'ORPHAN_KEY';
  table: string | null;
  column: string | null;
  key_columns: string[];
  positions: string[];
  count: number;
  explanation: string;
  handling_options: HandlingOption[];
  recommended: HandlingOption;
  /** Read-only SELECT for the DBA — shown, never executed by ALELT (R1). */
  dba_sql: string;
}

export interface Finding {
  rule_id: string;
  title: string;
  root_cause: string;
  explanation: string;
  confidence: number;
  category: Classification;
  evidence_refs: string[];
  fix_steps: string[];
}

export interface IncidentAction {
  id: Id;
  kind: ActionKind;
  params: Record<string, unknown>;
  description: string;
  /** safe + idempotent (may be auto-run when enabled by ALELT_DIAG_AUTO_ACTIONS) */
  safe: boolean;
  rule_id: string | null;
  status: 'PROPOSED' | 'EXECUTED' | 'REJECTED' | 'FAILED';
  created_at: Iso8601;
  decided_by: string | null;
  decided_at: Iso8601 | null;
  reason: string | null;
  result: Record<string, unknown> | null;
}

export interface PatchValidation {
  paths: string[];
  paths_ok: boolean;
  disallowed: string[];
  has_test: boolean;
  apply_check: 'ok' | 'failed' | 'unverified';
  message: string;
}

export interface CodeFix {
  id: Id;
  incident_id: Id;
  status: 'PROPOSED' | 'REVIEWED' | 'REJECTED' | 'EXPORTED';
  summary: string;
  explanation: string | null;
  files: string[];
  patch: string;
  test_path: string | null;
  validation: PatchValidation;
  regression_case: { path: string; content: Record<string, unknown> } | null;
  model: string | null;
  confidence: number | null;
  created_at: Iso8601;
  created_by: string;
  reviewed_by: string | null;
  reviewed_at: Iso8601 | null;
}

export interface AiRun {
  id: Id;
  at: Iso8601;
  actor: string;
  provider: string | null;
  model: string | null;
  prompt_hash: string | null;
  answer: { root_cause: string; confidence: number; reasoning_summary: string; fix_steps: string[]; risk: string; needs_human: boolean } | null;
  tokens: number | null;
  error: string | null;
}

export interface IncidentSummary {
  id: Id;
  kind: string;
  severity: Severity;
  status: IncidentStatus;
  title: string;
  connection_id: string | null;
  table: string | null;
  batch_id: string | null;
  job_id: number | null;
  agent_id: string | null;
  position: string | null;
  error: string | null;
  count: number;
  first_seen: Iso8601;
  last_seen: Iso8601;
  rule_id: string | null;
  root_cause: string | null;
  explanation: string | null;
  confidence: number | null;
  fix_steps: string[];
  classification: Classification;
  source_data: SourceDataIssue | null;
  resolved_by: string | null;
  resolved_at: Iso8601 | null;
  confirmed_cause: string | null;
}

export interface IncidentDetail extends IncidentSummary {
  timeline: { at: Iso8601; kind: string; actor: string; detail: Record<string, unknown> }[];
  actions: IncidentAction[];
  findings: Finding[];
  ai: AiRun[];
  code_fixes: CodeFix[];
  sensitive_samples: number;
  llm_error?: string | null;
}

export interface IncidentCounts {
  open: number;
  critical: number;
  warning: number;
  info: number;
  by_kind: Record<string, number>;
}

export type ExplainKind = 'incident' | 'job' | 'batch' | 'quarantine' | 'agent_event' | 'alert';
export interface ExplainRequest {
  kind: ExplainKind;
  ref_id: string;
  labels?: Record<string, string>;
  use_llm?: boolean;
}
export interface ExplainResult {
  kind: ExplainKind;
  ref_id: string;
  source: 'rules' | 'llm';
  rule_id: string | null;
  root_cause: string;
  explanation: string;
  fix_steps: string[];
  confidence: number;
  needs_human: boolean;
  llm_configured: boolean;
  llm_error: string | null;
  classification: Classification;
}

export type LlmProvider = 'anthropic' | 'openai_compatible' | 'ollama';
export interface LlmSettings {
  enabled: boolean;
  provider: LlmProvider;
  url: string;
  model: string;
  timeout_s: number;
  max_tokens_day: number;
  max_output_tokens: number;
  redaction: 'strict' | 'standard';
  /** The key itself is write-only and never returned. */
  key_set: boolean;
  configured: boolean;
  tokens_used_today: number;
  source: Record<string, 'env' | 'console' | 'default'>;
}
/** PUT /admin/llm — `api_key`: omit = keep, "" = clear. */
export interface LlmSettingsUpdate extends Mutation {
  enabled?: boolean;
  provider?: LlmProvider;
  url?: string;
  model?: string;
  api_key?: string;
  timeout_s?: number;
  max_tokens_day?: number;
  max_output_tokens?: number;
  redaction?: 'strict' | 'standard';
}
export interface LlmTestResult {
  ok: boolean;
  latency_ms: number;
  model: string;
  error: string | null;
}
export interface Meta {
  llm: { configured: boolean; enabled: boolean; provider: string; model: string };
  diagnose: { rules: number; auto_actions: string[] };
}

export interface DiagnoseEndpoints {
  'GET /meta': { res: Meta };
  'GET /incidents': { res: IncidentSummary[] };
  'GET /incidents/summary': { res: IncidentCounts };
  'GET /incidents/{id}': { res: IncidentDetail };
  'GET /incidents/{id}/evidence': { res: Record<string, unknown> };
  'POST /incidents/{id}/diagnose': { req: { use_llm?: boolean; reason?: string; question?: string }; res: IncidentDetail };
  'POST /incident-actions/{id}/decision': { req: Mutation & { decision: 'APPROVE' | 'REJECT' }; res: { id: Id; status: string } };
  'POST /incidents/{id}/resolve': { req: Mutation & { confirmed_root_cause: string | null; save_known_issue?: boolean }; res: IncidentDetail };
  'GET /incidents/{id}/regression-note': { res: string };
  'POST /incidents/{id}/code-fix': { req: Mutation; res: CodeFix };
  'POST /code-fixes/{id}/review': { req: Mutation & { decision: 'APPROVE' | 'REJECT' }; res: CodeFix };
  'GET /code-fixes/{id}/patch': { res: string };
  'POST /incidents/{id}/source-values': { req: Mutation; res: { column: string; position: string | null; key: Record<string, string>; value: string }[] };
  'POST /diagnose/explain': { req: ExplainRequest; res: ExplainResult };
  'POST /diagnose/scan': { res: { triggers: number } };
  'GET /admin/llm': { res: LlmSettings };
  'PUT /admin/llm': { req: LlmSettingsUpdate; res: LlmSettings };
  'POST /admin/llm/test': { req: Partial<LlmSettingsUpdate>; res: LlmTestResult };
}

/** Coloured unified-diff lines for display (pure, unit-tested). */
export function diffLines(patch: string): { kind: 'add' | 'del' | 'hunk' | 'file' | 'ctx'; text: string }[] {
  return patch.split('\n').map((text) => ({
    kind: text.startsWith('+++') || text.startsWith('---') || text.startsWith('diff ') ? 'file'
      : text.startsWith('@@') ? 'hunk' : text.startsWith('+') ? 'add' : text.startsWith('-') ? 'del' : 'ctx',
    text,
  }));
}
