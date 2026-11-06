import type { DiagnoseEndpoints } from '../api/diagnose';
import type { Permission } from './rbac';

/** Server-side permission per incidents/diagnosis endpoint (hub/alelt_hub/diagnose_api.py). */
export const DIAG_ENDPOINT_PERMISSION: Record<keyof DiagnoseEndpoints, Permission | null> = {
  'GET /meta': null,
  'GET /incidents': 'view.incidents',
  'GET /incidents/summary': 'view.dashboard',
  'GET /incidents/{id}': 'view.incidents',
  'GET /incidents/{id}/evidence': 'view.incidents',
  'POST /incidents/{id}/diagnose': 'view.incidents', // use_llm additionally needs incident.act
  'POST /incident-actions/{id}/decision': 'incident.act',
  'POST /incidents/{id}/resolve': 'incident.act',
  'GET /incidents/{id}/regression-note': 'view.incidents',
  'POST /incidents/{id}/code-fix': 'incident.act',
  'POST /code-fixes/{id}/review': 'incident.act',
  'GET /code-fixes/{id}/patch': 'view.incidents',
  'POST /incidents/{id}/source-values': 'incident.reveal',
  'POST /diagnose/explain': 'view.incidents', // LLM only with incident.act, otherwise rules
  'POST /diagnose/scan': 'incident.act',
  'GET /admin/llm': 'llm.configure',
  'PUT /admin/llm': 'llm.configure',
  'POST /admin/llm/test': 'llm.configure',
};
