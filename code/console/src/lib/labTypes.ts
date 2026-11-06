// SPDX-License-Identifier: Apache-2.0
/** Types of the local test lab catalog (`lab/catalog.yaml` → `labCatalog.generated.ts` via `python lab/gen_catalog.py`). */
export type LabKind = 'source' | 'target';
export type LabAvailability = 'image' | 'no_image';
export type LabMaturity = 'stable' | 'beta' | 'planned';

export interface LabGuidance {
  what: string;
  signup_url: string;
  docs_url: string;
  objects: string[];
  form_fields: Record<string, string | number>;
  test_engine: string;
}

export interface LabProfile {
  id: string;
  kind: LabKind;
  engine: string;
  label: string;
  availability: LabAvailability;
  status: LabMaturity;
  images?: string[];
  arch?: string[];
  privileged?: boolean;
  /** Not part of all/sources; the container starts only with LAB_DB2_CONTAINER=1 (native install preferred). */
  optional?: boolean;
  native?: string;
  /** Compatible local tags used by `lab up` when the pinned image is absent (`var` = compose LAB_IMG_* override). */
  local_fallbacks?: { var: string; image: string; tags: string[] }[];
  services?: string[];
  ports?: Record<string, number>;
  connection?: Record<string, string | number>;
  dsn?: string;
  demo?: { source_engine: string; schemas: string };
  notes?: string;
  guidance?: LabGuidance;
}

export interface LabCatalog {
  version: number;
  project: string;
  network: string;
  helper: { bind: string; port: number };
  note?: string;
  profiles: LabProfile[];
}
