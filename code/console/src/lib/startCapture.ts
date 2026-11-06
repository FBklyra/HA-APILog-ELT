import type { Position, StartCaptureArgs } from '../api/contract';

export type StartMode = 'resume' | 'from_pos';

/** START_CAPTURE args for the chosen mode, or null when the explicit position is empty (the hub refuses it). */
export function startCaptureArgs(mode: StartMode, fromPos: string): StartCaptureArgs | null {
  if (mode === 'resume') return { resume: true };
  const p = fromPos.trim();
  return p ? { from_pos: p } : null;
}

/**
 * Format hint for the position input. Positions are opaque (never parsed by the console): the hint names the engine
 * format when known and shows the agent's current durable position as an example of the exact text to type.
 */
export function positionHintKey(engine: string): 'agent.start.hint.db2' | 'agent.start.hint.generic' {
  return engine.toLowerCase() === 'db2' ? 'agent.start.hint.db2' : 'agent.start.hint.generic';
}

export function positionExample(durable: Position | null): string {
  return durable?.text ?? '—';
}
