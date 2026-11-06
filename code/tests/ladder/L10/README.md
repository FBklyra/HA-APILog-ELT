# L10 - Production

**Scope.** Legacy pipeline decommission at renewal: pause -> 30 days -> delete.

**Exit criterion (test ladder).** All tables certified + 30 days stable; legacy pipeline decommissioned

| File | Purpose |
|---|---|
| `l10_checklist.md` | Checklist with evidence column and two sign-offs. |
| `l10_check.py` | Green only when every item is ticked with evidence and both sign-offs are filled. |

## Run

```
python l10_check.py
```

Writes `L10/result.json` (schema: `tests/ladder/_common/ladder_result.py`). Exit code 0 = PASS, 1 = FAIL, 2 = SKIP (prerequisite missing or gate closed).

**Gate.** Refuses to run production steps unless `L9/result.json` is PASS.
