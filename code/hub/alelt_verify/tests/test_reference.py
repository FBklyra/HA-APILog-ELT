"""Reference-landing parity (X-124): two schemas in one destination, generic mapping."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from typing import Any

from alelt_verify.edge import factory
from alelt_verify.models import CheckName, Status
from alelt_verify.reference import ReferenceMapping, mapping_from_profiles, run_reference_suite, schema_rows

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)
OLD = NOW - timedelta(hours=5)


def ref_rows() -> list[dict[str, Any]]:
    return [{"id": i, "v": f"v{i}", "_LEGACY_SYNCED": OLD, "_LEGACY_DELETED": False, "_LEGACY_ID": f"h{i}"}
            for i in range(1, 51)]


def cand_rows() -> list[dict[str, Any]]:
    return [{"ID": i, "V": f"v{i}", "_OUR_SYNCED": OLD, "_OUR_DELETED": False} for i in range(1, 51)]


M = mapping_from_profiles({"synced_column": "_LEGACY_SYNCED", "deleted_column": "_LEGACY_DELETED",
                           "id_column": "_LEGACY_ID"},
                          {"synced_column": "_OUR_SYNCED", "deleted_column": "_OUR_DELETED", "id_column": "_OUR_ID"})


def run(r: list[dict[str, Any]], c: list[dict[str, Any]], m: ReferenceMapping = M) -> Any:
    return run_reference_suite(factory("S.T"), r, c, ["ID"], m, settled_before=NOW - timedelta(hours=1))


class Reference(unittest.TestCase):
    def test_match_ignores_system_columns(self) -> None:
        out = run(ref_rows(), cand_rows())
        self.assertEqual(out.outcome, "MATCH", [x for x in out.results if x.status is not Status.PASS])
        self.assertEqual(out.summary["columns"], 2)
        self.assertIn("_LEGACY_SYNCED", out.summary["excluded"])

    def test_deleted_rows_excluded_on_each_side(self) -> None:
        r, c = ref_rows(), cand_rows()
        r[3]["_LEGACY_DELETED"] = True
        c[3]["_OUR_DELETED"] = True
        self.assertEqual(run(r, c).outcome, "MATCH")
        c[4]["_OUR_DELETED"] = True                                 # deleted only on our side
        out = run(r, c)
        self.assertEqual(out.outcome, "MISMATCH")
        self.assertTrue(any(x.check is CheckName.KEY_SET and x.key == '["5"]' for x in out.results))

    def test_unsettled_rows_excluded_both_sides(self) -> None:
        r, c = ref_rows(), cand_rows()
        c[7]["V"] = "newer"
        c[7]["_OUR_SYNCED"] = NOW                                   # changed recently: legacy has not synced yet
        out = run(r, c)
        self.assertEqual(out.outcome, "MATCH")
        self.assertEqual(out.summary["unsettled_keys"], 1)

    def test_value_mismatch_located(self) -> None:
        r, c = ref_rows(), cand_rows()
        c[9]["V"] = "wrong"
        out = run(r, c)
        self.assertEqual(out.outcome, "MISMATCH")
        self.assertTrue(any(x.check is CheckName.COLUMN_AGGREGATES and x.column == "V" for x in out.results))

    def test_rename(self) -> None:
        r = [{**x, "VAL_OLD": x.pop("v")} for x in ref_rows()]
        m = ReferenceMapping(**{**M.__dict__, "rename": {"VAL_OLD": "V"}})
        self.assertEqual(run(r, cand_rows(), m).outcome, "MATCH")

    def test_schema_rows_needs_spi(self) -> None:
        class D:
            def fetch_rows(self, name: str) -> list[dict[str, Any]]:
                return []

        class E:
            def fetch_rows(self, name: str, schema: str | None = None) -> list[dict[str, Any]]:
                return [{"schema": schema, "name": name}]
        with self.assertRaises(RuntimeError):
            schema_rows(D())("LEGACY", "T")
        self.assertEqual(schema_rows(E())("LEGACY", "T"), [{"schema": "LEGACY", "name": "T"}])


if __name__ == "__main__":
    unittest.main()
