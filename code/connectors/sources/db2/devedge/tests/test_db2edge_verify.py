"""Python Db2 edge VERIFY_TABLE (alelt_db2edge.verify): same statements as the C plugin (strings below are the ones
asserted by connectors/sources/db2/edge/tests/test_db2.c), read-only shape, result contract. No database needed."""
from __future__ import annotations

import sys
import unittest
import uuid
from pathlib import Path
from typing import Any, List, Optional

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parents[4] / "hub"))

from alelt_db2edge import verify as v  # noqa: E402

FORBIDDEN = ("INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER", "GRANT", "CALL", "TRUNCATE", "LOCK",
             " SET ", " INTO ", "FOR UPDATE", "WITH UR", ";")


class Builders(unittest.TestCase):
    def test_same_text_as_c_plugin(self) -> None:
        sql, pt = v.snapshot_sql("S", "T", ["ID", "AMT", "NAME", "NOTE"], ["ID"], True, True, 50)
        self.assertEqual(sql, 'SELECT "ID","AMT","NAME","NOTE" FROM "S"."T" WHERE (("ID" > ?)) AND '
                              '(("ID" < ?) OR ("ID" = ?)) ORDER BY "ID" FETCH FIRST 50 ROWS ONLY WITH CS')
        self.assertEqual(pt, [0, 100, 100])
        cols = [("ID", "int"), ("AMT", "dec"), ("NAME", "str"), ("NOTE", "cnt")]
        sql, pt = v.agg_sql("S", "T", ["ID"], cols, False, True)
        self.assertEqual(sql, 'SELECT COUNT(*),COUNT("ID"),MIN("ID"),MAX("ID"),SUM(CAST("ID" AS DECIMAL(31,0))),'
                              'COUNT("AMT"),MIN("AMT"),MAX("AMT"),SUM("AMT"),'
                              'COUNT("NAME"),SUM(CAST(CHARACTER_LENGTH(RTRIM("NAME"),CODEUNITS32) AS DECIMAL(31,0))),'
                              'SUM(CASE WHEN "NOTE" IS NULL THEN 0 ELSE 1 END) FROM "S"."T" WHERE '
                              '(("ID" < ?) OR ("ID" = ?)) WITH CS')
        self.assertEqual(v.agg_sql("S", "T", ["ID"], [], False, False)[0], 'SELECT COUNT(*) FROM "S"."T" WITH CS')
        sql, pt = v.keys_sql("S", "T", ["K1", "K2"], True, False, 10001)
        self.assertEqual(sql, 'SELECT "K1","K2" FROM "S"."T" WHERE (("K1" > ?) OR ("K1" = ? AND "K2" > ?)) '
                              'ORDER BY "K1","K2" FETCH FIRST 10001 ROWS ONLY WITH CS')
        self.assertEqual(pt, [0, 0, 1])
        self.assertEqual(v.rows_sql("S", "T", ["K1", "K2"], ["K1", "K2", "V"])[0],
                         'SELECT "K1","K2","V" FROM "S"."T" WHERE "K1" = ? AND "K2" = ? WITH CS')

    def test_read_only_shape(self) -> None:
        for sql in (v.agg_sql("S", "T", ["A"], [("A", "int"), ("B", "str")], True, True)[0],
                    v.keys_sql("S", "T", ["A"], True, True, 5)[0], v.rows_sql("S", "T", ["A"], ["A", "B"])[0],
                    v.snapshot_sql("S", "T", ["A"], ["A"], False, False, 5)[0]):
            self.assertTrue(sql.startswith("SELECT ") and sql.endswith(" WITH CS"))
            for f in FORBIDDEN:
                self.assertNotIn(f, sql.replace('"', " "), sql)
        with self.assertRaises(ValueError):
            v.q('X"; DROP TABLE T')
        self.assertEqual(v.q('A"B'), '"A""B"')
        self.assertEqual(v.Q_META.endswith(" WITH UR"), True)       # catalog only

    def test_agg_class(self) -> None:
        self.assertEqual([v.agg_class(t, cp) for t, cp in (("INTEGER", 0), ("DECIMAL", 0), ("DOUBLE", 0),
                                                            ("TIMESTAMP", 0), ("VARCHAR", 1208), ("VARCHAR", 0),
                                                            ("CLOB", 1208))],
                         ["int", "dec", "flt", "tmp", "str", "cnt", "cnt"])


class FakeDb:
    """Answers the plugin's statements for table S.T(ID INTEGER PK, AMT DECIMAL, NAME VARCHAR)."""

    def __init__(self) -> None:
        self.sql: List[str] = []
        self.data = {1: ("1", "1.50", "a"), 2: ("2", "2.00", "bb")}

    def commit(self) -> None:
        pass

    def query_params(self, sql: str, params: List[str], timeout_s: int = 0) -> List[List[Optional[str]]]:
        self.sql.append(sql)
        if sql == v.Q_TABLE:
            return [["1" if params == ["S", "T"] else "0"]]
        if sql == v.Q_META:
            return [["ID", "1", "INTEGER", "0"], ["AMT", None, "DECIMAL", "0"], ["NAME", None, "VARCHAR", "1208"]]
        if sql.startswith("SELECT COUNT(*)"):
            return [["2", "2", "1", "2", "3", "2", "1.50", "2.00", "3.50", "2", "3"]]
        if sql.startswith('SELECT "ID" FROM'):
            return [["1"], ["2"]]
        if sql.startswith('SELECT "ID","AMT","NAME" FROM'):
            if not params[0].isdigit():
                raise RuntimeError("CLI execute: rc=-1 22005 -99999 CLI0112E Error in assignment.")
            r = self.data.get(int(params[0]))
            return [list(r)] if r else []
        raise AssertionError(sql)


class Run(unittest.TestCase):
    def args(self, **kw: Any) -> dict[str, Any]:
        return {"table": {"schema": "S", "name": "T"}, "verify_id": str(uuid.uuid4()), **kw}

    def test_result_contract(self) -> None:
        from alelt_common.models import VerifyTableResult

        db = FakeDb()
        r = v.verify_table(db, self.args(checks=["row_count", "aggregates", "keys"]), lambda: "5:0000000000000001")
        VerifyTableResult.model_validate(r)
        self.assertEqual(r["isolation"], "CS")
        self.assertEqual(r["ranges"][0]["rows"], 2)
        self.assertEqual(r["ranges"][0]["agg"]["AMT"], {"count": 2, "min": "1.50", "max": "2.00", "sum": "3.50"})
        self.assertEqual(r["ranges"][0]["agg"]["NAME"], {"count": 2, "len": "3"})
        self.assertEqual(r["ranges"][0]["keys"], [["1"], ["2"]])
        self.assertTrue(all(s.endswith(" WITH CS") or s.endswith(" WITH UR") for s in db.sql))
        self.assertTrue(all(s.endswith(" WITH CS") for s in db.sql if '"S"."T"' in s))
        r = v.verify_table(FakeDb(), self.args(checks=["rows"], key_values=[[1], [9], ["not-a-number"]]))
        self.assertEqual(r["rows"], [{"ID": "1", "AMT": "1.50", "NAME": "a"}])
        self.assertEqual(r["missing"], [["9"], ["not-a-number"]])        # cannot convert = cannot exist

    def test_unique_index_fallback_key(self) -> None:
        class NoPk(FakeDb):
            def query_params(self, sql: str, params: List[str], timeout_s: int = 0) -> Any:
                if sql == v.Q_META:
                    return [["ID", None, "INTEGER", "0"], ["AMT", None, "DECIMAL", "0"], ["NAME", None, "VARCHAR", "1208"]]
                if sql == v.Q_UNIQUE_TAB:          # first unique index by name wins (= the catalog rule, SRC-db2-07)
                    return [["IX_A", "ID", "1"], ["IX_B", "NAME", "1"]]
                return super().query_params(sql, params, timeout_s)
        self.assertEqual(v.table_meta(NoPk(), "S", "T")[1], ["ID"])

    def test_refusals(self) -> None:
        from alelt_common.verify_edge import VerifyArgsError

        with self.assertRaises(VerifyArgsError):
            v.verify_table(FakeDb(), self.args(checks=["aggregates"], sql="DELETE FROM T"))
        with self.assertRaises(VerifyArgsError):
            v.verify_table(FakeDb(), self.args(checks=["aggregates"], columns=["NOPE"]))
        with self.assertRaises(LookupError):
            v.verify_table(FakeDb(), {**self.args(checks=["row_count"]), "table": {"schema": "S", "name": "X"}})
        with self.assertRaises(VerifyArgsError):
            v.verify_table(FakeDb(), self.args(checks=["keys"], ranges=[{"after": [1, 2]}]))


if __name__ == "__main__":
    unittest.main()


class Snapshot(unittest.TestCase):
    """SNAPSHOT_TABLE on the Python edges: keyset paging on the key, WITH CS, text values, allow-list rule."""

    class PagedDb(FakeDb):
        def __init__(self, n: int) -> None:
            super().__init__()
            self.data = {i: (str(i), f"{i}.50", f"n{i}") for i in range(1, n + 1)}

        def query_params(self, sql: str, params: List[str], timeout_s: int = 0) -> List[List[Optional[str]]]:
            if sql.startswith('SELECT "ID","AMT","NAME" FROM "S"."T"') and "ORDER BY" in sql:
                self.sql.append(sql)
                lim = int(sql.split("FETCH FIRST ")[1].split(" ")[0])
                after = int(params[0]) if params else 0
                return [list(self.data[k]) for k in sorted(self.data) if k > after][:lim]
            return super().query_params(sql, params, timeout_s)

    def test_pages_whole_table_by_key(self) -> None:
        db = self.PagedDb(7)
        chunks = list(v.snapshot_chunks(db, "S", "T", 3))
        self.assertEqual([len(c[0]) for c in chunks], [3, 3, 1])
        self.assertEqual([c[1] for c in chunks], [[], ["3"], ["6"]])          # key_from (exclusive) of each chunk
        self.assertEqual([c[2] for c in chunks], [False, False, True])
        self.assertEqual(chunks[0][0][0], {"ID": "1", "AMT": "1.50", "NAME": "n1"})
        reads = [s for s in db.sql if 'FROM "S"."T"' in s]
        self.assertTrue(all(s.endswith(" WITH CS") for s in reads))
        for s in db.sql:
            for f in FORBIDDEN:
                self.assertNotIn(f, s.replace('"', " ").replace(" WITH UR", ""), s)
        exact = list(v.snapshot_chunks(self.PagedDb(6), "S", "T", 3))       # exact multiple: empty last chunk
        self.assertEqual([len(c[0]) for c in exact][-1], 0)
        self.assertTrue(exact[-1][2])

    def test_allow_list_shared_vectors(self) -> None:
        """Same vectors as aleltd and the C agent (edge-agent/shared/allowlist_vectors.json)."""
        import json

        vec = json.loads((HERE.parents[4] / "edge-agent" / "shared" / "allowlist_vectors.json").read_text())
        for entry, schema, table, want in vec["cases"]:
            self.assertEqual(v.table_allowed([entry], schema, table), want, (entry, schema, table))
        for bad in vec["invalid_entries"]:
            self.assertFalse(v.valid_allow_entry(bad), bad)
        self.assertTrue(v.valid_allow_entry("S.T") and v.valid_allow_entry("S.*"))
        self.assertFalse(v.table_allowed([], "S", "T"))
        self.assertTrue(v.table_allowed(["S.T", "X.*"], "X", "ANY"))

    def test_resume_after_key_and_last_key(self) -> None:
        full = list(v.snapshot_chunks(self.PagedDb(7), "S", "T", 3))
        self.assertEqual([c[3] for c in full], [["3"], ["6"], ["7"]])          # key of each chunk's last row
        rest = list(v.snapshot_chunks(self.PagedDb(7), "S", "T", 3, key_from=["3"]))
        self.assertEqual([[r["ID"] for r in c[0]] for c in rest], [["4", "5", "6"], ["7"]])
        self.assertEqual(rest[0][1], ["3"])
        with self.assertRaises(ValueError):
            list(v.snapshot_chunks(self.PagedDb(7), "S", "T", 3, key_from=["3", "x"]))
