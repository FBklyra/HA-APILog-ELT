"""Prod -> test host mapping (required step before an import apply)."""
from __future__ import annotations

from types import SimpleNamespace

from alelt_import import hostmap
from alelt_import.guard import DenyList


def test_hosts_apply_keep_ports_paths_credentials() -> None:
    s = {"host": "db2prod.corp.local", "port": 50000, "url": "postgresql://u:pw@pgprod.corp:5432/land",
         "nested": {"server": "sqlprod:1433"}, "account": "xy12345.ca-central-1"}
    assert hostmap.hosts(s) == ["db2prod.corp.local", "pgprod.corp", "sqlprod", "xy12345.ca-central-1"]
    m = hostmap.normalize({"DB2PROD.corp.local": "db2test", "pgprod.corp": "localhost", "sqlprod": "sqltest",
                           "xy12345.ca-central-1": "xy99999.ca-central-1"})
    out = hostmap.apply(s, m)
    assert out["host"] == "db2test" and out["port"] == 50000
    assert out["url"] == "postgresql://u:pw@localhost:5432/land"
    assert out["nested"]["server"] == "sqltest:1433" and out["account"] == "xy99999.ca-central-1"


def test_check_requires_every_host_and_keeps_denylist_strict() -> None:
    pc = SimpleNamespace(connection_id="c", source={"settings": {"host": "db2prod.corp.local"}},
                         destination={"settings": {"host": "localhost"}})
    deny = DenyList.of(["*.corp.local"])
    assert hostmap.check([pc], {}, deny) == (["db2prod.corp.local", "localhost"], [])
    assert hostmap.check([pc], {"db2prod.corp.local": "db2.corp.local", "localhost": "localhost"}, deny) == (
        [], ["db2.corp.local"])
    assert hostmap.check([pc], {"db2prod.corp.local": "db2test", "localhost": "localhost"}, deny) == ([], [])
