# SPDX-License-Identifier: Apache-2.0
"""Local test lab consistency: catalog.yaml (source of truth) <-> generated files <-> docker-compose.lab.yml.

Run: python -m pytest tests/static -q   (needs PyYAML)
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LAB = REPO / "lab"
yaml = pytest.importorskip("yaml")


def _catalog() -> dict:
    return yaml.safe_load((LAB / "catalog.yaml").read_text(encoding="utf-8"))


def _compose() -> dict:
    return yaml.safe_load((LAB / "docker-compose.lab.yml").read_text(encoding="utf-8"))


def test_generated_files_up_to_date() -> None:
    r = subprocess.run([sys.executable, str(LAB / "gen_catalog.py"), "--check"], capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    assert json.loads((LAB / "catalog.json").read_text(encoding="utf-8")) == _catalog()


def test_compose_matches_catalog() -> None:
    services = _compose()["services"]
    for p in _catalog()["profiles"]:
        if p["availability"] != "image":
            continue
        images = set()
        for s in p["services"]:
            assert s in services, f"{p['id']}: service {s} missing in compose"
            svc = services[s]
            assert p["id"] in svc["profiles"], f"{s}: compose profile {p['id']} missing"
            assert svc.get("container_name") == s and s.startswith("alelt-lab-")
            images.add(re.sub(r"^\$\{[A-Z0-9_]+:-(.*)\}$", r"\1", svc["image"]))
            for port in svc.get("ports", []):
                assert str(port).startswith("127.0.0.1:"), f"{s}: ports must bind to 127.0.0.1 only"
            assert "/var/run/docker.sock" not in json.dumps(svc), f"{s}: never mount the Docker socket"
        assert images == set(p["images"]), f"{p['id']}: compose images {images} != catalog {p['images']}"
        published = set()
        for s in p["services"]:
            for port in services[s].get("ports", []):
                m = re.match(r"127\.0\.0\.1:\$\{[A-Z0-9_]+:-(\d+)\}:\d+", str(port))
                assert m, f"{s}: port {port} must be 127.0.0.1:${{LAB_PORT_X:-default}}:container"
                published.add(int(m.group(1)))
        assert published == set(p["ports"].values()), f"{p['id']}: ports {published} != catalog {p['ports']}"


def test_images_pinned_and_names() -> None:
    for p in _catalog()["profiles"]:
        for img in p.get("images", []):
            assert ":" in img.rsplit("/", 1)[-1] and not img.endswith(":latest"), f"{img}: pin a tag"
    assert _compose()["networks"]["lab"]["name"] == "alelt-lab"
    assert _compose()["name"] == "alelt-lab"
