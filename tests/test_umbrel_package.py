from __future__ import annotations

from pathlib import Path

import yaml


def test_docker_context_excludes_secret_files() -> None:
    root = Path(__file__).resolve().parents[1]
    patterns = {
        line.strip()
        for line in (root / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    required = {
        ".env",
        ".env.*",
        "*.key",
        "*.pem",
        "*.p12",
        "*.pfx",
        "*.keystore",
        "*keystore*",
    }

    assert required <= patterns


def test_umbrel_package_is_rooted_and_hardened() -> None:
    root = Path(__file__).resolve().parents[1]
    store_path = root / "umbrel-app-store.yml"
    compose_path = root / "jjlab-hyperlab" / "docker-compose.yml"
    manifest_path = root / "jjlab-hyperlab" / "umbrel-app.yml"

    assert store_path.is_file(), "Umbrel requires umbrel-app-store.yml at repository root"
    assert compose_path.is_file()
    assert manifest_path.is_file()

    store = yaml.safe_load(store_path.read_text(encoding="utf-8"))
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    compose_text = compose_path.read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_text)

    assert manifest["id"].startswith(f'{store["id"]}-')
    assert manifest["category"] == "finance"
    assert "main/jjlab-hyperlab/icon.svg" in manifest["icon"]

    for name in ("dashboard", "collector"):
        service = compose["services"][name]
        assert service["user"] == "1000:1000"
        assert service["read_only"] is True
        assert "ALL" in service["cap_drop"]
        assert "no-new-privileges:true" in service["security_opt"]
        assert service.get("privileged") is not True

    assert "/var/run/docker.sock" not in compose_text
    assert "PRIVATE_KEY" not in compose_text
    assert "SEED_PHRASE" not in compose_text
