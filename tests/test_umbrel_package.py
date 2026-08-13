from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
UMBREL_COMPOSE = ROOT / "jjlab-hyperlab" / "docker-compose.yml"


def _load_yaml(path: Path) -> dict[str, object]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _assert_hardened_service(service: dict[str, object]) -> None:
    assert service["user"] == "1000:1000"
    assert service["init"] is True
    assert service["read_only"] is True
    assert service["privileged"] is False
    assert "ALL" in service["cap_drop"]
    assert "no-new-privileges:true" in service["security_opt"]
    for forbidden in ("cap_add", "devices", "device_cgroup_rules", "network_mode", "pid", "ipc"):
        assert forbidden not in service
    assert service["restart"] == "unless-stopped"
    assert service["stop_signal"] == "SIGTERM"
    assert int(service["pids_limit"]) > 0
    assert float(service["cpus"]) > 0
    assert service["mem_limit"]
    assert service["mem_reservation"]
    assert "noexec" in " ".join(service["tmpfs"])
    assert "nosuid" in " ".join(service["tmpfs"])
    assert "nodev" in " ".join(service["tmpfs"])

    healthcheck = service["healthcheck"]
    assert healthcheck["test"][0] == "CMD"
    assert healthcheck["interval"]
    assert healthcheck["timeout"]
    assert int(healthcheck["retries"]) >= 3
    assert healthcheck["start_period"]

    logging = service["logging"]
    assert logging["driver"] == "json-file"
    assert logging["options"]["max-size"]
    assert int(logging["options"]["max-file"]) >= 3

    environment = service["environment"]
    assert {
        "HYPERLAB_MODE": "readonly",
        "HYPERLAB_DATA_DIR": "/data",
        "HYPERLAB_CONFIG": "/app/config/research.toml",
        "HYPERLAB_REQUIRE_PERSISTENT_LAYOUT": "1",
    }.items() <= environment.items()


def test_docker_context_excludes_secret_and_persistent_state_files() -> None:
    patterns = {
        line.strip()
        for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
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
        "data",
        "reports",
        ".pytest-tmp*",
        "**/.pytest-tmp*",
        ".pytest*",
        "**/.pytest*",
    }

    assert required <= patterns


def test_runtime_image_is_non_root_read_only_ready_and_does_not_declare_anonymous_state() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert dockerfile.startswith(
        "FROM python:3.12.13-alpine3.24@sha256:"
        "6d43704baacd1bfbe7c295d7f13079d5d8104ed33568873133f8fc69980419df AS runtime\n"
    )
    assert 'org.opencontainers.image.version="0.2.1"' in dockerfile
    assert "adduser -S -D -H -u 1000" in dockerfile
    assert "-s /sbin/nologin" in dockerfile
    assert "USER hyperlab" in dockerfile
    assert dockerfile.index("USER hyperlab") < dockerfile.index("ENTRYPOINT")
    assert "VOLUME" not in dockerfile
    assert "pip install --upgrade pip" not in dockerfile
    assert "COPY requirements-runtime.lock ./" in dockerfile
    assert "--only-binary=:all:" in dockerfile
    assert "--require-hashes --requirement requirements-runtime.lock" in dockerfile
    assert "pip install ." not in dockerfile
    assert "PYTHONPATH=/app/src" in dockerfile
    assert 'ENTRYPOINT ["python", "-m", "hyperlab.cli"]' in dockerfile
    assert "chown -R hyperlab:hyperlab /app" not in dockerfile
    assert "/data/lake /data/reports /data/paper /data/backups" in dockerfile


def test_local_compose_has_separate_state_readiness_limits_and_loopback_only_ui() -> None:
    compose = _load_yaml(ROOT / "compose.yaml")
    services = compose["services"]
    dashboard = services["dashboard"]
    collector = services["collector"]

    _assert_hardened_service(dashboard)
    _assert_hardened_service(collector)

    assert dashboard["ports"] == ["127.0.0.1:8000:8000"]
    assert "ports" not in collector
    assert dashboard["networks"] == ["dashboard"]
    assert collector["networks"] == ["collector_egress"]
    assert compose["networks"]["dashboard"]["internal"] is True
    assert compose["networks"]["collector_egress"]["internal"] is False

    assert set(dashboard["volumes"]) == {
        "./data/runtime:/data:ro",
        "./data/reports:/data/reports:ro",
        "./data/paper:/data/paper:ro",
        "./data/config:/app/config:ro",
    }
    assert set(collector["volumes"]) == {
        "./data/runtime:/data",
        "./data/market:/data/lake",
        "./data/config:/app/config:ro",
    }
    assert collector["environment"]["HYPERLAB_MIN_FREE_BYTES"] == "2147483648"
    assert collector["environment"]["HYPERLAB_MIN_FREE_PERCENT"] == "5.0"
    assert "HYPERLAB_MIN_FREE_BYTES" not in dashboard["environment"]

    dashboard_health = " ".join(dashboard["healthcheck"]["test"])
    collector_health = " ".join(collector["healthcheck"]["test"])
    assert "http://127.0.0.1:8000/ready" in dashboard_health
    assert "runtime_status.json" in collector_health
    assert "stale_channels" in collector_health
    assert "orders_enabled" in collector_health


def test_umbrel_package_is_rooted_immutable_isolated_and_fail_closed() -> None:
    store_path = ROOT / "umbrel-app-store.yml"
    manifest_path = ROOT / "jjlab-hyperlab" / "umbrel-app.yml"

    assert store_path.is_file(), "Umbrel requires umbrel-app-store.yml at repository root"
    assert UMBREL_COMPOSE.is_file()
    assert manifest_path.is_file()

    store = _load_yaml(store_path)
    manifest = _load_yaml(manifest_path)
    compose_text = UMBREL_COMPOSE.read_text(encoding="utf-8")
    compose = _load_yaml(UMBREL_COMPOSE)
    services = compose["services"]

    assert manifest["id"].startswith(f'{store["id"]}-')
    assert manifest["category"] == "finance"
    assert manifest["version"] == "0.2.1"
    assert "/vREPLACE_WITH_IMAGE_VERSION/jjlab-hyperlab/icon.svg" in manifest["icon"]
    assert "/main/" not in manifest["icon"]
    assert "read-only" in manifest["description"]

    assert set(services) == {"app_proxy", "dashboard", "collector"}
    assert "volumes" not in services["app_proxy"]
    assert "ports" not in services["app_proxy"]
    assert services["app_proxy"]["user"] == "1000:1000"
    assert services["app_proxy"]["init"] is True
    assert services["app_proxy"]["read_only"] is True
    assert services["app_proxy"]["privileged"] is False
    assert services["app_proxy"]["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in services["app_proxy"]["security_opt"]
    for forbidden in ("cap_add", "devices", "device_cgroup_rules", "network_mode", "pid", "ipc"):
        assert forbidden not in services["app_proxy"]
    assert int(services["app_proxy"]["pids_limit"]) > 0
    assert float(services["app_proxy"]["cpus"]) > 0
    assert services["app_proxy"]["mem_limit"]
    assert services["app_proxy"]["restart"] == "unless-stopped"
    assert services["app_proxy"]["healthcheck"]["test"][:3] == ["CMD", "node", "-e"]
    assert "http://127.0.0.1:8000/" in services["app_proxy"]["healthcheck"]["test"][3]
    assert services["app_proxy"]["logging"]["options"] == {
        "max-size": "10m",
        "max-file": "3",
    }
    assert set(services["app_proxy"]["networks"]) == {"dashboard", "default"}
    assert compose["networks"]["default"] == {
        "external": True,
        "name": "umbrel_main_network",
    }

    dashboard = services["dashboard"]
    collector = services["collector"]
    _assert_hardened_service(dashboard)
    _assert_hardened_service(collector)

    immutable_image = (
        "ghcr.io/REPLACE_WITH_GITHUB_USER/REPLACE_WITH_REPOSITORY:"
        "REPLACE_WITH_IMAGE_VERSION@sha256:REPLACE_WITH_IMAGE_DIGEST"
    )
    assert dashboard["image"] == immutable_image
    assert collector["image"] == immutable_image
    assert "ports" not in dashboard
    assert dashboard["expose"] == ["8000"]
    assert "ports" not in collector
    assert dashboard["networks"] == ["dashboard"]
    assert collector["networks"] == ["collector_egress"]
    assert compose["networks"]["dashboard"]["internal"] is True
    assert compose["networks"]["collector_egress"]["internal"] is False

    required_root = "${APP_DATA_DIR:?APP_DATA_DIR must be set}/data"
    assert set(dashboard["volumes"]) == {
        f"{required_root}/runtime:/data:ro",
        f"{required_root}/reports:/data/reports:ro",
        f"{required_root}/paper:/data/paper:ro",
        f"{required_root}/config:/app/config:ro",
    }
    assert set(collector["volumes"]) == {
        f"{required_root}/runtime:/data",
        f"{required_root}/market:/data/lake",
        f"{required_root}/config:/app/config:ro",
    }
    assert collector["environment"]["HYPERLAB_MIN_FREE_BYTES"] == "2147483648"
    assert collector["environment"]["HYPERLAB_MIN_FREE_PERCENT"] == "5.0"
    assert "HYPERLAB_MIN_FREE_BYTES" not in dashboard["environment"]
    assert all(volume.endswith(":ro") for volume in dashboard["volumes"])

    assert collector["command"][0] == "collect"
    assert dashboard["command"][0] == "serve"
    assert collector["stop_grace_period"] == "60s"
    assert "http://127.0.0.1:8000/ready" in " ".join(dashboard["healthcheck"]["test"])
    assert "runtime_status.json" in " ".join(collector["healthcheck"]["test"])

    assert "/var/run/docker.sock" not in compose_text
    assert "network_mode: host" not in compose_text
    for forbidden in ("PRIVATE_KEY", "SEED_PHRASE", "MNEMONIC", "WALLET_KEY", "API_KEY"):
        assert forbidden not in compose_text


def test_umbrel_persistent_directories_and_read_only_config_are_seeded() -> None:
    data_root = ROOT / "jjlab-hyperlab" / "data"
    for name in ("runtime", "market", "reports", "paper", "backups", "config"):
        assert (data_root / name).is_dir()
        assert (data_root / name / ".hyperlab-volume").read_text(encoding="utf-8") == (
            f"hyperlab-{name}-volume-v1\n"
        )

    config = (data_root / "config" / "research.toml").read_text(encoding="utf-8")
    assert 'mode = "readonly"' in config
    assert 'data_dir = "/data"' in config
    for forbidden in ("private_key", "seed_phrase", "mnemonic", "wallet", "api_key"):
        assert forbidden not in config.casefold()
