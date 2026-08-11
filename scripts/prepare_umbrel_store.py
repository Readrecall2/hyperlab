from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Personnalise le store Umbrel privé HyperLab.")
    parser.add_argument("github_user", help="Nom du compte ou de l'organisation GitHub")
    parser.add_argument("--repository", default="hyperlab")
    parser.add_argument("--tag", default="0.2.0")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    store_paths = [root / "umbrel-app-store.yml", root / "jjlab-hyperlab"]
    replacements = {
        "REPLACE_WITH_GITHUB_USER": args.github_user,
        "REPLACE_WITH_REPOSITORY": args.repository,
        "REPLACE_WITH_TAG": args.tag,
    }
    touched = 0
    for entry in store_paths:
        paths = [entry] if entry.is_file() else list(entry.rglob("*"))
        for path in paths:
            if not path.is_file() or path.suffix not in {".yml", ".yaml", ".md"}:
                continue
            text = path.read_text(encoding="utf-8")
            updated = text
            for old, new in replacements.items():
                updated = updated.replace(old, new)
            if updated != text:
                path.write_text(updated, encoding="utf-8")
                touched += 1
    print(f"Store configuré pour github.com/{args.github_user}/{args.repository}")
    print(f"Image attendue: ghcr.io/{args.github_user}/{args.repository}:{args.tag}")
    print(f"Fichiers modifiés: {touched}")


if __name__ == "__main__":
    main()
