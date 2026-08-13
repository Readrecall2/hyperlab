# Store Umbrel privé HyperLab 0.2.1

Les fichiers `umbrel-app-store.yml` et `jjlab-hyperlab/` restent à la racine du
dépôt, conformément au format Community App Store Umbrel.

Le tag de release est d'abord publié avec le template contrôlé. Après réussite des
tests, scans `amd64`/`arm64`, SBOM, attestations et signature, téléchargez le reçu
`signed-release-receipt-v0.2.1` depuis le run vert exact. Le préparateur refuse tout
digest qui ne correspond pas à ce reçu signé et au tag SemVer effectivement promu :

```powershell
.\.venv\Scripts\python.exe scripts\prepare_umbrel_store.py VOTRE_NOM_GITHUB `
  --repository hyperlab `
  --image-version 0.2.1 `
  --image-digest DIGEST_MULTIARCH_64_HEX `
  --release-receipt .\release-evidence\release-receipt.json `
  --receipt-bundle .\release-evidence\release-receipt.sigstore.json
.\.venv\Scripts\python.exe scripts\verify_manifest.py --write
.\.venv\Scripts\python.exe scripts\verify_release.py --prepared --tag v0.2.1 --check-manifest
```

Les deux services applicatifs doivent utiliser le même `tag@sha256:digest`. Aucun
placeholder ou tag `latest` n'est déployable. L'application contient uniquement un
collecteur public et un dashboard local read-only, sans secret ni exécuteur d'ordre.

Avant update, rollback, reinstall ou uninstall, suivez le runbook de
[`UMBREL_SETUP.md`](UMBREL_SETUP.md). Umbrel peut supprimer `${APP_DATA_DIR}` pendant
la désinstallation : seule une sauvegarde vérifiée et restaurée avec succès hors de ce
répertoire constitue une conservation valide.
