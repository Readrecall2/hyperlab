# ruff: noqa: RUF001

from __future__ import annotations

H1_PAGE = """<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="dark">
  <meta name="description" content="Cockpit local read-only de la campagne HyperLab H1 Ghost.">
  <title>HyperLab · H1 Ghost Observatory</title>
  <link rel="stylesheet" href="/assets/h1-dashboard.css">
  <script src="/assets/h1-dashboard.js" defer></script>
</head>
<body>
  <a class="skip-link" href="#main">Aller au contenu principal</a>
  <div class="safety-ribbon" role="status" aria-label="Frontière de sécurité permanente">
    <span aria-hidden="true">◆</span>
    GHOST ONLY <span aria-hidden="true">·</span> PUBLIC DATA <span aria-hidden="true">·</span> ORDERS IMPOSSIBLE
    <span class="sr-only">READ-ONLY — ORDRES IMPOSSIBLES</span>
  </div>

  <header class="topbar">
    <a class="brand" href="/" aria-label="HyperLab H1, accueil">
      <span class="brand-mark" aria-hidden="true">H1</span>
      <span><strong>HyperLab</strong><small>Ghost Observatory</small></span>
    </a>
    <div class="topbar-actions">
      <label class="fixture-control" for="fixture-select">
        <span>État de démonstration</span>
        <select id="fixture-select" aria-describedby="fixture-help"></select>
      </label>
      <span class="connection-pill" id="connection-pill"><span class="status-dot" aria-hidden="true"></span>Connexion locale</span>
    </div>
  </header>

  <div class="fixture-banner" id="fixture-banner" role="note" hidden>
    <strong>Démonstration synthétique</strong>
    <span id="fixture-help">Ces valeurs illustrent l’interface. Elles ne prouvent ni performance, ni alpha, ni état réel de campagne.</span>
  </div>

  <main id="main">
    <section class="hero surface" aria-labelledby="campaign-heading">
      <div class="hero-copy">
        <p class="eyebrow">Campagne prospective · 14 jours</p>
        <div class="status-heading">
          <span class="status-orb" aria-hidden="true"></span>
          <div>
            <p class="status-kicker" id="status-kicker">Lecture de l’état…</p>
            <h1 id="campaign-heading">Observatoire H1</h1>
          </div>
        </div>
        <p class="hero-summary" id="hero-summary">Connexion au snapshot local en lecture seule.</p>
        <div class="hero-meta">
          <span id="campaign-id">Campagne · NON DISPONIBLE</span>
          <span id="last-update">Dernière mise à jour · NON DISPONIBLE</span>
        </div>
      </div>
      <div class="hero-signal" aria-label="Résumé immédiat">
        <div><span>Collecte</span><strong id="hero-collection">NON DISPONIBLE</strong></div>
        <div><span>Données</span><strong id="hero-freshness">NON DISPONIBLE</strong></div>
        <div><span>Intégrité</span><strong id="hero-integrity">NON DISPONIBLE</strong></div>
      </div>
    </section>

    <section class="campaign-progress surface" aria-labelledby="progress-title">
      <div class="section-heading">
        <div>
          <p class="eyebrow">Où en est la campagne ?</p>
          <h2 id="progress-title">Progression sur 14 jours</h2>
        </div>
        <div class="progress-readout"><strong id="progress-percent">NON DISPONIBLE</strong><span id="phase-label">Phase inconnue</span></div>
      </div>
      <div class="phase-track" aria-label="Découpage chronologique Train, Validation, Holdout">
        <div class="phase phase-train"><strong>Train</strong><span>J0–J7 · observation initiale</span></div>
        <div class="phase phase-validation"><strong>Validation</strong><span>J7–J10 · vérification séparée</span></div>
        <div class="phase phase-holdout" id="holdout-phase"><strong>Holdout</strong><span id="holdout-label">J10–J14 · SEALED</span></div>
        <div class="progress-fill" id="progress-fill" aria-hidden="true"></div>
      </div>
      <div class="progress-foot">
        <span id="progress-time">Dates non disponibles</span>
        <span class="seal-note" id="seal-note">Le holdout scellé ne révèle aucune métrique.</span>
      </div>
    </section>

    <section aria-labelledby="overview-title">
      <div class="section-heading section-heading-simple">
        <div><p class="eyebrow">L’essentiel en un regard</p><h2 id="overview-title">Santé des données</h2></div>
      </div>
      <div class="priority-alert" id="priority-alert" role="status">
        <span class="alert-icon" aria-hidden="true">✓</span>
        <div><strong id="alert-title">Vérification en cours</strong><p id="alert-detail">Le cockpit attend son premier snapshot.</p></div>
      </div>
      <div class="metric-grid" id="collection-metrics" aria-label="Métriques de collecte"></div>
      <div class="safety-grid">
        <article class="surface safety-card">
          <div class="card-heading"><div><p class="eyebrow">Protection</p><h3>Garde-fous actifs</h3></div><span class="soft-badge">Fail-closed</span></div>
          <ul class="clean-list" id="kill-rules"></ul>
        </article>
        <article class="surface safety-card">
          <div class="card-heading"><div><p class="eyebrow">Attention requise</p><h3>Flux à surveiller</h3></div><span class="soft-badge" id="stale-count">0 signalé</span></div>
          <div id="stale-feeds" class="empty-state">Aucun flux stale publié.</div>
        </article>
      </div>
    </section>

    <section aria-labelledby="strategy-title">
      <div class="section-heading section-heading-simple">
        <div><p class="eyebrow">Ce que la politique décide</p><h2 id="strategy-title">Activité Ghost</h2></div>
      </div>
      <div class="strategy-layout">
        <article class="surface spacious-card">
          <div class="card-heading"><div><h3>Décisions de cotation</h3><p>Un seul côté à la fois, ou aucune cotation.</p></div><span class="soft-badge">500 ms</span></div>
          <div class="decision-bars" id="decision-bars"></div>
          <div class="mini-metrics" id="ghost-metrics"></div>
        </article>
        <article class="surface spacious-card">
          <div class="card-heading"><div><h3>Pourquoi aucune cotation ?</h3><p>Principaux refus de sécurité ou de sélectivité.</p></div></div>
          <ol class="reason-list" id="no-quote-reasons"></ol>
        </article>
      </div>
    </section>

    <section aria-labelledby="economics-title">
      <div class="section-heading section-heading-simple">
        <div><p class="eyebrow">Seulement quand la provenance le permet</p><h2 id="economics-title">Exposition, coûts et résultats</h2></div>
        <span class="evidence-badge" id="evidence-status">ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE</span>
      </div>
      <div class="surface economics-panel">
        <div class="provenance-line"><span aria-hidden="true">ⓘ</span><span id="economics-provenance">Les valeurs absentes restent NON DISPONIBLE.</span></div>
        <div class="economics-grid" id="economics-metrics"></div>
        <div class="exposure-row">
          <div><h3>Inventaire publié</h3><div class="chip-list" id="inventory"></div></div>
          <div><h3>Closeouts non résolus</h3><div class="chip-list" id="closeouts"></div></div>
        </div>
      </div>
    </section>

    <section aria-labelledby="markets-title">
      <div class="section-heading section-heading-simple">
        <div><p class="eyebrow">Couverture du flux public</p><h2 id="markets-title">Marchés et feeds</h2></div>
      </div>
      <div class="market-grid" id="market-grid"></div>
    </section>

    <section class="advanced-section" aria-labelledby="advanced-title">
      <div class="section-heading section-heading-simple">
        <div><p class="eyebrow">Preuves et discipline de recherche</p><h2 id="advanced-title">Détails avancés</h2></div>
      </div>
      <div class="details-grid">
        <details class="surface detail-card" open>
          <summary><span>Variantes préenregistrées</span><small>La primaire et toutes les variantes restent visibles.</small></summary>
          <div class="detail-content" id="variants"></div>
        </details>
        <details class="surface detail-card">
          <summary><span>Gates économiques</span><small>Aucun classement anticipé.</small></summary>
          <div class="detail-content" id="economic-gates"></div>
        </details>
        <details class="surface detail-card">
          <summary><span>Identités et intégrité</span><small>Hashes, politique et head authentifié.</small></summary>
          <dl class="identity-list" id="identities"></dl>
        </details>
        <details class="surface detail-card">
          <summary><span>Rapports autorisés</span><small>Téléchargements allowlistés après ouverture.</small></summary>
          <div class="detail-content" id="reports"></div>
        </details>
      </div>
    </section>

    <section aria-labelledby="timeline-title">
      <div class="section-heading section-heading-simple">
        <div><p class="eyebrow">Historique récent et borné</p><h2 id="timeline-title">Incidents et changements d’état</h2></div>
      </div>
      <ol class="surface timeline" id="timeline"></ol>
    </section>

    <footer>
      <p><strong>Lecture seule par conception.</strong> Aucun bouton d’ordre, aucune commande système, aucun secret.</p>
      <p>Testnet reste séparé, sans attendre Gate D. Tout exécuteur Micro/Mainnet reste séparé et bloqué par ses preuves et revues dédiées.</p>
      <p class="technical-links">API locale : <a href="/api/h1/snapshot">snapshot H1</a> · <a href="/health/live">liveness</a> · <a href="/ready">readiness</a></p>
    </footer>
  </main>
  <div class="sr-only" aria-live="polite" id="live-region"></div>
</body>
</html>
"""


H1_CSS = r"""
:root {
  --night-950: #071017;
  --night-900: #0b1721;
  --night-850: #10202c;
  --night-800: #142734;
  --surface: #10212d;
  --surface-raised: #152a38;
  --surface-soft: #0d1c27;
  --line: #29404d;
  --line-soft: #203641;
  --text: #f2f7f9;
  --text-soft: #c2d0d6;
  --muted: #8fa5ae;
  --teal: #55d6c2;
  --teal-soft: #183f42;
  --cyan: #69cde4;
  --amber: #f3be6b;
  --amber-soft: #3d3020;
  --red: #ff807d;
  --red-soft: #432527;
  --blue-soft: #1a3342;
  --shadow-1: 0 18px 55px rgba(0, 0, 0, .24);
  --shadow-2: 0 8px 24px rgba(0, 0, 0, .18);
  --radius-sm: 12px;
  --radius-md: 18px;
  --radius-lg: 26px;
  --space-1: .4rem;
  --space-2: .7rem;
  --space-3: 1rem;
  --space-4: 1.4rem;
  --space-5: 2rem;
  --space-6: 3rem;
  --content: 1240px;
}

* { box-sizing: border-box; }
html { scroll-behavior: smooth; }
body {
  margin: 0;
  min-width: 320px;
  background:
    radial-gradient(circle at 82% -8%, rgba(80, 162, 178, .12), transparent 32rem),
    linear-gradient(180deg, var(--night-900), var(--night-950) 42rem);
  color: var(--text);
  font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  font-size: 16px;
  line-height: 1.55;
}

a { color: var(--cyan); text-underline-offset: .18em; }
a:hover { color: #a9ecfa; }
button, select { font: inherit; }
:focus-visible { outline: 3px solid var(--amber); outline-offset: 3px; }
.skip-link { position: fixed; left: 1rem; top: -5rem; z-index: 100; padding: .8rem 1rem; border-radius: var(--radius-sm); background: var(--text); color: var(--night-950); }
.skip-link:focus { top: 1rem; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }

.safety-ribbon {
  position: sticky;
  top: 0;
  z-index: 30;
  display: flex;
  justify-content: center;
  align-items: center;
  gap: .65rem;
  min-height: 38px;
  padding: .5rem 1rem;
  background: #0b2b31;
  border-bottom: 1px solid #28636a;
  color: #baf5ea;
  font-size: .77rem;
  font-weight: 800;
  letter-spacing: .14em;
  text-align: center;
}

.topbar {
  width: min(calc(100% - 2rem), var(--content));
  margin: 0 auto;
  padding: 1.15rem 0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
}
.brand { display: inline-flex; align-items: center; gap: .8rem; color: var(--text); text-decoration: none; }
.brand-mark { display: grid; place-items: center; width: 44px; height: 44px; border: 1px solid #39707a; border-radius: 14px; background: linear-gradient(145deg, #173945, #10222e); color: var(--teal); font-weight: 900; letter-spacing: -.04em; box-shadow: var(--shadow-2); }
.brand strong, .brand small { display: block; }
.brand strong { font-size: 1.05rem; letter-spacing: -.02em; }
.brand small { margin-top: -.12rem; color: var(--muted); font-size: .76rem; }
.topbar-actions { display: flex; align-items: end; gap: 1rem; }
.fixture-control { display: grid; gap: .25rem; color: var(--muted); font-size: .72rem; }
.fixture-control select { min-height: 42px; max-width: 240px; padding: .55rem 2.2rem .55rem .75rem; border: 1px solid var(--line); border-radius: 10px; background: var(--surface-soft); color: var(--text); }
.connection-pill, .soft-badge, .evidence-badge { display: inline-flex; align-items: center; gap: .45rem; min-height: 34px; padding: .4rem .7rem; border: 1px solid var(--line); border-radius: 999px; background: var(--surface-soft); color: var(--text-soft); font-size: .75rem; font-weight: 700; }
.status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--teal); box-shadow: 0 0 0 4px rgba(85, 214, 194, .12); }
.connection-pill.is-offline .status-dot { background: var(--amber); box-shadow: 0 0 0 4px rgba(243, 190, 107, .12); }

.fixture-banner { width: min(calc(100% - 2rem), var(--content)); margin: 0 auto 1rem; padding: .8rem 1rem; border: 1px solid #80653b; border-radius: var(--radius-sm); background: var(--amber-soft); color: #f7dcae; }
.fixture-banner strong { margin-right: .6rem; }
main { width: min(calc(100% - 2rem), var(--content)); margin: 0 auto; padding: .2rem 0 5rem; }
section { margin-top: var(--space-6); }
main > section:first-child { margin-top: var(--space-4); }
.surface { border: 1px solid var(--line-soft); border-radius: var(--radius-lg); background: linear-gradient(145deg, rgba(21, 42, 56, .96), rgba(13, 28, 39, .98)); box-shadow: var(--shadow-1); }

.hero { min-height: 330px; padding: clamp(1.6rem, 3.4vw, 2.7rem); display: grid; grid-template-columns: minmax(0, 1.5fr) minmax(260px, .65fr); align-items: end; gap: clamp(2rem, 5vw, 5rem); overflow: hidden; position: relative; }
.hero::after { content: ""; position: absolute; right: -8rem; top: -11rem; width: 29rem; height: 29rem; border-radius: 50%; border: 1px solid rgba(105, 205, 228, .11); box-shadow: inset 0 0 0 5rem rgba(105, 205, 228, .018), inset 0 0 0 10rem rgba(105, 205, 228, .014); pointer-events: none; }
.hero-copy, .hero-signal { position: relative; z-index: 1; }
.eyebrow { margin: 0 0 .45rem; color: var(--cyan); font-size: .72rem; font-weight: 800; letter-spacing: .12em; text-transform: uppercase; }
.status-heading { display: flex; align-items: center; gap: 1rem; }
.status-orb { flex: 0 0 auto; width: 18px; height: 18px; border: 4px solid rgba(85, 214, 194, .18); border-radius: 50%; background: var(--teal); box-shadow: 0 0 0 7px rgba(85, 214, 194, .08); }
.status-kicker { margin: 0; color: var(--text-soft); font-size: .9rem; font-weight: 700; }
h1 { margin: 0; max-width: 15ch; font-size: clamp(2.35rem, 5.2vw, 4.8rem); line-height: .97; letter-spacing: -.062em; }
h2 { margin: 0; font-size: clamp(1.55rem, 3vw, 2.25rem); line-height: 1.15; letter-spacing: -.035em; }
h3 { margin: 0; font-size: 1.03rem; letter-spacing: -.015em; }
.hero-summary { max-width: 63ch; margin: 1.4rem 0 1.25rem; color: var(--text-soft); font-size: clamp(1rem, 1.7vw, 1.18rem); }
.hero-meta { display: flex; flex-wrap: wrap; gap: .65rem 1.4rem; color: var(--muted); font-size: .83rem; }
.hero-signal { display: grid; gap: .55rem; padding: .65rem; border: 1px solid var(--line); border-radius: var(--radius-md); background: rgba(7, 16, 23, .42); }
.hero-signal div { padding: .9rem 1rem; border-radius: var(--radius-sm); background: rgba(255, 255, 255, .025); }
.hero-signal span, .metric-card span, .mini-metric span { display: block; color: var(--muted); font-size: .74rem; }
.hero-signal strong { display: block; margin-top: .18rem; font-size: 1.02rem; }
body[data-tone="warning"] .status-orb { background: var(--amber); border-color: rgba(243, 190, 107, .2); box-shadow: 0 0 0 7px rgba(243, 190, 107, .08); }
body[data-tone="danger"] .status-orb { background: var(--red); border-color: rgba(255, 128, 125, .22); box-shadow: 0 0 0 7px rgba(255, 128, 125, .08); }

.campaign-progress { padding: clamp(1.3rem, 3vw, 2rem); }
.section-heading { display: flex; align-items: end; justify-content: space-between; gap: 1rem; margin-bottom: 1.35rem; }
.section-heading-simple { margin: 0 0 1.2rem; }
.progress-readout { text-align: right; }
.progress-readout strong, .progress-readout span { display: block; }
.progress-readout strong { font-size: 1.8rem; }
.progress-readout span { color: var(--muted); font-size: .8rem; }
.phase-track { position: relative; display: grid; grid-template-columns: 7fr 3fr 4fr; min-height: 96px; overflow: hidden; border: 1px solid var(--line); border-radius: var(--radius-md); background: var(--night-950); }
.phase { z-index: 2; display: flex; flex-direction: column; justify-content: center; gap: .18rem; padding: 1rem; border-right: 1px solid var(--line); }
.phase:last-of-type { border-right: 0; }
.phase strong { font-size: .92rem; }
.phase span { color: var(--muted); font-size: .72rem; }
.phase-holdout { background-image: repeating-linear-gradient(135deg, rgba(243, 190, 107, .055) 0, rgba(243, 190, 107, .055) 8px, transparent 8px, transparent 16px); }
.phase-holdout.is-open { background-image: none; background-color: rgba(85, 214, 194, .06); }
.progress-fill { position: absolute; inset: auto auto 0 0; z-index: 3; width: 0; height: 5px; background: linear-gradient(90deg, var(--teal), var(--cyan)); transition: width .5s ease; }
.progress-foot { display: flex; justify-content: space-between; gap: 1rem; margin-top: .85rem; color: var(--muted); font-size: .8rem; }
.seal-note { color: var(--amber); text-align: right; }

.priority-alert { display: flex; align-items: center; gap: .9rem; min-height: 76px; margin-bottom: 1rem; padding: 1rem 1.15rem; border: 1px solid #2d5d5c; border-radius: var(--radius-md); background: var(--teal-soft); }
.priority-alert.warning { border-color: #765b34; background: var(--amber-soft); }
.priority-alert.danger { border-color: #7f4142; background: var(--red-soft); }
.alert-icon { display: grid; place-items: center; flex: 0 0 auto; width: 34px; height: 34px; border-radius: 50%; background: rgba(255, 255, 255, .08); font-weight: 900; }
.priority-alert p { margin: .14rem 0 0; color: var(--text-soft); font-size: .88rem; }
.metric-grid { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: .8rem; }
.metric-card { min-height: 126px; padding: 1rem; border: 1px solid var(--line-soft); border-radius: var(--radius-md); background: var(--surface); box-shadow: var(--shadow-2); }
.metric-card strong { display: block; margin: .45rem 0 .3rem; font-size: clamp(1.2rem, 2vw, 1.65rem); letter-spacing: -.035em; }
.metric-card small { color: var(--muted); font-size: .68rem; }
.safety-grid, .strategy-layout { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1rem; margin-top: 1rem; }
.safety-card, .spacious-card { padding: 1.25rem; }
.card-heading { display: flex; align-items: start; justify-content: space-between; gap: .8rem; }
.card-heading p { margin: .25rem 0 0; color: var(--muted); font-size: .8rem; }
.clean-list { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: .6rem; margin: 1rem 0 0; padding: 0; list-style: none; }
.clean-list li { padding: .65rem .75rem; border: 1px solid var(--line-soft); border-radius: 10px; color: var(--text-soft); font-size: .78rem; overflow-wrap: anywhere; }
.empty-state { min-height: 90px; display: grid; place-items: center; margin-top: 1rem; border: 1px dashed var(--line); border-radius: var(--radius-sm); color: var(--muted); text-align: center; }
.chip-list { display: flex; flex-wrap: wrap; gap: .5rem; margin-top: .7rem; }
.chip { display: inline-flex; align-items: center; min-height: 34px; padding: .4rem .65rem; border: 1px solid var(--line); border-radius: 999px; background: var(--surface-soft); color: var(--text-soft); font-size: .76rem; }

.decision-bars { display: grid; gap: 1rem; margin: 1.5rem 0; }
.decision-row { display: grid; grid-template-columns: 110px 1fr 74px; align-items: center; gap: .75rem; }
.decision-row > span { color: var(--text-soft); font-size: .8rem; font-weight: 700; }
.decision-track { height: 12px; overflow: hidden; border-radius: 999px; background: var(--night-950); }
.decision-fill { width: 0; height: 100%; border-radius: inherit; background: var(--teal); transition: width .45s ease; }
.decision-fill.ask { background: var(--cyan); }
.decision-fill.none { background: #728892; }
.decision-row strong { text-align: right; font-size: .85rem; }
.mini-metrics { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: .6rem; }
.mini-metric { padding: .8rem; border: 1px solid var(--line-soft); border-radius: var(--radius-sm); background: rgba(7, 16, 23, .28); }
.mini-metric strong { display: block; margin-top: .2rem; }
.reason-list { display: grid; gap: .7rem; margin: 1.2rem 0 0; padding: 0; list-style: none; counter-reset: reason; }
.reason-list li { counter-increment: reason; display: grid; grid-template-columns: 30px 1fr auto; align-items: center; gap: .7rem; padding-bottom: .7rem; border-bottom: 1px solid var(--line-soft); }
.reason-list li::before { content: counter(reason, decimal-leading-zero); color: var(--muted); font-size: .72rem; }
.reason-list span { color: var(--text-soft); font-size: .8rem; overflow-wrap: anywhere; }
.reason-list strong { font-size: .82rem; }

.evidence-badge { max-width: 100%; border-color: #735b37; background: var(--amber-soft); color: #f5d49e; overflow-wrap: anywhere; }
.economics-panel { padding: 1.3rem; }
.provenance-line { display: flex; gap: .65rem; margin-bottom: 1rem; padding: .8rem .9rem; border-radius: var(--radius-sm); background: var(--blue-soft); color: var(--text-soft); font-size: .8rem; }
.economics-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: .75rem; }
.economics-metric { min-height: 112px; padding: .9rem; border: 1px solid var(--line-soft); border-radius: var(--radius-sm); }
.economics-metric span, .economics-metric small { display: block; color: var(--muted); font-size: .7rem; }
.economics-metric strong { display: block; margin: .45rem 0; font-size: 1.05rem; overflow-wrap: anywhere; }
.cert-tag { color: var(--teal) !important; }
.not-certifiable { color: var(--amber) !important; }
.exposure-row { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1rem; margin-top: 1rem; padding-top: 1rem; border-top: 1px solid var(--line-soft); }

.market-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: .8rem; }
.market-card { padding: 1rem; border: 1px solid var(--line-soft); border-radius: var(--radius-md); background: var(--surface); box-shadow: var(--shadow-2); }
.market-card h3 { display: flex; justify-content: space-between; align-items: center; }
.market-card h3 small { color: var(--muted); font-size: .67rem; font-weight: 600; }
.feed-list { display: grid; gap: .48rem; margin-top: .9rem; }
.feed-row { display: flex; justify-content: space-between; gap: .5rem; padding: .4rem 0; border-bottom: 1px solid var(--line-soft); color: var(--text-soft); font-size: .73rem; }
.feed-row:last-child { border: 0; }
.feed-state { display: inline-flex; align-items: center; gap: .35rem; font-weight: 700; }
.feed-state::before { content: ""; width: 7px; height: 7px; border-radius: 50%; background: var(--muted); }
.feed-state.fresh::before { background: var(--teal); }
.feed-state.stale::before { background: var(--amber); }
.feed-state.failed::before { background: var(--red); }

.details-grid { display: grid; gap: .75rem; }
.detail-card { border-radius: var(--radius-md); }
.detail-card summary { display: grid; grid-template-columns: 1fr auto; align-items: center; gap: 1rem; min-height: 74px; padding: 1rem 1.15rem; cursor: pointer; }
.detail-card summary span { font-weight: 800; }
.detail-card summary small { color: var(--muted); text-align: right; }
.detail-content, .identity-list { margin: 0; padding: 0 1.15rem 1.15rem; }
.variant-row, .gate-row, .report-row { display: grid; grid-template-columns: minmax(0, 1fr) auto auto; align-items: center; gap: .8rem; padding: .75rem 0; border-top: 1px solid var(--line-soft); }
.variant-row strong, .gate-row strong, .report-row strong { overflow-wrap: anywhere; }
.variant-row span, .gate-row span, .report-row span { color: var(--muted); font-size: .75rem; }
.identity-list div { display: grid; grid-template-columns: 210px minmax(0, 1fr); gap: 1rem; padding: .7rem 0; border-top: 1px solid var(--line-soft); }
.identity-list dt { color: var(--muted); font-size: .78rem; }
.identity-list dd { margin: 0; font-family: ui-monospace, "SFMono-Regular", Consolas, monospace; font-size: .74rem; overflow-wrap: anywhere; }
.pass { color: var(--teal) !important; }
.fail { color: var(--red) !important; }
.sealed { color: var(--amber) !important; }

.timeline { margin: 0; padding: 1.15rem; list-style: none; }
.timeline-item { position: relative; display: grid; grid-template-columns: 160px 1fr; gap: 1rem; min-height: 68px; padding: .7rem 0 .7rem 1.3rem; border-left: 1px solid var(--line); }
.timeline-item::before { content: ""; position: absolute; left: -5px; top: 1.2rem; width: 9px; height: 9px; border-radius: 50%; background: var(--cyan); box-shadow: 0 0 0 4px var(--surface); }
.timeline-item.warning::before { background: var(--amber); }
.timeline-item.danger::before { background: var(--red); }
.timeline time { color: var(--muted); font-size: .72rem; }
.timeline strong, .timeline span { display: block; }
.timeline span { margin-top: .18rem; color: var(--text-soft); font-size: .8rem; }
footer { margin-top: 4rem; padding-top: 1.5rem; border-top: 1px solid var(--line-soft); color: var(--muted); font-size: .78rem; }
footer p { margin: .35rem 0; }
.technical-links { margin-top: 1rem; }

@media (max-width: 1050px) {
  .metric-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .market-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .economics-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
}

@media (max-width: 760px) {
  :root { --space-6: 2.2rem; }
  .safety-ribbon { position: static; font-size: .67rem; letter-spacing: .08em; }
  .topbar { align-items: start; }
  .topbar-actions { flex-direction: column; align-items: stretch; }
  .connection-pill { display: none; }
  .fixture-control select { width: 180px; }
  .hero { min-height: 0; grid-template-columns: 1fr; gap: 1.5rem; }
  .hero::after { opacity: .55; }
  .hero-signal { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .hero-signal div { padding: .75rem; }
  .section-heading { align-items: start; }
  .phase-track { min-height: 124px; }
  .phase { padding: .7rem .55rem; }
  .phase span { font-size: .64rem; }
  .progress-foot { flex-direction: column; }
  .seal-note { text-align: left; }
  .safety-grid, .strategy-layout, .exposure-row { grid-template-columns: 1fr; }
  .economics-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}

@media (max-width: 520px) {
  main, .topbar, .fixture-banner { width: min(calc(100% - 1.1rem), var(--content)); }
  .topbar { flex-direction: column; }
  .topbar-actions, .fixture-control, .fixture-control select { width: 100%; max-width: none; }
  .hero { padding: 1.3rem; border-radius: var(--radius-md); }
  h1 { font-size: clamp(2.2rem, 14vw, 3.5rem); }
  .hero-meta { display: grid; gap: .35rem; }
  .hero-signal { grid-template-columns: 1fr; }
  .campaign-progress { padding: 1rem; border-radius: var(--radius-md); }
  .section-heading { display: grid; }
  .progress-readout { text-align: left; }
  .phase-track { min-height: 148px; }
  .phase strong { font-size: .76rem; }
  .phase span { font-size: .58rem; line-height: 1.35; }
  .metric-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .metric-card { min-height: 112px; }
  .clean-list, .mini-metrics, .economics-grid, .market-grid { grid-template-columns: 1fr; }
  .decision-row { grid-template-columns: 92px 1fr 54px; gap: .45rem; }
  .variant-row, .gate-row, .report-row { grid-template-columns: 1fr; gap: .25rem; }
  .detail-card summary { grid-template-columns: 1fr; gap: .2rem; }
  .detail-card summary small { text-align: left; }
  .identity-list div { grid-template-columns: 1fr; gap: .2rem; }
  .timeline-item { grid-template-columns: 1fr; gap: .2rem; }
  .evidence-badge { border-radius: var(--radius-sm); }
}

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { scroll-behavior: auto !important; transition-duration: .01ms !important; animation-duration: .01ms !important; animation-iteration-count: 1 !important; }
}
"""


H1_JS = r"""
(() => {
  "use strict";

  const fixtureNames = [
    "PREPARED_NOT_STARTED", "ARMED", "RUNNING_HEALTHY", "STALE_RECONNECTING",
    "INTERRUPTED_RECOVERABLE", "INTEGRITY_FAILED", "COMPLETE_COLLECTION_WINDOW",
    "HOLDOUT_SEALED", "HOLDOUT_OPEN"
  ];
  const stateCopy = {
    ABSENT: ["Campagne non reliée", "La racine configurée n’existe pas encore. Le cockpit reste prêt en lecture seule.", "warning"],
    PREPARED_NOT_STARTED: ["Campagne préparée, pas encore démarrée", "Les identités sont figées. Aucune frame prospective n’a encore été publiée.", "calm"],
    ARMED: ["Campagne armée et prête", "La fenêtre de départ approche. Le collecteur n’a pas encore commencé.", "calm"],
    RUNNING_HEALTHY: ["La campagne tourne normalement", "Les publications récentes sont fraîches et l’intégrité du tail est authentifiée.", "calm"],
    STALE: ["Les données ne se rafraîchissent plus", "La dernière publication dépasse le délai attendu. Vérifiez le collecteur sans modifier la campagne.", "warning"],
    RECONNECTING: ["Reconnexion en cours", "La collecte reste fail-closed jusqu’à une nouvelle génération de données fraîches.", "warning"],
    INTERRUPTED_RECOVERABLE: ["Campagne interrompue mais reprenable", "Le tail publié reste authentifié. La reprise appartient au processus opératoire séparé.", "warning"],
    COMPLETE_COLLECTION_WINDOW: ["Fenêtre de collecte terminée", "La campagne a atteint sa borne finale. Les preuves publiées restent consultées en lecture seule.", "calm"],
    COMPLETE_VERIFIED_THRESHOLDS: ["Collecte terminée avec seuils vérifiés", "Le rapport final est lié au head authentifié, sans autoriser de trading réel.", "calm"],
    INTEGRITY_FAILED: ["Échec d’intégrité — lecture bloquée", "Le cockpit masque les données non fiables et n’expose aucune métrique dérivée.", "danger"],
    UNREADABLE_FAIL_CLOSED: ["Campagne illisible — lecture bloquée", "La publication ne peut pas être lue en sécurité. Aucune valeur n’est supposée.", "danger"],
    HEAD_CHANGED_RETRY: ["Publication en cours de changement", "Deux lectures cohérentes n’ont pas pu être assemblées. Le cockpit réessaiera automatiquement.", "warning"]
  };
  const metricLabels = {
    fees: ["Frais", "Coût maker/taker publié"], funding: ["Funding", "Règlements réellement certifiés"],
    slippage: ["Slippage de closeout", "Écart d’exécution p99, en bps"], spread: ["Capture de spread", "Composante attribuée au spread"],
    adverse_selection: ["Sélection adverse", "Mouvement défavorable après fill"], opportunity_cost: ["Coût d’opportunité", "Valeur des occasions non exécutées"],
    realized_pnl: ["PnL réalisé", "Résultat des positions clôturées"], unrealized_pnl: ["PnL non réalisé", "Résultat encore ouvert"],
    net_pnl: ["PnL net", "Après coûts et attribution"], drawdown: ["Drawdown", "Repli maximal du résultat"],
    turnover_notional: ["Turnover notionnel", "Volume notionnel rempli"], gross_exposure: ["Exposition brute", "Positions publiées"],
    concentration_top_one_percent: ["Concentration top 1 %", "Part des meilleurs fills dans le gain positif"]
  };
  const feedLabels = { metadata: "Métadonnées", bbo: "Meilleur bid/ask", l2_book: "Carnet L2", trades: "Trades", all_mids: "Prix médians", active_asset_context: "Contexte marché" };
  const $ = (id) => document.getElementById(id);
  const clear = (node) => { while (node.firstChild) node.removeChild(node.firstChild); };
  const node = (tag, className, text) => {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text !== undefined && text !== null) element.textContent = String(text);
    return element;
  };
  const available = (value) => value !== null && value !== undefined && value !== "";
  const display = (value) => available(value) ? String(value) : "NON DISPONIBLE";
  const number = (value) => typeof value === "number" ? new Intl.NumberFormat("fr-FR").format(value) : display(value);
  const bytes = (value) => {
    if (typeof value !== "number") return "NON DISPONIBLE";
    const units = ["o", "Kio", "Mio", "Gio", "Tio"];
    let amount = value;
    let unit = 0;
    while (amount >= 1024 && unit < units.length - 1) { amount /= 1024; unit += 1; }
    return `${new Intl.NumberFormat("fr-FR", { maximumFractionDigits: unit ? 1 : 0 }).format(amount)} ${units[unit]}`;
  };
  const dateText = (value) => {
    if (!available(value) || value === "SYNTHETIC") return display(value);
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? display(value) : new Intl.DateTimeFormat("fr-FR", { dateStyle: "medium", timeStyle: "medium", timeZone: "UTC" }).format(parsed) + " UTC";
  };
  const duration = (seconds) => {
    if (typeof seconds !== "number") return "NON DISPONIBLE";
    const days = Math.floor(seconds / 86400);
    const hours = Math.floor((seconds % 86400) / 3600);
    return days > 0 ? `${days} j ${hours} h` : `${hours} h`;
  };
  const toneFor = (code) => (stateCopy[code] || ["État publié", "Consultez les détails d’intégrité.", "warning"])[2];

  function setupFixtureControl() {
    const select = $("fixture-select");
    const real = node("option", "", "Source configurée / fixture par défaut");
    real.value = "";
    select.appendChild(real);
    fixtureNames.forEach((name) => {
      const option = node("option", "", name.replaceAll("_", " "));
      option.value = name;
      select.appendChild(option);
    });
    const requested = new URLSearchParams(window.location.search).get("fixture") || "";
    select.value = fixtureNames.includes(requested.toUpperCase()) ? requested.toUpperCase() : "";
    select.addEventListener("change", () => {
      const url = new URL(window.location.href);
      if (select.value) url.searchParams.set("fixture", select.value);
      else url.searchParams.delete("fixture");
      window.history.replaceState({}, "", url);
      refresh(true);
    });
  }

  function renderHero(snapshot) {
    const state = snapshot.state || {};
    const code = state.code || "UNREADABLE_FAIL_CLOSED";
    const copy = stateCopy[code] || [code.replaceAll("_", " "), "État technique publié par la campagne.", "warning"];
    document.body.dataset.tone = copy[2];
    $("status-kicker").textContent = code.replaceAll("_", " ");
    $("campaign-heading").textContent = copy[0];
    $("hero-summary").textContent = copy[1];
    $("campaign-id").textContent = `Campagne · ${display((snapshot.identity || {}).campaign_id)}`;
    $("last-update").textContent = `Dernière mise à jour · ${dateText(state.last_updated_at_utc)}`;
    $("hero-collection").textContent = ["RUNNING_HEALTHY", "STALE", "RECONNECTING"].includes(code) ? "EN COURS" : code.includes("COMPLETE") ? "TERMINÉE" : code.includes("FAILED") || code.includes("UNREADABLE") ? "BLOQUÉE" : "EN ATTENTE";
    $("hero-freshness").textContent = display(state.freshness);
    $("hero-integrity").textContent = display(state.integrity);
    $("fixture-banner").hidden = snapshot.fixture !== true;
  }

  function renderProgress(snapshot) {
    const progress = snapshot.progress || {};
    const holdout = progress.holdout || {};
    const percent = typeof progress.progress_percent === "number" ? Math.max(0, Math.min(100, progress.progress_percent)) : 0;
    $("progress-percent").textContent = typeof progress.progress_percent === "number" ? `${percent} %` : "NON DISPONIBLE";
    $("phase-label").textContent = `Phase · ${display(progress.phase).replaceAll("_", " ")}`;
    $("progress-fill").style.width = `${percent}%`;
    const open = holdout.access === "OPEN";
    $("holdout-phase").classList.toggle("is-open", open);
    $("holdout-label").textContent = open ? "J10–J14 · OUVERT CANONIQUEMENT" : "J10–J14 · SEALED";
    $("seal-note").textContent = open ? "Le holdout est ouvert par l’état terminal canonique." : `Holdout scellé · ${duration(holdout.remaining_seconds)} restantes · aucune métrique exposée.`;
    $("progress-time").textContent = available(progress.starts_at_utc) ? `${dateText(progress.starts_at_utc)} → ${dateText(progress.ends_at_utc)}` : "Dates non disponibles";
  }

  function metricCard(label, value, detail) {
    const card = node("article", "metric-card");
    card.append(node("span", "", label), node("strong", "", value), node("small", "", detail));
    return card;
  }

  function renderCollection(snapshot) {
    const collection = snapshot.collection || {};
    const grid = $("collection-metrics");
    clear(grid);
    [
      ["Frames", number(collection.frames), "Observations publiées"],
      ["Segments", number(collection.segments), "Blocs raw authentifiés"],
      ["Volume", bytes(collection.stored_bytes), "Octets de segments"],
      ["Gaps", number(collection.gaps), "Discontinuités visibles"],
      ["Doublons", number(collection.duplicates), collection.duplicates_scope || "Portée non disponible"],
      ["Reconnexions", number(collection.reconnects), `Génération ${display(collection.connection_generation)}`]
    ].forEach((item) => grid.appendChild(metricCard(item[0], item[1], item[2])));

    const safety = snapshot.safety || {};
    const stale = Array.isArray(safety.stale_feeds) ? safety.stale_feeds : [];
    const alert = $("priority-alert");
    const tone = toneFor((snapshot.state || {}).code);
    alert.className = `priority-alert ${tone === "calm" ? "" : tone}`;
    $("alert-title").textContent = tone === "danger" ? "Lecture fail-closed" : stale.length ? `${stale.length} flux demande de l’attention` : "Aucune alerte prioritaire publiée";
    $("alert-detail").textContent = tone === "danger" ? "Les métriques non fiables sont masquées; aucun zéro n’est inventé." : stale.length ? stale.join(" · ") : "Les contrôles visibles ne signalent ni flux stale ni échec d’intégrité.";
    $("stale-count").textContent = `${stale.length} signalé${stale.length > 1 ? "s" : ""}`;
    const staleBox = $("stale-feeds");
    clear(staleBox);
    staleBox.className = stale.length ? "chip-list" : "empty-state";
    if (stale.length) stale.forEach((entry) => staleBox.appendChild(node("span", "chip sealed", entry)));
    else staleBox.textContent = "Aucun flux stale publié.";
    const kills = $("kill-rules");
    clear(kills);
    const rules = Array.isArray(safety.kill_rules) ? safety.kill_rules : [];
    if (rules.length) rules.slice(0, 8).forEach((rule) => kills.appendChild(node("li", "", String(rule).replaceAll("_", " "))));
    else kills.appendChild(node("li", "", "NON DISPONIBLE"));
  }

  function renderStrategy(snapshot) {
    const strategy = snapshot.strategy || {};
    const decisions = strategy.decisions || {};
    const total = ["BID_ONLY", "ASK_ONLY", "NO_QUOTE"].reduce((sum, key) => sum + (typeof decisions[key] === "number" ? decisions[key] : 0), 0);
    const bars = $("decision-bars");
    clear(bars);
    [["BID_ONLY", "Cotation acheteuse", ""], ["ASK_ONLY", "Cotation vendeuse", "ask"], ["NO_QUOTE", "Aucune cotation", "none"]].forEach(([key, label, kind]) => {
      const count = typeof decisions[key] === "number" ? decisions[key] : null;
      const row = node("div", "decision-row");
      const track = node("div", "decision-track");
      const fill = node("div", `decision-fill ${kind}`);
      fill.style.width = total && count !== null ? `${Math.max(2, count / total * 100)}%` : "0";
      track.appendChild(fill);
      row.append(node("span", "", label), track, node("strong", "", number(count)));
      bars.appendChild(row);
    });
    const mini = $("ghost-metrics");
    clear(mini);
    [["Intentions Ghost", strategy.intentions], ["Fills hypothétiques", strategy.fills], ["Fills partiels", strategy.partial_fills], ["Fills manqués", strategy.missed_fills]].forEach(([label, value]) => {
      const item = node("div", "mini-metric");
      item.append(node("span", "", label), node("strong", "", number(value)));
      mini.appendChild(item);
    });
    const reasons = $("no-quote-reasons");
    clear(reasons);
    const rows = Array.isArray(strategy.no_quote_reasons) ? strategy.no_quote_reasons : [];
    if (!rows.length) reasons.appendChild(node("li", "", "Aucune raison publiée — NON DISPONIBLE"));
    rows.slice(0, 8).forEach((entry) => {
      const item = node("li");
      item.append(node("span", "", display(entry.reason).replaceAll("_", " ")), node("strong", "", number(entry.count)));
      reasons.appendChild(item);
    });
  }

  function renderEconomics(snapshot) {
    const economics = snapshot.economics || {};
    $("evidence-status").textContent = snapshot.economic_evidence_status || "ECONOMIC_EVIDENCE_NOT_YET_AVAILABLE";
    $("economics-provenance").textContent = economics.certifiable === true ? `Provenance : ${display(economics.provenance)}. Chaque valeur est liée au rapport final authentifié.` : `Provenance : ${display(economics.provenance)}. Les données non certifiables ne constituent aucune preuve économique.`;
    const grid = $("economics-metrics");
    clear(grid);
    const metrics = economics.metrics || {};
    Object.entries(metricLabels).forEach(([key, labels]) => {
      const metric = metrics[key] || {};
      const card = node("article", "economics-metric");
      card.append(node("span", "", labels[0]), node("strong", "", display(metric.value)), node("small", "", labels[1]), node("small", metric.certifiable ? "cert-tag" : "not-certifiable", metric.certifiable ? "CERTIFIABLE" : "NON CERTIFIABLE"));
      grid.appendChild(card);
    });
    renderMapChips($("inventory"), (snapshot.strategy || {}).inventory, "Aucun inventaire publié");
    renderMapChips($("closeouts"), (snapshot.strategy || {}).unresolved_closeouts, "Aucun closeout non résolu publié");
    const gates = $("economic-gates");
    clear(gates);
    const gateRows = Array.isArray(economics.gates) ? economics.gates : [];
    if (!gateRows.length) gates.appendChild(node("div", "empty-state", "Gates NON DISPONIBLES avant un rapport final authentifié."));
    gateRows.forEach((entry) => {
      const row = node("div", "gate-row");
      row.append(node("strong", "", display(entry.gate).replaceAll("_", " ")), node("span", "", "Évalué sur le hurdle principal 500 ms"), node("span", entry.passed ? "pass" : "fail", entry.passed ? "PASS" : "FAIL"));
      gates.appendChild(row);
    });
  }

  function renderMapChips(container, value, emptyText) {
    clear(container);
    const entries = value && typeof value === "object" && !Array.isArray(value) ? Object.entries(value) : [];
    if (!entries.length) container.appendChild(node("span", "chip", emptyText));
    entries.forEach(([key, amount]) => container.appendChild(node("span", "chip", `${key} · ${display(amount)}`)));
  }

  function renderMarkets(snapshot) {
    const rows = Array.isArray(snapshot.feeds) ? snapshot.feeds : [];
    const grid = $("market-grid");
    clear(grid);
    ["BTC", "ETH", "SOL", "HYPE"].forEach((market) => {
      const card = node("article", "market-card");
      const title = node("h3");
      const marketRows = rows.filter((row) => row.market === market);
      const stale = marketRows.filter((row) => row.status === "STALE").length;
      title.append(node("span", "", market), node("small", stale ? `${stale} à surveiller` : "flux publics"));
      const list = node("div", "feed-list");
      if (!marketRows.length) list.appendChild(node("div", "empty-state", "NON DISPONIBLE"));
      marketRows.forEach((entry) => {
        const row = node("div", "feed-row");
        const status = display(entry.status);
        const statusClass = status === "FRESH" ? "fresh" : status === "STALE" ? "stale" : status.includes("FAIL") ? "failed" : "";
        row.append(node("span", "", feedLabels[entry.feed] || display(entry.feed)), node("span", `feed-state ${statusClass}`, status === "FRESH" && typeof entry.age_seconds === "number" ? `${entry.age_seconds} s` : status));
        list.appendChild(row);
      });
      card.append(title, list);
      grid.appendChild(card);
    });
  }

  function renderAdvanced(snapshot) {
    const variants = $("variants");
    clear(variants);
    const rows = Array.isArray(snapshot.variants) ? snapshot.variants : [];
    if (!rows.length) variants.appendChild(node("div", "empty-state", "Variantes NON DISPONIBLES"));
    rows.forEach((entry) => {
      const row = node("div", "variant-row");
      row.append(node("strong", "", display(entry.variant_id)), node("span", "", `${display(entry.role)} · ${display(entry.status).replaceAll("_", " ")}`), node("span", entry.holdout_access === "OPEN" ? "pass" : "sealed", display(entry.holdout_access)));
      variants.appendChild(row);
    });
    const identities = $("identities");
    clear(identities);
    const labels = { campaign_id: "Campaign ID", policy_id: "Politique", policy_config_sha256: "Hash configuration", campaign_manifest_sha256: "Hash manifest campagne", raw_manifest_sha256: "Hash manifest raw", raw_root_sha256: "Racine raw", fee_artifact_sha256: "Hash frais publics", source_commit: "Commit source" };
    const values = snapshot.identity || {};
    Object.entries(labels).forEach(([key, label]) => {
      const wrapper = node("div");
      wrapper.append(node("dt", "", label), node("dd", "", display(values[key])));
      identities.appendChild(wrapper);
    });
    const reports = $("reports");
    clear(reports);
    const reportRows = Array.isArray(snapshot.reports) ? snapshot.reports : [];
    if (!reportRows.length) reports.appendChild(node("div", "empty-state", "Aucun téléchargement autorisé dans cet état."));
    reportRows.forEach((entry) => {
      const row = node("div", "report-row");
      const action = entry.download_url ? node("a", "", "Télécharger") : node("span", "sealed", "INDISPONIBLE");
      if (entry.download_url) action.href = entry.download_url;
      row.append(node("strong", "", display(entry.title)), node("span", "", display(entry.sha256)), action);
      reports.appendChild(row);
    });
  }

  function renderTimeline(snapshot) {
    const timeline = $("timeline");
    clear(timeline);
    const incidents = Array.isArray(snapshot.incidents) ? snapshot.incidents : [];
    if (!incidents.length) {
      const item = node("li", "timeline-item");
      item.append(node("time", "", "État courant"), node("div", "", "Aucun incident récent publié dans la fenêtre bornée."));
      timeline.appendChild(item);
      return;
    }
    incidents.slice(-40).reverse().forEach((entry) => {
      const severity = entry.severity === "danger" ? "danger" : entry.severity === "warning" ? "warning" : "";
      const item = node("li", `timeline-item ${severity}`);
      const detail = node("div");
      detail.append(node("strong", "", display(entry.code).replaceAll("_", " ")), node("span", "", display(entry.detail)));
      item.append(node("time", "", dateText(entry.at_utc)), detail);
      timeline.appendChild(item);
    });
  }

  function render(snapshot) {
    renderHero(snapshot);
    renderProgress(snapshot);
    renderCollection(snapshot);
    renderStrategy(snapshot);
    renderEconomics(snapshot);
    renderMarkets(snapshot);
    renderAdvanced(snapshot);
    renderTimeline(snapshot);
    $("live-region").textContent = `Dashboard actualisé : ${display((snapshot.state || {}).code)}`;
  }

  let timer = null;
  let failures = 0;
  async function refresh(immediate = false) {
    if (timer) window.clearTimeout(timer);
    const fixture = $("fixture-select").value;
    const endpoint = fixture ? `/api/h1/fixtures/${encodeURIComponent(fixture)}` : "/api/h1/snapshot";
    try {
      const response = await fetch(endpoint, { headers: { Accept: "application/json" }, cache: "no-store" });
      const payload = await response.json();
      render(payload);
      failures = response.ok ? 0 : failures + 1;
      $("connection-pill").classList.toggle("is-offline", !response.ok);
      $("connection-pill").lastChild.textContent = response.ok ? "Connexion locale" : "Lecture fail-closed";
    } catch (_error) {
      failures += 1;
      $("connection-pill").classList.add("is-offline");
      $("connection-pill").lastChild.textContent = "Hors ligne — nouvelle tentative";
      $("priority-alert").className = "priority-alert warning";
      $("alert-title").textContent = "Le dashboard est hors ligne";
      $("alert-detail").textContent = "La dernière vue reste affichée. Une nouvelle lecture locale sera tentée avec backoff.";
    }
    const delay = immediate ? 10000 : Math.min(60000, 10000 * (2 ** Math.min(failures, 3)));
    timer = window.setTimeout(() => refresh(false), delay);
  }

  setupFixtureControl();
  refresh(true);
})();
"""


__all__ = ["H1_CSS", "H1_JS", "H1_PAGE"]
