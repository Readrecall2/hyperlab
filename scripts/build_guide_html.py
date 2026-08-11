from __future__ import annotations

import argparse
import html
import re
import unicodedata
from pathlib import Path

import mistune


class GuideRenderer(mistune.HTMLRenderer):
    def __init__(self) -> None:
        super().__init__(escape=False)
        self.headings: list[tuple[int, str, str]] = []
        self._seen: dict[str, int] = {}

    @staticmethod
    def _plain_text(value: str) -> str:
        value = re.sub(r"<[^>]+>", "", value)
        return html.unescape(value).strip()

    def _slug(self, value: str) -> str:
        plain = self._plain_text(value)
        normalized = unicodedata.normalize("NFKD", plain)
        normalized = "".join(c for c in normalized if not unicodedata.combining(c))
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", normalized.lower()).strip("-") or "section"
        count = self._seen.get(slug, 0)
        self._seen[slug] = count + 1
        return slug if count == 0 else f"{slug}-{count + 1}"

    def heading(self, text: str, level: int, **attrs: object) -> str:
        slug = self._slug(text)
        self.headings.append((level, slug, self._plain_text(text)))
        return f'<h{level} id="{slug}">{text}<a class="heading-link" href="#{slug}" aria-label="Lien vers cette section">#</a></h{level}>\n'


def render_guide(markdown_path: Path, output_path: Path) -> None:
    renderer = GuideRenderer()
    md = mistune.create_markdown(
        renderer=renderer,
        plugins=["table", "strikethrough", "task_lists", "url"],
    )
    body = md(markdown_path.read_text(encoding="utf-8"))

    nav_items: list[str] = []
    for level, slug, title in renderer.headings:
        if level == 1 and title.startswith("HyperLab"):
            continue
        nav_level = min(max(level, 1), 3)
        nav_items.append(
            f'<a class="l{nav_level}" href="#{slug}">{html.escape(title)}</a>'
        )
    nav = "".join(nav_items)

    document = f'''<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="dark light">
<title>HyperLab — Guide complet Windows 11 + Umbrel + Codex</title>
<style>
:root{{--bg:#07101d;--paper:#0e1b2d;--paper2:#12243c;--text:#edf5ff;--muted:#9fb2ca;--line:#29445f;--accent:#60dfbf;--accent2:#86a9ff;--warn:#ffd070;--danger:#ff8f9d;--code:#06101e}}
*{{box-sizing:border-box;scroll-behavior:smooth}}
html{{scroll-padding-top:24px}}
body{{margin:0;background:radial-gradient(circle at 25% 0,#1c4775 0,#07101d 35%,#050b14 100%);color:var(--text);font-family:Inter,"Segoe UI",system-ui,-apple-system,sans-serif;line-height:1.66}}
a{{color:#8cc8ff;text-decoration:none}}a:hover{{text-decoration:underline}}
.layout{{display:grid;grid-template-columns:310px minmax(0,1fr);max-width:1520px;margin:auto}}
aside{{position:sticky;top:0;height:100vh;overflow:auto;padding:24px 18px;border-right:1px solid var(--line);background:rgba(5,12,22,.84);backdrop-filter:blur(18px)}}
.brand{{padding:18px;border:1px solid var(--line);border-radius:18px;background:linear-gradient(145deg,#143154,#0b1728);margin-bottom:18px;box-shadow:0 20px 50px #0003}}
.brand strong{{font-size:1.38rem;letter-spacing:-.03em}}.brand span{{display:block;color:var(--muted);font-size:.82rem;margin-top:4px}}
.brand .pill{{display:inline-block;margin-top:12px;padding:4px 9px;border-radius:999px;border:1px solid #60dfbf66;background:#60dfbf14;color:#aaf4df;font-size:.72rem;font-weight:800;letter-spacing:.04em}}
nav a{{display:block;padding:7px 9px;border-radius:9px;color:var(--muted);font-size:.85rem;line-height:1.3}}nav a:hover{{background:#ffffff0b;color:var(--text);text-decoration:none}}nav .l1{{color:var(--accent);font-weight:800;margin-top:10px}}nav .l2{{padding-left:16px}}nav .l3{{padding-left:28px;font-size:.79rem}}
main{{min-width:0;padding:56px clamp(24px,5vw,78px) 100px}}article{{max-width:990px;margin:auto}}
article>h1:first-of-type{{font-size:clamp(2.7rem,7vw,6.4rem);line-height:.98;letter-spacing:-.065em;margin:0 0 20px;background:linear-gradient(90deg,#fff,#9fe8da 55%,#a9bfff);-webkit-background-clip:text;color:transparent}}
h1{{font-size:2.35rem;letter-spacing:-.04em;margin:72px 0 22px;padding-top:10px}}h2{{font-size:1.65rem;letter-spacing:-.025em;margin:46px 0 16px}}h3{{font-size:1.22rem;margin:30px 0 10px;color:#ddebff}}
.heading-link{{opacity:0;margin-left:.45rem;font-size:.68em;color:var(--accent)}}h1:hover .heading-link,h2:hover .heading-link,h3:hover .heading-link{{opacity:.75;text-decoration:none}}
p,li{{color:#d8e4f3}}strong{{color:#fff}}em{{color:#c9d9ec}}
article>p:first-of-type{{font-size:1.07rem;color:var(--muted);padding:12px 0 4px}}
blockquote{{margin:24px 0;padding:16px 20px;border-left:4px solid var(--warn);background:#ffd07010;border-radius:0 12px 12px 0;color:#f9e7b2}}
hr{{border:0;border-top:1px solid var(--line);margin:58px 0}}
pre{{position:relative;overflow:auto;background:var(--code);border:1px solid #203b58;border-radius:15px;padding:18px 20px;box-shadow:inset 0 1px 0 #ffffff08}}code{{font-family:"Cascadia Code",Consolas,ui-monospace,monospace;font-size:.9em}}p code,li code{{background:#152a43;border:1px solid #294867;padding:2px 6px;border-radius:6px;color:#dff8f2}}
table{{border-collapse:separate;border-spacing:0;width:100%;display:block;overflow:auto;margin:24px 0;border:1px solid var(--line);border-radius:14px;background:#0c192a}}thead{{position:sticky;top:0}}th,td{{padding:12px 14px;border-bottom:1px solid var(--line);border-right:1px solid var(--line);text-align:left;white-space:nowrap}}th{{background:#142b47;color:#fff}}td{{color:#d8e4f3}}tr:last-child td{{border-bottom:0}}th:last-child,td:last-child{{border-right:0}}
ul,ol{{padding-left:24px}}li{{margin:5px 0}}
.backtop{{position:fixed;right:22px;bottom:22px;width:46px;height:46px;display:grid;place-items:center;border-radius:50%;background:#173552;border:1px solid #3e6487;color:white;box-shadow:0 10px 30px #0008;text-decoration:none}}
.copy-button{{position:absolute;right:10px;top:10px;border:1px solid #35516d;background:#10243a;color:#cfe4fb;border-radius:8px;padding:5px 9px;font-size:.72rem;cursor:pointer;opacity:.82}}.copy-button:hover{{opacity:1}}
footer{{margin-top:80px;padding-top:24px;border-top:1px solid var(--line);color:var(--muted);font-size:.85rem}}
@media(max-width:1050px){{.layout{{display:block}}aside{{position:relative;height:auto;border-right:0;border-bottom:1px solid var(--line)}}nav{{columns:2}}main{{padding-top:38px}}}}
@media(max-width:650px){{aside{{display:none}}main{{padding:32px 18px 70px}}article>h1:first-of-type{{font-size:3rem}}h1{{font-size:1.9rem}}nav{{columns:1}}.heading-link{{display:none}}}}
@media print{{aside,.backtop,.copy-button{{display:none}}.layout{{display:block}}body{{background:white;color:#111}}main{{padding:0}}article{{max-width:none}}p,li,td{{color:#222}}h1,h2,h3,strong{{color:#000}}pre{{white-space:pre-wrap;background:#f5f5f5;color:#111}}a{{color:#111}}}}
</style>
</head>
<body id="top">
<div class="layout">
<aside>
<div class="brand"><strong>HyperLab</strong><span>Guide complet · v0.2.0 · 11 août 2026</span><span class="pill">READ-ONLY PAR DÉFAUT</span></div>
<nav>{nav}</nav>
</aside>
<main><article>{body}<footer>HyperLab 0.2.0 · Laboratoire de recherche multi-stratégies · aucune promesse de rendement.</footer></article></main>
</div>
<a class="backtop" href="#top" aria-label="Retour en haut">↑</a>
<script>
document.querySelectorAll('pre').forEach((pre) => {{
  const button = document.createElement('button');
  button.className = 'copy-button';
  button.type = 'button';
  button.textContent = 'Copier';
  button.addEventListener('click', async () => {{
    const code = pre.querySelector('code');
    try {{
      await navigator.clipboard.writeText(code ? code.innerText : pre.innerText);
      button.textContent = 'Copié';
    }} catch (_) {{
      button.textContent = 'Sélectionner';
    }}
    setTimeout(() => button.textContent = 'Copier', 1500);
  }});
  pre.appendChild(button);
}});
</script>
</body>
</html>'''

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(document, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Construire le guide HTML autonome HyperLab.")
    parser.add_argument("markdown", nargs="?", default="docs/GUIDE_COMPLET_FR.md")
    parser.add_argument("output", nargs="?", default="docs/GUIDE_COMPLET_FR.html")
    args = parser.parse_args()
    render_guide(Path(args.markdown), Path(args.output))
    print(Path(args.output).resolve())


if __name__ == "__main__":
    main()
