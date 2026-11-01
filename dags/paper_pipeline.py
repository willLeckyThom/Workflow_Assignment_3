"""
Configuration comes from your .env file via docker-compose.yml:
    LIBRARY_PATH         (.env, Windows path) is bind-mounted into the container at /library,
                         so inside Airflow the library is always /library
    LLM_MODEL            (.env) model name as known to Ollama, default llama3.1
    DISCORD_WEBHOOK_URL  (.env) webhook URL; notifications are skipped if empty

Set by docker-compose.yml itself (override only if you know why):
    RESEARCH_LIBRARY     default /library
    PAPER_DB             default /data/papers.db  (a Docker volume, outside the synced folder)
    LLM_BASE_URL         default http://ollama:11434/v1
"""
import html
import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


def env(name: str, default: str = "") -> str:
    """Read a setting from the environment, tolerating blanks and stray quotes.

    Args:
        name: Name of the environment variable to read.
        default: Value to use when the variable is unset or empty.

    Returns:
        The cleaned value as a string, or ``default`` if nothing usable was set.
    """
    return (os.environ.get(name) or default).strip().strip('"').strip("'")


# ---------- configuration (read once when Airflow parses this file) ----------
LIBRARY = Path(env("RESEARCH_LIBRARY", "/library"))   # Drive-synced folder as seen in the container
PAPERS_DIR = LIBRARY / "papers"                       # input PDFs
OUTPUT_DIR = LIBRARY / "output"                       # generated index.html
DB_PATH = Path(env("PAPER_DB", "/data/papers.db"))    # SQLite file, NOT in the synced folder

LLM_BASE_URL = env("LLM_BASE_URL", "http://ollama:11434/v1")
LLM_MODEL = env("LLM_MODEL", "llama3.1")
DISCORD_WEBHOOK_URL = env("DISCORD_WEBHOOK_URL")

MAX_MARKDOWN_CHARS = 5000

PROMPT = """You are a research librarian. Read the paper below and reply with ONLY a JSON object:
{{
  "title": "Paper title",
  "summary": "A short summary of the paper (2-4 sentences).",
  "keywords": ["keyword one", "keyword two"],
  "category": "One broad category, e.g. Data Engineering, Machine Learning, Databases, Systems, Security"
}}

PAPER (Markdown, may be truncated):
{markdown}
"""


# ---------- helpers ----------
def get_db() -> sqlite3.Connection:
    """Open the SQLite database, creating the folder and table if needed.

    Args:
        None. The location comes from the module-level ``DB_PATH``.

    Returns:
        An open ``sqlite3.Connection``. The caller is responsible for closing it.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS papers (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               filename TEXT UNIQUE NOT NULL,
               title TEXT,
               summary TEXT,
               keywords TEXT,   -- JSON string
               category TEXT,
               processed_at TEXT DEFAULT CURRENT_TIMESTAMP
           )"""
    )
    return conn


def validate(meta: dict) -> dict:
    """Check that the LLM's parsed JSON has the required fields and basic types.

    Description:
        Deliberately light validation: ``title``, ``summary`` and ``category``
        must be non-empty strings, and ``keywords`` must be a list of strings.

    Args:
        meta: The dictionary produced by ``json.loads`` on the model's reply.

    Returns:
        The same ``meta`` dictionary, unchanged, if it is valid.

    Raises:
        ValueError: If ``meta`` is not a dict, or a required field is missing
            or has the wrong type.
    """
    if not isinstance(meta, dict):
        raise ValueError("LLM output is not a JSON object")
    for key in ("title", "summary", "category"):
        if not isinstance(meta.get(key), str) or not meta[key].strip():
            raise ValueError(f"Missing or invalid field: {key}")
    kws = meta.get("keywords")
    if not isinstance(kws, list) or not all(isinstance(k, str) for k in kws):
        raise ValueError("Missing or invalid field: keywords")
    return meta


def ask_llm(markdown: str) -> dict:
    """Ask the local LLM for paper metadata and return it as a validated dict.

    Args:
        markdown: The paper converted to Markdown. Only the first
            ``MAX_MARKDOWN_CHARS`` characters are sent to the model.

    Returns:
        A dict with keys ``title`` (str), ``summary`` (str),
        ``keywords`` (list[str]) and ``category`` (str).

    Raises:
        RuntimeError: If both attempts returned invalid JSON or failed validation.
    """
    from openai import OpenAI  # imported here so DAG parsing stays fast

    client = OpenAI(base_url=LLM_BASE_URL, api_key="local-not-needed")
    prompt = PROMPT.format(markdown=markdown[:MAX_MARKDOWN_CHARS])
    last_err = None
    try:
        resp = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        return validate(json.loads(resp.choices[0].message.content))
    except (json.JSONDecodeError, ValueError) as err:
        last_err = err
        print(f"Failed to parse LLM output: {err}")
    raise RuntimeError(f"LLM did not return valid JSON after retry: {last_err}")


def build_html() -> int:
    """Regenerate the static ``index.html`` from everything in the database.

    Args:
        None. Reads ``DB_PATH`` and writes to ``OUTPUT_DIR``.

    Returns:
        The number of papers included in the page.
    """
    conn = get_db()
    rows = conn.execute(
        "SELECT filename, title, summary, keywords, category FROM papers ORDER BY processed_at DESC"
    ).fetchall()
    conn.close()

    papers = [
        {
            "filename": r[0],
            "title": r[1],
            "summary": r[2],
            "keywords": json.loads(r[3] or "[]"),
            "category": r[4],
            "href": f"../papers/{r[0]}",
        }
        for r in rows
    ]
    data = json.dumps(papers)

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Research Library</title>
<style>
  body {{ font-family: system-ui, sans-serif; max-width: 860px; margin: 2rem auto; padding: 0 1rem; }}
  .controls {{ display: flex; gap: .5rem; margin-bottom: 1rem; }}
  input, select {{ padding: .5rem; font-size: 1rem; }}
  input {{ flex: 1; }}
  .paper {{ border: 1px solid #8884; border-radius: 8px; padding: 1rem; margin-bottom: .75rem; }}
  .paper h2 {{ margin: 0 0 .25rem; font-size: 1.1rem; }}
  .tag {{ display: inline-block; font-size: .8rem; background: #8882; border-radius: 4px; padding: 0 .4rem; margin-right: .25rem; }}
  .cat {{ font-size: .85rem; opacity: .7; }}
</style>
</head>
<body>
<h1>Research Library</h1>
<p>Last updated: {html.escape(datetime.now().strftime("%Y-%m-%d %H:%M"))} &middot; <span id="count"></span></p>
<div class="controls">
  <input id="q" type="search" placeholder="Search title, summary, keywords...">
  <select id="cat"><option value="">All categories</option></select>
</div>
<div id="list"></div>
<script>
const papers = {data};
const q = document.getElementById('q');
const cat = document.getElementById('cat');
const list = document.getElementById('list');
const count = document.getElementById('count');
const esc = s => String(s).replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[c]));

[...new Set(papers.map(p => p.category))].sort().forEach(c => {{
  const o = document.createElement('option'); o.value = c; o.textContent = c; cat.appendChild(o);
}});

function render() {{
  const term = q.value.toLowerCase();
  const shown = papers.filter(p =>
    (!cat.value || p.category === cat.value) &&
    (p.title + ' ' + p.summary + ' ' + p.keywords.join(' ')).toLowerCase().includes(term));
  count.textContent = shown.length + ' of ' + papers.length + ' papers';
  list.innerHTML = shown.map(p => `
    <div class="paper">
      <h2><a href="${{encodeURI(p.href)}}">${{esc(p.title)}}</a></h2>
      <div class="cat">${{esc(p.category)}}</div>
      <p>${{esc(p.summary)}}</p>
      <div>${{p.keywords.map(k => '<span class="tag">' + esc(k) + '</span>').join('')}}</div>
    </div>`).join('');
}}
q.addEventListener('input', render);
cat.addEventListener('change', render);
render();
</script>
</body>
</html>
"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "index.html").write_text(page, encoding="utf-8")
    return len(papers)


# ---------- DAG ----------
@dag(
    dag_id="paper_pipeline",
    schedule="*/10 * * * *",          
    catchup=False,                    
    max_active_runs=1,                
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["papers"],
)
def paper_pipeline():
    """Process new research-paper PDFs into a searchable static library page.

    Flow: find_new_pdfs -> process_paper (one task per PDF) -> regenerate_html -> notify_discord.
    Calling this function only DESCRIBES the task graph; Airflow runs the tasks later.
    """

    @task
    def find_new_pdfs() -> list[str]:
        """Find PDFs in ``papers/`` that are not yet in the database.
 
        Args:
            None.

        Returns:
            A sorted list of PDF filenames (not full paths) that still need
            processing. May be empty. Passed to ``process_paper`` via XCom.
        """
        conn = get_db()
        known = {r[0] for r in conn.execute("SELECT filename FROM papers")}
        conn.close()
        PAPERS_DIR.mkdir(parents=True, exist_ok=True)
        new = sorted(p.name for p in PAPERS_DIR.glob("*.pdf") if p.name not in known)
        print(f"Found {len(new)} new PDF(s): {new}")
        return new

    @task(max_active_tis_per_dag=1)
    def process_paper(filename: str) -> dict:
        """Convert one PDF to Markdown, get metadata from the LLM, save it to SQLite.

        Args:
            filename: Name of a PDF inside ``papers/`` (e.g. ``"paper.pdf"``).

        Returns:
            A small dict ``{"filename", "title", "category"}`` used by
            ``notify_discord``. (Full metadata lives in SQLite, not XCom.)

        Raises:
            RuntimeError: If the LLM never returns valid JSON.
            Exception: Any PDF-conversion or LLM connection error propagates
                and fails the task, with the cause visible in the task log.
        """
        import pymupdf4llm

        markdown = pymupdf4llm.to_markdown(str(PAPERS_DIR / filename))
        meta = ask_llm(markdown)

        conn = get_db()
        with conn:  # commits on success, rolls back on error
            conn.execute(
                "INSERT OR REPLACE INTO papers (filename, title, summary, keywords, category) "
                "VALUES (?, ?, ?, ?, ?)",
                (filename, meta["title"], meta["summary"], json.dumps(meta["keywords"]), meta["category"]),
            )
        conn.close()
        return {"filename": filename, "title": meta["title"], "category": meta["category"]}

    @task(trigger_rule="all_done")
    def regenerate_html() -> int:
        """Rebuild ``output/index.html`` from the database.

        Args:
            None.

        Returns:
            The number of papers listed on the generated page.
        """
        n = build_html()
        print(f"Wrote {OUTPUT_DIR / 'index.html'} with {n} papers")
        return n

    @task(trigger_rule="all_done")
    def notify_discord(results) -> None:
        """Send one Discord webhook message per newly processed paper.

        Args:
            results: Collection of the dicts returned by every ``process_paper``

        Returns:
            None.

        Raises:
            requests.HTTPError: If Discord rejects the webhook request.
        """
        import requests

        if not DISCORD_WEBHOOK_URL:
            print("DISCORD_WEBHOOK_URL not set; skipping notification")
            return
        for r in (x for x in results if x):
            requests.post(
                DISCORD_WEBHOOK_URL,
                json={"content": f"New paper processed: **{r['title']}** ({r['category']}) - `{r['filename']}`"},
                timeout=15,
            ).raise_for_status()

    new_files = find_new_pdfs()
    processed = process_paper.expand(filename=new_files)
    html_done = regenerate_html()
    notified = notify_discord(processed)
    processed >> html_done >> notified


paper_pipeline()
