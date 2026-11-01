# Research Paper Pipeline

An Airflow pipeline, running in Docker Desktop, that turns a folder of PDFs into a searchable
library page. Drop a PDF into a Google Drive-synced folder and, within about 10 minutes, it is
converted to Markdown, described by a locally served LLM, saved to SQLite, added to a static
`index.html`, and announced on Discord.

Nothing is sent to a cloud AI service: the model runs on your own machine.

## Folder layout

```
paper-project/                  # keep this OUTSIDE OneDrive (see Troubleshooting)
├── docker-compose.yml          # defines the airflow + ollama services and storage
├── Dockerfile                  # Airflow image + Python dependencies
├── .env                        # your settings (copy from .env.example)
├── README.md
└── dags/
    └── paper_pipeline.py       # the DAG

Google Drive/ResearchLibrary/   # synced by Google Drive for Desktop
├── papers/                     # put PDFs here
└── output/                     # index.html is written here
```

## Requirements

- Windows 11 with Docker Desktop (WSL 2 backend, the default)
- Google Drive for Desktop set to Mirror files (not Stream), so PDFs are real files on disk
- A Discord webhook URL.

## Create Google Drive folder

- Download and install `.exe` from this url: https://support.google.com/drive/answer/10838124?hl=en#zippy=%2Cinstall-on-windows
- Click on Settings
- Select Preferences
- Click Add Folder
- Choose Sync with Google Drive

## Setup

1. Pull the github project in a folder
2. Create the Drive folders: `ResearchLibrary\papers` and `ResearchLibrary\output`.
3. Create `.env` next to `docker-compose.yml` (copy `.env.example`):

   ```ini
   LIBRARY_PATH="C:/Users/YOUR_NAME/Google Drive/ResearchLibrary"
   LLM_MODEL=llama3.1
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
   ```

   Use forward slashes. `DISCORD_WEBHOOK_URL` may be left blank to disable notifications.
   Create a webhook in Discord under Server Settings → Integrations → Webhooks → New Webhook.
4. Build and start:

   ```powershell
   docker compose build --no-cache airflow
   docker compose up -d
   ```
5. Download the model:

   ```powershell
   docker compose exec ollama ollama pull llama3.1
   ```
6. Verify the install:

   ```powershell
   docker compose exec airflow python -m pip list | findstr /i "pymupdf openai requests"
   ```

   You should see `PyMuPDF`, `pymupdf4llm`, `openai` and `requests`.

## Running it

1. Open http://localhost:8080 (login is disabled; this is for local use only).
2. Find `paper_pipeline`, switch it on, and press the play button to run it immediately.
3. Copy a PDF into `ResearchLibrary\papers`.
4. Watch `process_paper` in the UI.
5. Open `ResearchLibrary\output\index.html`. Google Drive syncs it to your other devices.

After that it is hands-off: every 10 minutes the DAG looks for new PDFs.

Useful commands:

```powershell
docker compose logs -f airflow       # follow Airflow's output
docker compose restart airflow       # after editing .env
docker compose down                  # stop everything (volumes, and so data, are kept)
```

## Configuration

| Setting                 | Where    | Default                    | Meaning                                                                     |
| ----------------------- | -------- | -------------------------- | --------------------------------------------------------------------------- |
| `LIBRARY_PATH`        | `.env` | (required)                 | Windows path of the Drive folder. Mounted at `/library` in the container. |
| `LLM_MODEL`           | `.env` | `llama3.1`               | Model name as known to Ollama.                                              |
| `DISCORD_WEBHOOK_URL` | `.env` | empty                      | Webhook for notifications; blank disables them.                             |
| `RESEARCH_LIBRARY`    | compose  | `/library`               | Library path inside the container.                                          |
| `PAPER_DB`            | compose  | `/data/papers.db`        | SQLite file (Docker volume, outside Drive).                                 |
| `LLM_BASE_URL`        | compose  | `http://ollama:11434/v1` | OpenAI-compatible endpoint.                                                 |

Constants in `paper_pipeline.py`: `MAX_MARKDOWN_CHARS` (how much of each paper is sent to the model,
default 5,000) and the schedule (`*/10 * * * *`).

## How the DAG works

| Task                | What it does                                                                                             |
| ------------------- | -------------------------------------------------------------------------------------------------------- |
| `find_new_pdfs`   | Lists `papers/*.pdf` and returns the filenames not yet in the database.                                |
| `process_paper`   | One task per new PDF: PDF → Markdown → LLM JSON → validate → insert into SQLite. Runs one at a time. |
| `regenerate_html` | Rebuilds `output/index.html` from the whole database.                                                  |
| `notify_discord`  | Posts one Discord message per processed paper.                                                           |

```
find_new_pdfs ──► process_paper [one per PDF] ──► regenerate_html ──► notify_discord
```

`regenerate_html` and `notify_discord` use `trigger_rule="all_done"`, so one failed paper does not
block the page or the other notifications.

### LLM output

The model is asked for JSON in this shape:

```json
{
  "title": "Paper title",
  "summary": "A short summary of the paper.",
  "keywords": ["keyword one", "keyword two"],
  "category": "Data Engineering"
}
```

The reply is parsed with `json.loads()` and checked for the required fields and basic types. If
parsing or the check fails, the call is retried once; after a second failure the task fails and the
paper is retried on the next scheduled run. Errors such as "server not running" or "model not found"
are not retried inside the function, so they show up immediately in the task log.

### Database

SQLite table `papers`:

| Column           | Type    | Notes                                |
| ---------------- | ------- | ------------------------------------ |
| `id`           | integer | Primary key                          |
| `filename`     | text    | Unique: filenames are assumed unique |
| `title`        | text    |                                      |
| `summary`      | text    |                                      |
| `keywords`     | text    | JSON-encoded list                    |
| `category`     | text    |                                      |
| `processed_at` | text    | Defaults to the insert time          |

To reprocess a paper, delete its row; the next run picks it up again.
