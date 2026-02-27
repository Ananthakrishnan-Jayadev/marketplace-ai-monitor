# FB Marketplace Monitor

A lightweight Python script that monitors Facebook Marketplace using natural-language criteria. It reuses your real Chrome browser session, gathers listing details, evaluates candidates with Minimax M2.5 via Ollama Cloud, and sends Telegram notifications for AI-approved matches.

---

## Prerequisites

- **Python 3.10+** — [python.org](https://www.python.org/downloads/)
- **Google Chrome** installed (the script piggybacks on your existing Chrome profile)
- A **Telegram account** (for notifications)

---

## Project Structure

```
fb_monitor/
├── main.py          # Scheduler + orchestrator
├── browser.py       # Playwright browser control
├── parser.py        # HTML listing parser
├── detail_parser.py # Listing detail extraction (year/km/description)
├── ai_filter.py     # Ollama Cloud evaluation
├── notifier.py      # Telegram notifications
├── db.py            # SQLite deduplication
├── service.py       # FastAPI service + local dashboard API
├── dashboard/       # React dashboard (single-page UI)
├── config.json      # Local watchlist configuration (gitignored)
├── config.example.json # Example watchlist configuration
├── .env.example     # Environment template
├── .env             # Secrets (never commit this)
└── requirements.txt # Python dependencies
```

---

## Setup

### 1. Create and activate a virtual environment

```cmd
cd fb_monitor
python -m venv venv
venv\Scripts\activate
```

### 2. Install dependencies

```cmd
pip install -r requirements.txt
```

### 3. Install the Playwright browser

```cmd
playwright install chromium
```

### 4. Find your Chrome user data directory

This lets the script use your existing logged-in Facebook session.

| OS      | Path |
|---------|------|
| Windows | `C:\Users\<YourName>\AppData\Local\Google\Chrome\User Data` |
| macOS   | `~/Library/Application Support/Google/Chrome` |
| Linux   | `~/.config/google-chrome` |

> **Important:** Close all Chrome windows before running the monitor, otherwise Playwright cannot open the profile.

### 5. Create a Telegram bot

1. Open Telegram and search for **@BotFather**
2. Send `/newbot` and follow the prompts
3. Copy the **bot token** (looks like `123456789:ABCdef...`)

### 6. Get your Telegram chat ID

1. Search for **@userinfobot** on Telegram
2. Send it any message — it will reply with your **chat ID** (a number like `987654321`)

### 7. Create `.env` from `.env.example`

Copy the template and update it:

```cmd
cd fb_monitor
# Windows (cmd)
copy .env.example .env

# macOS/Linux
cp .env.example .env
```

### 8. Fill in `.env`

Open `fb_monitor/.env` and replace the placeholders:

```env
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrSTUvwxYZ
TELEGRAM_CHAT_ID=987654321
CHROME_USER_DATA_DIR=C:\Users\YourName\AppData\Local\Google\Chrome\User Data
OLLAMA_API_BASE_URL=https://your-ollama-endpoint
OLLAMA_API_KEY=your_ollama_cloud_api_key
OLLAMA_MODEL=minimax/m2.5
# MODEL_NAME also supported as an alias (takes priority if set)
# MODEL_NAME=minimax/m2.5
OLLAMA_TIMEOUT_SEC=30
LOG_LEVEL=INFO
DASHBOARD_ACCESS_TOKEN=change_me_to_a_long_random_secret
SERVICE_HOST=127.0.0.1
SERVICE_PORT=8080

# Optional global location filter (used in search URL)
LATITUDE=49.2827
LONGITUDE=-123.1207
RADIUS_KM=50
```

`LATITUDE`, `LONGITUDE`, and `RADIUS_KM` are optional. If provided, searches include Marketplace location filtering.
`LOG_LEVEL` is optional (`DEBUG`, `INFO`, `WARNING`, `ERROR`).
`DASHBOARD_ACCESS_TOKEN` is required for service/dashboard mode.

---

## Configure your watchlist

Create `fb_monitor/config.json` from `fb_monitor/config.example.json`, then edit it for your local watchlist.

```json
{
  "watchlist": [
    {
      "product": "Used Car",
      "query_prompt": "I need a car under 5000, less than 100k km, and model year 2017 or newer.",
      "min_price": 500,
      "max_price": 5000,
      "radius_km": 50,
      "ai_max_candidates": 25,
      "seed_keywords": ["toyota", "honda", "mazda", "hyundai"]
    }
  ]
}
```

| Field       | Description |
|-------------|-------------|
| `product`   | Display name used in Telegram notifications |
| `query_prompt` | Natural-language criteria sent to the AI evaluator |
| `seed_keywords` | Broad search terms used to retrieve candidates before AI scoring |
| `min_price` | Optional deterministic prefilter minimum (inclusive) |
| `max_price` | Optional deterministic prefilter maximum (inclusive) |
| `radius_km` | Optional per-product radius override (must be `> 0`); falls back to global `RADIUS_KM` |
| `ai_max_candidates` | Maximum number of deduped candidates to send to AI per run (default: `25`) |

> Legacy `keywords` is still accepted as a fallback if `seed_keywords` is not provided.

---

## Run the monitor (script mode)

```cmd
cd fb_monitor
python main.py
```

- Runs an initial check immediately on startup
- Schedules subsequent checks every **45–75 minutes** (randomized)
- Only runs between **8:00 AM and 11:00 PM** local time
- Validates `.env` and `config.json` at startup (exits early on invalid config)
- Pipeline: search by `seed_keywords` -> parse cards -> prefilter -> fetch detail pages -> AI evaluate -> notify pass results
- New matches trigger a Telegram message; already-seen listings are skipped automatically

## Run the local service + dashboard

```cmd
cd fb_monitor
python service.py
```

- Starts scheduler worker and FastAPI server in one process
- Dashboard available at `http://127.0.0.1:8080/` (or your configured host/port)
- All API/dashboard actions require `DASHBOARD_ACCESS_TOKEN`
- API endpoints:
  - `GET /api/health`
  - `GET /api/runs`
  - `GET /api/errors`
  - `GET /api/matches`
  - `GET /api/watchlist`
  - `POST /api/run/trigger`
  - `POST /api/watchlist/{watch_id}/pause`
  - `POST /api/watchlist/{watch_id}/resume`

### Example Telegram notification

```
Match Found: Used Car

Title: 2018 Honda Civic LX
Price: $4,900.00 (Range: $500 - $5000)
Location: Burnaby, BC
URL: https://www.facebook.com/marketplace/item/123456789
AI: Meets year and mileage constraints from description.
Extracted: Year: 2018 | Mileage: 92000 km
```

---

## Notes

- **Do not** have Chrome open when running the script — Playwright needs exclusive access to the profile directory.
- If you see `ProcessSingleton`/`SingletonLock` errors, close all Chrome windows and retry. The app also attempts one stale-lock cleanup retry automatically.
- The `seen_listings.db` SQLite file is created automatically on first run.
- SQLite also stores run history, classified errors, match decisions, and watchlist pause/resume state.
- Deduplication uses Marketplace item ID from URL when available (with URL/title-price fallback), so duplicate cards from the same listing are ignored reliably.
- If Ollama is unavailable, candidates are skipped for that run and retried in later runs.
- AI responses are parsed in dual mode: strict JSON first, then text fallback.
- Secrets are redacted in logs and persisted error messages.
- This tool is for personal, local use only. Use responsibly and in accordance with Facebook's Terms of Service.
