# Optibus Payroll Compare Streamlit App

## What this project does

This project refactors the uploaded local script into a GitHub-ready Streamlit app for comparing Optibus payroll outputs before and after Work Entity changes.

The workflow stays aligned with the original script:

1. Fetch **PRE** payroll data for a date range
2. Fetch **PRE** absences
3. Fetch **PRE** actual and planned allocations
4. Pause while you update Work Entities in the Optibus web UI
5. Fetch **POST** payroll data
6. Generate:
   - PRE payroll CSV
   - POST payroll CSV
   - payroll differences CSV
   - enriched payroll differences CSV
   - PRE absences CSV
   - PRE actual allocation CSV
   - PRE planned allocation CSV
   - a ZIP containing all outputs

The CSV column shapes are preserved so downstream processes should continue to work.

## Proposed architecture

The refactor separates the project into three layers:

- `streamlit_app.py`: Streamlit UI only
- `optibus_payroll_compare/api.py`: API client and data-fetching logic
- `optibus_payroll_compare/processing.py`: CSV shaping, diffing, enrichment, and ZIP creation
- `optibus_payroll_compare/pipeline.py`: PRE/POST orchestration
- `optibus_payroll_compare/models.py` and `utils.py`: shared data structures and helpers

This removes local-only UI assumptions such as AppleScript prompts, `input()`, and macOS Keychain storage from the core logic.

## Repo structure

```text
.
├── .env.example
├── .gitignore
├── README.md
├── requirements.txt
├── streamlit_app.py
└── optibus_payroll_compare
    ├── __init__.py
    ├── api.py
    ├── models.py
    ├── pipeline.py
    ├── processing.py
    └── utils.py
```

## Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Environment variables

You can set these locally in a `.env` file or in your shell:

```bash
OPTIBUS_BASE_URL=https://YOUR-ACCOUNT.api.ops.optibus.co
OPTIBUS_API_CLIENT=YOUR_ACCOUNT_NAME
OPTIBUS_API_KEY=YOUR_API_KEY
```

A sample template is included in `.env.example`.

## How to run locally

```bash
streamlit run streamlit_app.py
```

Then:

1. Enter your connection details if they are not already in environment variables
2. Choose the start and end date
3. Optionally provide paycodes, batch overrides, or a diff tolerance
4. Click **Run PRE fetch**
5. Make your Work Entity changes in Optibus
6. Return to the app and click **Run POST fetch + compare**
7. Download the CSVs or the ZIP bundle

## Example usage

Typical local usage:

```bash
export OPTIBUS_BASE_URL="https://YOUR-ACCOUNT.api.ops.optibus.co"
export OPTIBUS_API_CLIENT="ADO"
export OPTIBUS_API_KEY="YOUR_API_KEY"
streamlit run streamlit_app.py
```

## Streamlit deployment notes

### Streamlit Community Cloud

Set the following in the app settings or secrets:

- `OPTIBUS_BASE_URL`
- `OPTIBUS_API_CLIENT`
- `OPTIBUS_API_KEY`

This app writes outputs to a temporary directory for the current session. Keep the same browser session open between the PRE and POST steps.

### Paths and secrets

- No hardcoded local paths are used
- No macOS-only AppleScript or Keychain features remain
- Credentials are read from Streamlit inputs, environment variables, or Streamlit secrets/environment settings

## What changed from the original script

### Preserved

- The same core Optibus API workflow
- Driver and date chunking to reduce 413 errors
- Pre/post payroll comparison logic
- Enriched differences with absences and allocation
- CSV shapes and naming style

### Changed for maintainability and Streamlit readiness

- Removed local-only AppleScript dialogs and CLI `input()` pause
- Replaced the pause with a two-step Streamlit workflow
- Split API access, processing, and orchestration into separate modules
- Added validation and clearer error handling
- Added downloadable outputs in the UI
- Added a ZIP bundle for all generated files
- Removed unused local-only configuration persistence behavior

## Notes

This refactor intentionally follows the behavior of the current uploaded script, which runs across **all regions/depots in the account** rather than prompting for a single depot.
