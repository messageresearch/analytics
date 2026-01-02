# Copilot Instructions

## ⚠️ CRITICAL: Git Deployment Rules

**NEVER commit or push to git without explicit user permission.** Always ask the user before running any of these commands:
- `git commit`
- `git push`
- `git add` followed by commit
- Any deployment scripts

This is a production website. Untested commits have caused outages. Always:
1. Test changes thoroughly in the dev server (`npm run dev`)
2. Ask the user to verify the changes work
3. Only then, ASK FOR PERMISSION before committing/deploying

## Project Overview

This project is a static website that provides a searchable archive of sermon transcripts. The site is built using a Python-based data processing pipeline and a modern frontend framework.

## Core Technologies

-   **Data Processing**: Python scripts are used to fetch, parse, and structure the sermon data.
-   **Frontend**: The website is built with a modern web stack (details in `package.json`).

## Data Pipeline

The data pipeline is a multi-step process:

1.  **Update Sermons**: The `update_sermons.py` script fetches the latest sermon transcripts from the YouTube channels defined in `channels.json`. It uses advanced logic, including the `spaCy` NLP library, to identify speakers and categorize video types.

    To update the sermon data, run:
    ```bash
    python update_sermons.py
    ```
    *Note: The script may prompt you to download the `en_core_web_sm` model on first run if you don't have it.*

2.  **Heal Archive**: The script also includes a `heal_archive` function that automatically runs to clean up existing data. It corrects speaker names, identifies song/worship services, and ensures metadata consistency.

3.  **Generate Site Data**: The `generate_site_data.py` script processes the raw text files in the `data/` directory. It extracts metadata (title, speaker, date), counts mentions of "Brother Branham", and generates structured JSON files in the `site_api/` directory. This data is then used by the frontend to display the sermon archive.

    To generate the site data, run:
    ```bash
    python generate_site_data.py
    ```

## Key Files and Directories

-   `channels.json`: Contains the list of YouTube channels to scrape for new sermons.
-   `speakers.json`: A list of speakers for metadata purposes.
-   `update_sermons.py`: Fetches new sermon transcripts from YouTube.
-   `generate_site_data.py`: Processes raw sermon data and generates the site's API data.
-   `data/`: Contains the raw sermon transcript files, organized by church.
-   `site_api/`: Contains the generated JSON data used by the frontend.
-   `src/`: Contains the frontend source code.

## Frontend Development

The frontend is a modern web application. To run the local development server and work on the frontend, use the following commands:

```bash
npm install
npm run dev
```

This will start a local server and you can view the site in your browser.
