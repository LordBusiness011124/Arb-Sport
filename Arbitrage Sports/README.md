# Arbitrage Sports

Python service that compares sportsbook fair probabilities against executable Kalshi prices, detects positive edge, and stores scan results in SQLite.

## Project Layout

- `src/arb/clients/`: sportsbook and Kalshi API clients
- `src/arb/core/`: odds math, matching, and edge calculations
- `src/arb/models/`: normalized data models
- `src/arb/services/`: SQLite storage helpers
- `src/arb/main.py`: runnable polling service
- `tests/`: unit tests for math, pricing, and matching
- `data/`: bind-mounted SQLite storage directory

## Local Setup

1. Create and activate a Python 3.11+ virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy the environment template:

```bash
cp .env.example .env
```

4. Fill in at least:

```bash
SPORTSBOOK_API_KEY=...
SPORTSBOOK_NAME=draftkings
SPORTSBOOK_SPORT=basketball_ncaab
```

## Local Run

Run the service directly:

```bash
python -m arb.main
```

Run the test suite:

```bash
pytest
```

## Discord Alerts

Discord is the easiest notification option for this project.

You only need:
- a Discord server where you can create webhooks
- one webhook URL

Set these in `.env`:

```bash
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
DISCORD_USERNAME=Arb Alerts
```

If `DISCORD_WEBHOOK_URL` is set, opportunities are still logged to stdout and are also posted to Discord.

## Docker

Build and run with Docker Compose:

```bash
docker compose up --build -d
```

View logs:

```bash
docker compose logs -f
```

Stop the service:

```bash
docker compose down
```

Notes:
- The container logs to stdout/stderr.
- SQLite is persisted in the local `data/` directory.
- Restart policy is `unless-stopped`.
- Runtime config is loaded from `.env`.
- SQLite history is pruned automatically based on `SQLITE_RETENTION_DAYS`.

## Ubuntu VPS Deployment

These steps assume Ubuntu 22.04 or 24.04.

1. Install Docker and Compose plugin:

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

2. Allow your user to run Docker without `sudo` and re-login:

```bash
sudo usermod -aG docker $USER
```

3. Clone the project onto the VPS:

```bash
git clone <your-repo-url>
cd arbitrage-sports
```

4. Create the runtime environment file:

```bash
cp .env.example .env
```

5. Edit `.env` with your real settings.

Minimum fields:

```bash
SPORTSBOOK_API_KEY=your_real_key
SPORTSBOOK_NAME=draftkings
SPORTSBOOK_SPORT=basketball_nba
SPORTSBOOK_REGIONS=us
SPORTSBOOK_POLL_SECONDS=180
KALSHI_POLL_SECONDS=45
EDGE_THRESHOLD=0.03
MINIMUM_LIQUIDITY=25
SQLITE_PATH=data/app.db
SQLITE_RETENTION_DAYS=14
```

6. Start the service:

```bash
docker compose up --build -d
```

7. Confirm it is running:

```bash
docker compose ps
docker compose logs -f
```

8. After code updates, redeploy:

```bash
git pull
docker compose up --build -d
```

## Operational Notes

- Database persistence lives in `./data/app.db` on the VPS.
- Database size is bounded by `SQLITE_RETENTION_DAYS` in `.env`. Older scan history is deleted automatically after each scan.
- Container restart behavior is handled by Docker with `restart: unless-stopped`.
- Logs stay in `docker compose logs` because the app writes to stdout.
- If the process exits repeatedly, inspect:

```bash
docker compose logs --tail=200
```
