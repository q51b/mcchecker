# MC Checker

A fast desktop tool that checks whether Minecraft usernames are available, built on the official `api.minecraftservices.com` endpoints. Dark-themed Tkinter UI, bulk lookups, optional verification, and rate-limit-aware retries.

**by qrz**

---

## Features

- **Bulk lookup**: checks 10 names per request
- **Optional verification**: confirms candidates with Mojang's availability endpoint (catches blocked/reserved names)
- **Self-test**: probes the API before scanning so you never get silently wrong results
- **Smart rate limiting**: per-endpoint limiters, honors `Retry-After`, backs off on 429 / 403 / 5xx
- **Live dashboard**: counters, progress bar, speed and ETA
- **Results table** with search, status filter, right-click menu (copy, open on NameMC)
- **Resumable**: skips names already saved from previous runs
- **Export** to `.txt` / `.csv`; one-click copy of all available names

## Requirements

- Python 3.9+
- `requests`
- Tkinter (bundled with the standard Python installers; on Debian/Ubuntu: `sudo apt install python3-tk`)

```bash
pip install requests
```

## Usage

1. Put one username per line in `usernames.txt` (lines starting with `#` are ignored).
2. Run:
   ```bash
   python mc_checker.py
   ```
3. Choose your file, adjust options, press **Start scan** (`Ctrl+Enter`). Press `Esc` to stop.

### Verification (optional)

Lookups alone can only tell you a name is *not registered*. To confirm a name is truly claimable, enable **Verify with availability endpoint** and provide a Minecraft access token, either:

```bash
# macOS / Linux
export MINECRAFT_ACCESS_TOKEN="your_token"
# Windows (PowerShell)
$env:MINECRAFT_ACCESS_TOKEN = "your_token"
```

or paste it into the token field in the UI. The token is never written to disk. The availability endpoint is heavily rate limited, so verification runs at about 3 names per minute by default.

## API endpoints

| Purpose | Endpoint |
|---|---|
| Single lookup | `GET  /minecraft/profile/lookup/name/{name}` |
| Bulk lookup | `POST /minecraft/profile/lookup/bulk/byname` |
| Availability | `GET  /minecraft/profile/name/{name}/available` (needs token) |

All under `https://api.minecraftservices.com`. Defaults live in the `Config` dataclass at the top of `mc_checker.py`.

## Output files

| File | Contents |
|---|---|
| `available.json` | Available names (with `verified` flag and timestamp) |
| `taken.txt` | Taken names with UUID |
| `invalid_pattern.txt` | Names that aren't 3-16 chars of `A-Z a-z 0-9 _` |
| `rejected_policy.txt` / `improper.txt` | Names Mojang refuses |
| `forbidden.txt` / `unknown_errors.log` | Persistent API errors |
| `failed.log` | Transient network failures (retried on the next run) |

Turn off **Skip previously checked names** to re-check names that were saved before.

## Configuration

Edit the `Config` dataclass in `mc_checker.py`:

```python
use_bulk: bool = True
verify_availability: bool = False
token_env: str = "MINECRAFT_ACCESS_TOKEN"
verify_requests_per_minute: float = 3.0
self_test: bool = True
```

## Disclaimer

Not affiliated with Mojang or Microsoft. Respect the API's rate limits and terms of use.

---

Made by **qrz**
