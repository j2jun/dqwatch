"""Fetch source data as CSV bytes: watched folder, CSV URL, or Open-Meteo API."""
import csv
import hashlib
import io
import json
import os
import urllib.request
from pathlib import Path

from dqwatch.models import Source


class FetchError(Exception):
    pass


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def open_meteo_url(lat: float, lon: float) -> str:
    return (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
            "&hourly=temperature_2m,precipitation,wind_speed_10m")


def flatten_open_meteo(payload: dict) -> bytes:
    hourly = payload["hourly"]
    keys = ["time"] + [k for k in hourly if k != "time"]
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(keys)
    w.writerows(zip(*(hourly[k] for k in keys)))
    return out.getvalue().encode()


def _get(url: str, timeout: float) -> bytes:
    if not url.startswith(("http://", "https://")):
        raise FetchError("Only http(s) links are supported.")
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:  # HTTPError, URLError, timeouts, resets
        raise FetchError(f"Couldn't reach {url}: {e}") from e


def fetch(source: Source, inbox_root: str, timeout: float = 30) -> list[tuple[str, bytes]]:
    if source.kind == "folder":
        root = os.path.realpath(inbox_root)
        folder = os.path.realpath(os.path.join(root, source.location))
        if os.path.commonpath([root, folder]) != root or not os.path.isdir(folder):
            raise FetchError(f"Folder {source.location!r} wasn't found in the inbox.")
        files = sorted((e for e in os.scandir(folder) if e.is_file() and e.name.lower().endswith(".csv")),
                       key=lambda e: (e.stat().st_mtime, e.name))
        if source.write_mode == "replace":
            files = files[-1:]
        return [(e.name, Path(e.path).read_bytes()) for e in files]
    if source.kind == "url":
        return [("", _get(source.location, timeout))]
    if source.kind == "api":
        try:
            return [("", flatten_open_meteo(json.loads(_get(source.location, timeout))))]
        except FetchError:
            raise
        except (ValueError, KeyError, TypeError) as e:
            raise FetchError(f"The API returned data in an unexpected shape: {e}") from e
    raise FetchError(f"Unknown source kind {source.kind!r}.")
