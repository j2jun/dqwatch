import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from dqwatch import fetch
from dqwatch.models import Source


def _src(kind, location, mode="upsert"):
    return Source(1, "s", kind, location, 1, mode, None, [])


@pytest.fixture
def server():
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/slow":
                time.sleep(2)
            code = 500 if self.path == "/boom" else 200
            self.send_response(code)
            self.end_headers()
            self.wfile.write(b"id\n1\n")

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


def test_folder_lists_csvs(tmp_path):
    d = tmp_path / "orders"
    d.mkdir()
    (d / "a.csv").write_bytes(b"x\n1\n")
    (d / "b.csv").write_bytes(b"x\n2\n")
    (d / "notes.txt").write_bytes(b"skip")
    os.utime(d / "a.csv", (1, 1))
    assert fetch.fetch(_src("folder", "orders"), str(tmp_path)) == [("a.csv", b"x\n1\n"), ("b.csv", b"x\n2\n")]


def test_folder_replace_newest_only(tmp_path):
    d = tmp_path / "orders"
    d.mkdir()
    (d / "a.csv").write_bytes(b"old")
    (d / "b.csv").write_bytes(b"new")
    os.utime(d / "a.csv", (1, 1))
    assert fetch.fetch(_src("folder", "orders", "replace"), str(tmp_path)) == [("b.csv", b"new")]


def test_folder_missing_raises(tmp_path):
    with pytest.raises(fetch.FetchError):
        fetch.fetch(_src("folder", "nope"), str(tmp_path))


def test_folder_escape_rejected(tmp_path):
    with pytest.raises(fetch.FetchError):
        fetch.fetch(_src("folder", "../etc"), str(tmp_path / "inbox"))


def test_url_ok(server):
    assert fetch.fetch(_src("url", server + "/ok"), "") == [("", b"id\n1\n")]


def test_url_500_raises(server):
    with pytest.raises(fetch.FetchError):
        fetch.fetch(_src("url", server + "/boom"), "")


def test_url_timeout_raises(server):
    with pytest.raises(fetch.FetchError):
        fetch.fetch(_src("url", server + "/slow"), "", timeout=0.5)


def test_url_non_http_rejected():
    with pytest.raises(fetch.FetchError):
        fetch.fetch(_src("url", "file:///etc/passwd"), "")


def test_flatten_open_meteo():
    payload = {"hourly": {"time": ["t1", "t2"], "temperature_2m": [1.0, 2.0]}}
    assert fetch.flatten_open_meteo(payload) == b"time,temperature_2m\r\nt1,1.0\r\nt2,2.0\r\n"


def test_open_meteo_url():
    assert fetch.open_meteo_url(1.5, -2) == ("https://api.open-meteo.com/v1/forecast?latitude=1.5&longitude=-2"
                                             "&hourly=temperature_2m,precipitation,wind_speed_10m")


def test_url_too_large_rejected(server, monkeypatch):
    monkeypatch.setattr(fetch, "MAX_BYTES", 3)
    with pytest.raises(fetch.FetchError, match="larger"):
        fetch.fetch(_src("url", server + "/ok"), "")


def test_api_only_open_meteo(server):
    with pytest.raises(fetch.FetchError, match="open-meteo"):
        fetch.fetch(_src("api", server + "/ok"), "")
