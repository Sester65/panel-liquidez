#!/usr/bin/env python3
"""
Panel de Liquidez Institucional — Servidor Cloud
=================================================
Desplegable en Render.com, Railway, Fly.io, etc.

Variables de entorno requeridas:
  FRED_KEY  — API key de FRED (fred.stlouisfed.org, gratuita)

Variables de entorno opcionales:
  PORT      — Puerto (Render lo asigna automaticamente)
"""

import http.server, socketserver, threading, os, sys
import json, urllib.request, urllib.error

# ── Configuracion ──────────────────────────────────────────────────────────────
PORT     = int(os.environ.get("PORT", 8765))
fred_key = os.environ.get("FRED_KEY", "").strip()
HTML_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "liquidity-dashboard.html")

if not fred_key:
    print("AVISO: FRED_KEY no configurada — se usaran datos embebidos.", flush=True)
else:
    print(f"  FRED key cargada: {fred_key[:8]}{'*'*10}", flush=True)

if not os.path.exists(HTML_FILE):
    print(f"ERROR: No encuentro {HTML_FILE}", flush=True)
    sys.exit(1)

FRED_SERIES = {
    "EFFR":       "EFFR",
    "IORB":       "IORB",
    "SOFR":       "SOFR",
    "TGCR":       "TGCRRATE",
    "CPAA":       "DCPF3M",
    "ONRRP":      "RRPONTSYAWARD",
    "ESTR":       "ECBESTRVOLWGTTRMDMNRT",
    "EURIBOR3M":  "IR3TIB01EZM156N",
    "STLFSI":     "STLFSI4",
    "ECBASSETS":  "ECBASSETSW",
}

def fred_fetch_series(series_id, api_key, start="2022-01-01"):
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&observation_start={start}"
           f"&api_key={api_key}&file_type=json&sort_order=asc")
    req = urllib.request.Request(url, headers={"User-Agent": "LiquidityDashboard/1.0"})
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read())
    if "error_message" in data:
        raise ValueError(f"FRED {series_id}: {data['error_message']}")
    result = {}
    for obs in data.get("observations", []):
        if obs["value"] != ".":
            ym = obs["date"][:7]
            result[ym] = float(obs["value"])
    return result

def ecb_fetch_banknotes(start="2022-01"):
    """Fetch banknotes in circulation from ECB Data Portal (no key needed).
    Corrected series key: BKN.M.U2.NC10.B.ALLD.AS.S.E
    Values in thousands of euros -> converted to billions."""
    try:
        url = ("https://data-api.ecb.europa.eu/service/data/BKN/"
               "M.U2.NC10.B.ALLD.AS.S.E"
               f"?startPeriod={start}&format=jsondata")
        req = urllib.request.Request(
            url, headers={"Accept": "application/json",
                          "User-Agent": "LiquidityDashboard/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        dims = data["structure"]["dimensions"]["observation"]
        time_dim = next((d for d in dims if d["id"] in ("TIME_PERIOD","PERIOD")), None)
        series_data = data["dataSets"][0]["series"]
        result = {}
        for _, sv in series_data.items():
            for obs_idx, obs_val in sv.get("observations", {}).items():
                if obs_val[0] is not None and time_dim:
                    period = time_dim["values"][int(obs_idx)]["id"][:7]
                    result[period] = round(obs_val[0] / 1e6, 1)  # thousands€ -> bn€
        if result:
            print(f"    OK: ECBBNK ({len(result)} obs)", flush=True)
        else:
            print("    WARN: ECBBNK sin observaciones", flush=True)
        return result
    except Exception as e:
        print(f"    WARN: ECBBNK fallo silencioso - {e}", flush=True)
        return {}


def build_spread(map_a, map_b):
    dates = sorted(set(map_a) & set(map_b))
    return [{"date": d, "spread": round(map_a[d] - map_b[d], 4)} for d in dates]

def to_series(map_data, divisor=1, decimals=1):
    return [{"date": d, "spread": round(v / divisor, decimals)}
            for d, v in sorted(map_data.items())]

# Cache
_rates_cache = None
_cache_lock  = threading.Lock()

def fetch_all_fred(api_key):
    print("  Descargando series FRED...", flush=True)
    series = {}
    for name, fred_id in FRED_SERIES.items():
        try:
            series[name] = fred_fetch_series(fred_id, api_key)
            print(f"    OK: {name} ({len(series[name])} obs)", flush=True)
        except Exception as e:
            print(f"    WARN: {name} fallo - {e}", flush=True)
            series[name] = {}

    effr  = series.get("EFFR", {})
    iorb  = series.get("IORB", {})
    sofr  = series.get("SOFR", {})
    tgcr  = series.get("TGCR", {})
    cpaa  = series.get("CPAA", {})
    estr  = series.get("ESTR", {})
    euribor = series.get("EURIBOR3M", {})
    stlfsi  = series.get("STLFSI", {})
    ecbassets = series.get("ECBASSETS", {})

    # ECB banknotes from ECB Data Portal
    print("  Descargando billetes BCE (ECB Data Portal)...", flush=True)
    try:
        ecb_bnk = ecb_fetch_banknotes()
        print(f"    OK: ECBBNK ({len(ecb_bnk)} obs)", flush=True)
    except Exception as e:
        print(f"    WARN: ECBBNK fallo - {e}", flush=True)
        ecb_bnk = {}

    result = {
        "asOf":         __import__("datetime").date.today().isoformat(),
        "source":       "FRED + ECB Data Portal",
        # Gauge spreads
        "effr_iorb":    build_spread(effr,    iorb),
        "sofr_iorb":    build_spread(sofr,    iorb),
        "tgcr_iorb":    build_spread(tgcr,    iorb),
        "cp_sofr":      build_spread(cpaa,    sofr),
        "euribor_estr": build_spread(euribor, estr),
        "stlfsi":       to_series(stlfsi),
        # Absolute rates for TASAS tab sparklines
        "ECBASSETS":    to_series(ecbassets, divisor=1000, decimals=0),  # M -> bn
        "ECBBNK":       [{"date": d, "spread": v} for d, v in sorted(ecb_bnk.items())],
    }
    total = sum(len(v) for v in result.values() if isinstance(v, list))
    print(f"  Datos listos - {total} puntos totales", flush=True)
    return result

def get_rates_data():
    global _rates_cache
    with _cache_lock:
        if _rates_cache is None and fred_key:
            try:
                _rates_cache = fetch_all_fred(fred_key)
            except Exception as e:
                print(f"  ERROR FRED: {e}", flush=True)
        if not _rates_cache:
            _rates_cache = {
                "asOf": __import__("datetime").date.today().isoformat(),
                "source": "Sin datos (usando embebidos)",
                "effr_iorb":[], "sofr_iorb":[], "tgcr_iorb":[],
                "cp_sofr":[], "euribor_estr":[], "stlfsi":[],
                "ECBASSETS":[], "ECBBNK":[],
            }
        return _rates_cache

# Pre-fetch en background al arrancar
_rates_cache = None
_cache_lock  = threading.Lock()

import datetime as _dt

_cache_timestamp = None   # when the cache was last populated

def _cache_is_stale():
    """Returns True if cache is missing or older than 6 hours."""
    if _rates_cache is None or _cache_timestamp is None:
        return True
    age = (_dt.datetime.utcnow() - _cache_timestamp).total_seconds()
    return age > 6 * 3600  # 6 hours

def get_rates_data(force=False):
    global _rates_cache, _cache_timestamp
    with _cache_lock:
        if fred_key and (force or _cache_is_stale()):
            print(f"  Actualizando datos FRED ({'forzado' if force else 'cache caducado'})...", flush=True)
            try:
                _rates_cache = fetch_all_fred(fred_key)
                _cache_timestamp = _dt.datetime.utcnow()
                print(f"  Cache actualizado: {_cache_timestamp.strftime('%Y-%m-%d %H:%M')} UTC", flush=True)
            except Exception as e:
                print(f"  ERROR FRED: {e}", flush=True)
        if not _rates_cache:
            _rates_cache = {
                "asOf": _dt.date.today().isoformat(),
                "source": "Sin datos FRED",
                "effr_iorb":[], "sofr_iorb":[], "tgcr_iorb":[],
                "cp_sofr":[], "euribor_estr":[], "stlfsi":[],
                "ECBASSETS":[], "ECBBNK":[],
            }
        return _rates_cache

if fred_key:
    threading.Thread(target=get_rates_data, daemon=True).start()
else:
    print("  Sin FRED_KEY — configura la variable de entorno en Render.", flush=True)

# ── Servidor HTTP ───────────────────────────────────────────────────────────────
class Handler(http.server.SimpleHTTPRequestHandler):

    def do_POST(self):
        # Shutdown not applicable in cloud — ignore gracefully
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def do_GET(self):
        if self.path == "/api/config":
            self._serve_json({"hasFredKey": bool(fred_key)})
        elif self.path in ("/api/rates", "/api/rates/"):
            self._serve_json(get_rates_data())
        elif self.path == "/api/refresh":
            self._serve_json(get_rates_data(force=True))
        elif self.path in ("/", "/index.html", ""):
            self._serve_html()
        else:
            self._serve_html()

    def _serve_html(self):
        try:
            with open(HTML_FILE, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())

    def _serve_json(self, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, fmt, *args):
        # Minimal logging for cloud
        if "/api/" in (args[0] if args else ""):
            print(f"  {args[0][:60]}", flush=True)

# ── Arranque ────────────────────────────────────────────────────────────────────
print("=" * 50, flush=True)
print(f"  Panel de Liquidez — Puerto {PORT}", flush=True)
print("=" * 50, flush=True)

socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("0.0.0.0", PORT), Handler) as httpd:
    httpd.serve_forever()
