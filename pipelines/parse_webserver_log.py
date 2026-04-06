from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from urllib.parse import urlsplit

LOG_RE = re.compile(
    r'^(?P<ip>\S+)\s+-\s+-\s+\[(?P<ts>[^\]]+)\]\s+"(?P<method>[A-Z]+)\s+(?P<url>\S+)\s+(?P<httpv>[^"]+)"\s+'
    r'(?P<status>\d{3})\s+(?P<bytes>\S+)\s+"(?P<ref>[^"]*)"\s+"(?P<ua>[^"]*)"(?:\s+"(?P<kv>[^"]*)")?\s*$'
)

MONTH = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}

KV_RE = re.compile(r"(?:^|;\s*)([A-Za-z0-9_\-]+)=([^;]*)")


def parse_apache_ts(raw: str) -> tuple[str, str] | None:
    m = re.match(
        r"(?P<dd>\d{2})/(?P<mon>[A-Za-z]{3})/(?P<yyyy>\d{4}):(?P<hh>\d{2}):(?P<mm>\d{2}):(?P<ss>\d{2})",
        raw,
    )
    if not m:
        return None
    dt = f"{m.group('yyyy')}-{MONTH.get(m.group('mon'), '01')}-{m.group('dd')}"
    ts = f"{dt} {m.group('hh')}:{m.group('mm')}:{m.group('ss')}"
    return dt, ts


def strip_jsessionid(path: str) -> str:
    return re.sub(r";jsessionid=[^/?]+", "", path, flags=re.I)


def ensure_full_url(url_raw: str, base_url: str | None) -> str:
    if url_raw.startswith(("http://", "https://")):
        return url_raw
    if base_url:
        if url_raw.startswith("/"):
            return base_url.rstrip("/") + url_raw
        return base_url.rstrip("/") + "/" + url_raw.lstrip("/")
    return url_raw


def norm_url(url_raw: str, base_url: str | None) -> tuple[str, str, str, str, str, str]:
    full = ensure_full_url(url_raw, base_url)
    p = urlsplit(full)

    host = p.netloc or ""
    if p.scheme or p.netloc:
        path = strip_jsessionid(p.path or "/")
        query = p.query
    else:
        path = strip_jsessionid(url_raw.split("?", 1)[0] or "/")
        query = url_raw.split("?", 1)[1] if "?" in url_raw else ""

    if p.scheme and host:
        url_norm = f"{p.scheme}://{host}{path}"
    else:
        url_norm = path

    return full, url_norm, host, path, query, p.scheme


def parse_kv(kv_raw: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in KV_RE.finditer(kv_raw or ""):
        out[m.group(1)] = m.group(2).strip()
    return out


def pick_uid(kv: dict[str, str]) -> str:
    for k in ("uid", "UID", "nth_uid", "NTH_UID"):
        v = kv.get(k, "").strip()
        if v:
            return v
    return ""


def pick_first(kv: dict[str, str], *keys: str) -> str:
    for k in keys:
        v = kv.get(k, "").strip()
        if v != "":
            return v
    return ""


def ref_host(ref: str) -> str:
    if not ref or ref == "-":
        return ""
    try:
        return urlsplit(ref).netloc
    except Exception:
        return ""


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse apache-like webserver log to TSV")
    ap.add_argument("--base-url", default="")
    ap.add_argument("in_log")
    ap.add_argument("out_tsv")
    args = ap.parse_args()

    base_url = args.base_url.strip() or None

    src = Path(args.in_log)
    dst = Path(args.out_tsv)
    dst.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    matched = 0
    ts_parsed = 0

    with src.open("r", encoding="utf-8", errors="replace") as f_in, dst.open(
        "w", encoding="utf-8", newline=""
    ) as f_out:
        w = csv.writer(f_out, delimiter="\t", lineterminator="\n")

        for line in f_in:
            total += 1
            m = LOG_RE.match(line.rstrip("\n"))
            if not m:
                continue
            matched += 1

            ts_pair = parse_apache_ts(m.group("ts"))
            if not ts_pair:
                continue
            ts_parsed += 1
            dt, ts = ts_pair

            ip = m.group("ip")
            method = m.group("method")
            url_raw = m.group("url")
            status = m.group("status")
            bytes_ = "" if m.group("bytes") == "-" else m.group("bytes")
            ref = m.group("ref") or "-"
            ua = m.group("ua") or "-"
            kv_raw = (m.group("kv") or "").strip()

            url_full, url_norm, host, path, query, _scheme = norm_url(url_raw, base_url)
            kv = parse_kv(kv_raw)

            latency_ms = pick_first(kv, "latency_ms")
            uid = pick_uid(kv)
            pcid = pick_first(kv, "pcid", "nth_pcid")
            sid = pick_first(kv, "sid", "nth_sid")
            device_type = pick_first(kv, "device", "device_type")
            evt = pick_first(kv, "evt")
            accept_lang = pick_first(kv, "al", "accept_lang")
            cc = pick_first(kv, "cc", "nth_locale_country", "originCountryCode")
            page_type = pick_first(kv, "page_type")

            w.writerow(
                [
                    dt,
                    ts,
                    ip,
                    method,
                    url_raw,
                    url_full,
                    url_norm,
                    host,
                    path,
                    query,
                    status,
                    bytes_,
                    latency_ms,
                    ref,
                    ref_host(ref),
                    ua,
                    kv_raw,
                    uid,
                    pcid,
                    sid,
                    device_type,
                    evt,
                    accept_lang,
                    cc,
                    page_type,
                ]
            )

    print(
        f"[parse_webserver_log] total={total} matched={matched} ts_parsed={ts_parsed} out={dst}"
    )


if __name__ == "__main__":
    main()
