from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlunparse

LOG_RE = re.compile(
    r'^(?P<ip>\S+)\s+\S+\s+\S+\s+\[(?P<ts>[^\]]+)\]\s+'
    r'"(?P<method>[A-Z]+)\s+(?P<url>[^"]+?)\s+HTTP/[0-9.]+"\s+'
    r'(?P<status>\d{3})\s+(?P<bytes>\S+)'
    r'(?:\s+(?P<latency_ms>\d+))?'
    r'(?:\s+"(?P<ref>[^"]*)")?'
    r'(?:\s+"(?P<ua>[^"]*)")?'
    r'(?:\s+"(?P<kv_raw>[^"]*)")?\s*$'
)
TS_FMT_RE = re.compile(r'(?P<day>\d{2})/(?P<mon>[A-Za-z]{3})/(?P<year>\d{4}):(?P<hms>\d{2}:\d{2}:\d{2})')
MONTH_MAP = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
    "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}
KV_UID_RE = re.compile(r'(?:^|[; ]+)(?:uid|UID|nth_uid|user_id)=([^; ]+)', re.I)
KV_PCID_RE = re.compile(r'(?:^|[; ]+)(?:pcid|PCID|nth_pcid)=([^; ]+)', re.I)
KV_SID_RE = re.compile(r'(?:^|[; ]+)(?:sid|SID|session_id)=([^; ]+)', re.I)
KV_EVT_RE = re.compile(r'(?:^|[; ]+)(?:evt|event|action)=([^; ]+)', re.I)
KV_LANG_RE = re.compile(r'(?:^|[; ]+)(?:accept_lang|lang)=([^; ]+)', re.I)
KV_CC_RE = re.compile(r'(?:^|[; ]+)(?:cc|country)=([^; ]+)', re.I)
KV_PAGE_RE = re.compile(r'(?:^|[; ]+)(?:page_type|page)=([^; ]+)', re.I)
JSESSION_RE = re.compile(r';jsessionid=[^/?#;]+', re.I)

def normalize_ts(raw: str) -> tuple[str, str]:
    m = TS_FMT_RE.search(raw)
    if not m:
        raise ValueError(f"unsupported timestamp format: {raw}")
    yyyy = m.group("year")
    mm = MONTH_MAP[m.group("mon")]
    dd = m.group("day")
    hms = m.group("hms")
    return f"{yyyy}-{mm}-{dd}", f"{yyyy}-{mm}-{dd} {hms}"

def to_full_url(url_raw: str, base_url: str) -> str:
    if url_raw.startswith("http://") or url_raw.startswith("https://"):
        return url_raw
    if not url_raw.startswith("/"):
        url_raw = "/" + url_raw
    return base_url.rstrip("/") + url_raw

def normalize_url(full_url: str) -> tuple[str, str, str, str]:
    p = urlparse(full_url)
    host = p.netloc or None
    path = JSESSION_RE.sub("", p.path or "")
    query = p.query or None
    norm = urlunparse((p.scheme, p.netloc, path, "", "", ""))
    return norm, host or "", path or "", query or ""

def extract_from_kv(kv_raw: str, regex: re.Pattern[str]) -> str:
    if not kv_raw:
        return ""
    m = regex.search(kv_raw)
    return m.group(1).strip() if m else ""

def infer_device_type(ua: str) -> str:
    ua_l = (ua or "").lower()
    if any(x in ua_l for x in ("iphone", "android", "mobile")):
        return "mobile"
    if any(x in ua_l for x in ("ipad", "tablet")):
        return "tablet"
    return "desktop"

def ref_host(ref: str) -> str:
    if not ref:
        return ""
    try:
        return urlparse(ref).netloc or ""
    except Exception:
        return ""

def parse_line(line: str, base_url: str) -> Optional[list[str]]:
    m = LOG_RE.match(line.rstrip("\n"))
    if not m:
        return None
    gd = m.groupdict()
    dt, ts = normalize_ts(gd["ts"])
    url_raw = gd["url"]
    url_full = to_full_url(url_raw, base_url)
    url_norm, host, path, query = normalize_url(url_full)
    kv_raw = gd.get("kv_raw") or ""
    ua = gd.get("ua") or ""
    ref = gd.get("ref") or ""
    bytes_val = "" if gd["bytes"] == "-" else gd["bytes"]
    latency_ms = gd.get("latency_ms") or ""
    uid = extract_from_kv(kv_raw, KV_UID_RE)
    pcid = extract_from_kv(kv_raw, KV_PCID_RE)
    sid = extract_from_kv(kv_raw, KV_SID_RE)
    evt = extract_from_kv(kv_raw, KV_EVT_RE)
    accept_lang = extract_from_kv(kv_raw, KV_LANG_RE)
    cc = extract_from_kv(kv_raw, KV_CC_RE)
    page_type = extract_from_kv(kv_raw, KV_PAGE_RE)
    return [
        dt, ts, gd["ip"], gd["method"], url_raw, url_full, url_norm, host, path, query,
        gd["status"], bytes_val, latency_ms, ref, ref_host(ref), ua, kv_raw,
        uid, pcid, sid, infer_device_type(ua), evt, accept_lang, cc, page_type
    ]

def main() -> None:
    ap = argparse.ArgumentParser(description="Parse webserver log to TSV (optimized)")
    ap.add_argument("--base-url", required=True)
    ap.add_argument("infile")
    ap.add_argument("outfile")
    ap.add_argument("--skip-bad-lines", action="store_true")
    args = ap.parse_args()

    infile = Path(args.infile)
    outfile = Path(args.outfile)
    rows = 0
    bad = 0

    with infile.open("r", encoding="utf-8", errors="replace") as fin, \
         outfile.open("w", encoding="utf-8", newline="") as fout:
        w = fout.write
        for line in fin:
            try:
                rec = parse_line(line, args.base_url)
                if rec is None:
                    bad += 1
                    continue
                w("\t".join("" if x is None else str(x) for x in rec) + "\n")
                rows += 1
            except Exception:
                bad += 1
                if not args.skip_bad_lines:
                    continue

    print(f"[parse_webserver_log_fast] rows={rows} bad={bad} outfile={outfile}")

if __name__ == "__main__":
    main()
