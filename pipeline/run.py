#!/usr/bin/env python3
"""观澜 Horizon · 免费信源采集管线

流程：读取 sources.json → 逐个信源抓取 → 去重合并 → DeepSeek 摘要与红黄绿初判 → 写出 data/feed.json

运行：
    pip install -r pipeline/requirements.txt
    DEEPSEEK_API_KEY=sk-... python pipeline/run.py

没有 DEEPSEEK_API_KEY 时只采集不分级，网页照常展示原文标题和链接。
任何一个信源失败都不会中断整体运行，失败原因写进「信源健康」表。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote_plus

import feedparser
import requests

ROOT = Path(__file__).resolve().parent.parent
SOURCES = ROOT / "pipeline" / "sources.json"
FEED = ROOT / "data" / "feed.json"

KEEP_DAYS = int(os.getenv("KEEP_DAYS", "14"))        # 库里保留多少天
SHOW_DAYS = int(os.getenv("SHOW_DAYS", "7"))         # 网页展示多少天
LOOKBACK = os.getenv("LOOKBACK", "2d")               # 每次向搜索类信源要多久的新闻
PER_SOURCE = int(os.getenv("PER_SOURCE", "40"))      # 每个信源最多取几条
MAX_AI = int(os.getenv("MAX_AI", "160"))             # 每次最多送多少条给 AI，控制成本
AI_BATCH = 10
TIMEOUT = 25
UA = "Mozilla/5.0 (compatible; GuanlanRadar/1.0; +https://sonia16-creator.github.io/guanlan/)"

DEEPSEEK_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/") + "/chat/completions"
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

GNEWS_LANG = {
    "en": ("en-US", "US", "US:en"),
    "en-PH": ("en-PH", "PH", "PH:en"),
    "es": ("es-419", "CL", "CL:es-419"),
    "pt": ("pt-BR", "BR", "BR:pt-419"),
    "zh": ("zh-CN", "CN", "CN:zh-Hans"),
    "ko": ("ko", "KR", "KR:ko"),
}
EVENT_TYPES = ["高管与治理", "业绩", "监管与司法", "股权与资本", "政策", "地缘与黑天鹅", "舆情发酵", "信用", "运营与市场"]
KEY_CHOKEPOINTS = ["suez", "bab el-mandeb", "hormuz", "panama", "malacca", "good hope", "taiwan", "bosporus", "gibraltar"]

session = requests.Session()
session.headers.update({"User-Agent": UA})


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def clean(text: str, n: int = 400) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:n]


def norm_title(t: str) -> str:
    t = re.sub(r"\s+[-–|]\s+[^-–|]{2,60}$", "", t or "")  # 去掉 Google News 标题后缀「 - 媒体名」
    return re.sub(r"[\W_]+", "", t.lower())[:120]


def item_id(link: str, title: str) -> str:
    return hashlib.sha1((norm_title(title) or link).encode("utf-8")).hexdigest()[:16]


# ---------------- 抓取 ----------------

def fetch_rss(url: str) -> list[dict]:
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    fp = feedparser.parse(r.content)
    if fp.bozo and not fp.entries:
        raise ValueError(f"无法解析 RSS：{fp.bozo_exception}")
    out = []
    for e in fp.entries[:PER_SOURCE]:
        tm = e.get("published_parsed") or e.get("updated_parsed")
        pub = iso(datetime(*tm[:6], tzinfo=timezone.utc)) if tm else None
        src = e.get("source", {}).get("title") if isinstance(e.get("source"), dict) else None
        out.append({"title": clean(e.get("title", ""), 300), "link": e.get("link", ""),
                    "published": pub, "snippet": clean(e.get("summary", "")), "outlet": src})
    return out


def fetch_gnews(q: str, lang: str) -> list[dict]:
    hl, gl, ceid = GNEWS_LANG.get(lang, GNEWS_LANG["en"])
    url = f"https://news.google.com/rss/search?q={quote_plus(q + ' when:' + LOOKBACK)}&hl={hl}&gl={gl}&ceid={ceid}"
    items = fetch_rss(url)
    for it in items:
        it["snippet"] = ""  # Google News 的摘要只是重复标题
    return items


_last_gdelt = 0.0


def fetch_gdelt(q: str) -> list[dict]:
    global _last_gdelt
    wait = 6 - (time.time() - _last_gdelt)  # GDELT 要求两次请求间隔 5 秒以上
    if wait > 0:
        time.sleep(wait)
    _last_gdelt = time.time()
    url = ("https://api.gdeltproject.org/api/v2/doc/doc?mode=artlist&format=json&sort=datedesc"
           f"&maxrecords={PER_SOURCE}&timespan={LOOKBACK}&query={quote_plus(q)}")
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    try:
        data = r.json()
    except ValueError:
        raise ValueError("GDELT 返回的不是 JSON：" + r.text[:120])
    out = []
    for a in data.get("articles", []):
        sd = a.get("seendate", "")
        pub = None
        if re.match(r"\d{8}T\d{6}Z", sd):
            pub = iso(datetime.strptime(sd, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc))
        out.append({"title": clean(a.get("title", ""), 300), "link": a.get("url", ""),
                    "published": pub, "snippet": "", "outlet": a.get("domain")})
    return out


def fetch_portwatch() -> list[dict]:
    """IMF PortWatch 要道每日通航量（AIS），返回各要道近 7 天与此前 28 天均值的变化。"""
    url = ("https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/"
           "FeatureServer/0/query?where=1%3D1&outFields=portname,date,n_total,n_tanker,n_container,n_dry_bulk"
           "&orderByFields=date%20DESC&resultRecordCount=2000&f=json")
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    feats = r.json().get("features", [])
    if not feats:
        raise ValueError("PortWatch 没有返回数据")
    rows: dict[str, list[dict]] = {}
    for f in feats:
        a = f.get("attributes", {})
        if a.get("portname") and a.get("date") is not None:
            rows.setdefault(a["portname"], []).append(a)
    out = []
    for name, rs in rows.items():
        rs.sort(key=lambda a: a["date"], reverse=True)
        last7, prev = rs[:7], rs[7:35]
        if len(last7) < 3 or len(prev) < 7:
            continue
        avg = lambda xs, k: sum((x.get(k) or 0) for x in xs) / len(xs)
        a7, a28 = avg(last7, "n_total"), avg(prev, "n_total")
        out.append({
            "name": name,
            "latest": datetime.fromtimestamp(rs[0]["date"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d"),
            "avg7": round(a7, 1), "avg_prev28": round(a28, 1),
            "change": round((a7 - a28) / a28 * 100, 1) if a28 else None,
            "tanker7": round(avg(last7, "n_tanker"), 1), "container7": round(avg(last7, "n_container"), 1),
            "drybulk7": round(avg(last7, "n_dry_bulk"), 1),
            "key": any(k in name.lower() for k in KEY_CHOKEPOINTS),
        })
    out.sort(key=lambda c: (not c["key"], -abs(c["change"] or 0)))
    return out


def collect(sources: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    items, health, chokepoints = [], [], []
    for s in sources:
        t0 = time.time()
        rec = {"id": s["id"], "name": s["name"], "sector": s["sector"], "type": s["type"], "ok": False, "count": 0}
        try:
            if s["type"] == "portwatch":
                chokepoints = fetch_portwatch()
                rec.update(ok=True, count=len(chokepoints))
            else:
                if s["type"] == "rss":
                    got = fetch_rss(s["url"])
                elif s["type"] == "gnews":
                    got = fetch_gnews(s["q"], s.get("lang", "en"))
                elif s["type"] == "gdelt":
                    got = fetch_gdelt(s["q"])
                else:
                    raise ValueError("未知信源类型 " + s["type"])
                if s.get("filter"):
                    pat = re.compile(s["filter"], re.I)
                    got = [g for g in got if pat.search(g["title"] + " " + g["snippet"])]
                for g in got:
                    if not g["title"] or not g["link"]:
                        continue
                    g.update(source_id=s["id"], source=s["name"], sector=s["sector"],
                             country=s.get("country"), persona=[s["persona"]] if s.get("persona") else [],
                             layer=s.get("layer"), official=bool(s.get("official")))
                    items.append(g)
                rec.update(ok=True, count=len(got))
        except Exception as e:  # 单个信源失败不影响整体
            rec["error"] = f"{type(e).__name__}: {str(e)[:160]}"
        rec["ms"] = int((time.time() - t0) * 1000)
        health.append(rec)
        print(("OK " if rec["ok"] else "ERR") + f" {s['id']:<18} {rec['count']:>3} {rec.get('error', '')}", flush=True)
    return items, health, chokepoints


# ---------------- 合并去重 ----------------

def merge(old: list[dict], new: list[dict], ts: datetime) -> tuple[list[dict], int]:
    store = {it["id"]: it for it in old}
    added = 0
    for it in new:
        iid = item_id(it["link"], it["title"])
        if iid in store:
            ex = store[iid]
            srcs = set(ex.get("also", [])) | {it["source"]}
            srcs.discard(ex["source"])
            ex["also"] = sorted(srcs)
            continue
        it["id"] = iid
        it["first_seen"] = iso(ts)
        added += 1
        if not it.get("published"):
            it["published"] = it["first_seen"]
        store[iid] = it
    cutoff = ts - timedelta(days=KEEP_DAYS)
    kept = [it for it in store.values() if (parse_iso(it.get("published")) or ts) >= cutoff]
    kept.sort(key=lambda it: it.get("published") or "", reverse=True)
    return kept, added


# ---------------- DeepSeek 摘要与分级 ----------------

SYSTEM_PROMPT = f"""你是锋睿「观澜 Horizon」舆情雷达的分析员。输入是一批新闻条目（可能是英文、西班牙文、葡萄牙文、韩文或中文）。
对每条输出简体中文结果，只依据条目里给出的信息，不编造细节。

字段：
- i：原样返回条目序号
- relevant：与所属子目录是否相关（energy=智利 CGE、巴西 CPFL、菲律宾 NGCP 及三国电力监管与政策；shipping=航运、港口、要道、船舶、运价、船舶融资、造船、制裁船舶）。无关为 false
- title_zh：中文标题，25 字以内
- summary_zh：一句话中文摘要，60 字以内，写清谁、做了什么、影响什么
- type：从 {EVENT_TYPES} 里选一个
- level：r / y / g，AI 初判
  · energy：r=可能影响股权、特许权或重大财务结果，或直接点名中资股东；y=影响经营或收益但有时间应对；g=一般动态
  · shipping：r=要道封锁或袭船导致航线中断、新制裁或港口费等重大政策即时生效、重大违约；y=运价大幅波动、政策草案、地缘升级、重要融资或买卖船交易；g=一般动态
- persona：仅 shipping 填，数组，可多选 dry（干散货·海岬型）/ box（集运与货代）/ tank（油轮），都不相关则空数组
- country：energy 填 智利 / 巴西 / 菲律宾 / 其他；shipping 留空字符串
- reason_zh：分级理由，30 字以内
- unverified：如果标题本身是传闻、未经证实或单一匿名消息，为 true

只输出 JSON：{{"items":[{{...}}]}}"""


def ai_batch(batch: list[dict]) -> dict[int, dict]:
    lines = []
    for k, it in enumerate(batch):
        lines.append(json.dumps({"i": k, "sector": it["sector"], "source": it.get("outlet") or it["source"],
                                 "title": it["title"], "snippet": it.get("snippet", "")[:300]}, ensure_ascii=False))
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                     {"role": "user", "content": "\n".join(lines)}],
        "response_format": {"type": "json_object"},
        "temperature": 0.2,
    }
    for attempt in range(3):
        try:
            r = session.post(DEEPSEEK_URL, json=payload, timeout=120,
                             headers={"Authorization": f"Bearer {DEEPSEEK_KEY}"})
            r.raise_for_status()
            content = r.json()["choices"][0]["message"]["content"]
            data = json.loads(content)
            return {int(x["i"]): x for x in data.get("items", []) if "i" in x}
        except Exception as e:
            print(f"  AI 批次失败（第 {attempt + 1} 次）：{str(e)[:160]}", flush=True)
            time.sleep(4 * (attempt + 1))
    return {}


def analyze(items: list[dict], ts: datetime) -> int:
    if not DEEPSEEK_KEY:
        print("未设置 DEEPSEEK_API_KEY，跳过 AI 摘要与分级")
        return 0
    recent = ts - timedelta(days=3)
    todo = [it for it in items if "ai" not in it and (parse_iso(it.get("published")) or ts) >= recent][:MAX_AI]
    done = 0
    for k in range(0, len(todo), AI_BATCH):
        batch = todo[k:k + AI_BATCH]
        res = ai_batch(batch)
        for j, it in enumerate(batch):
            r = res.get(j)
            if not r:
                continue
            lv = r.get("level") if r.get("level") in ("r", "y", "g") else "g"
            it["ai"] = {
                "relevant": bool(r.get("relevant", True)),
                "title_zh": clean(r.get("title_zh", ""), 60),
                "summary_zh": clean(r.get("summary_zh", ""), 160),
                "type": r.get("type") if r.get("type") in EVENT_TYPES else "运营与市场",
                "level": lv,
                "reason_zh": clean(r.get("reason_zh", ""), 80),
                "unverified": bool(r.get("unverified", False)),
                "model": DEEPSEEK_MODEL, "at": iso(ts),
            }
            if it["sector"] == "shipping":
                ps = [p for p in (r.get("persona") or []) if p in ("dry", "box", "tank")]
                it["persona"] = sorted(set(ps) | set(it.get("persona") or []))
            elif r.get("country") and not it.get("country"):
                it["country"] = r["country"]
            done += 1
        print(f"  AI 已处理 {min(k + AI_BATCH, len(todo))}/{len(todo)}", flush=True)
    return done


# ---------------- 主流程 ----------------

def main() -> int:
    ts = now_utc()
    cfg = json.loads(SOURCES.read_text(encoding="utf-8"))
    sources = [s for s in cfg["sources"] if not s.get("disabled")]
    old = {}
    if FEED.exists():
        try:
            old = json.loads(FEED.read_text(encoding="utf-8"))
        except ValueError:
            print("旧 feed.json 损坏，重新开始")
    new_items, health, chokepoints = collect(sources)
    if not chokepoints:
        chokepoints = old.get("chokepoints", [])  # PortWatch 偶尔失败时保留上一次数据
    items, added = merge(old.get("items", []), new_items, ts)
    n_ai = analyze(items, ts)

    show_cut = ts - timedelta(days=SHOW_DAYS)
    shown = [it for it in items if (parse_iso(it.get("published")) or ts) >= show_cut
             and it.get("ai", {}).get("relevant", True)]
    lv = lambda l: sum(1 for it in shown if it.get("ai", {}).get("level") == l)
    feed = {
        "generated_at": iso(ts),
        "ai": {"enabled": bool(DEEPSEEK_KEY), "model": DEEPSEEK_MODEL if DEEPSEEK_KEY else None, "processed_this_run": n_ai},
        "stats": {
            "sources_total": len(health), "sources_ok": sum(1 for h in health if h["ok"]),
            "fetched_this_run": len(new_items), "items_shown": len(shown),
            "new_this_run": added,
            "red": lv("r"), "yellow": lv("y"), "green": lv("g"),
            "unclassified": sum(1 for it in shown if "ai" not in it),
        },
        "health": health,
        "chokepoints": chokepoints,
        "items": items,
    }
    FEED.parent.mkdir(parents=True, exist_ok=True)
    tmp = FEED.with_suffix(".tmp")
    tmp.write_text(json.dumps(feed, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(FEED)
    s = feed["stats"]
    print(f"完成：信源 {s['sources_ok']}/{s['sources_total']} 可用，本次抓取 {s['fetched_this_run']} 条，新增 {s['new_this_run']} 条，"
          f"展示 {s['items_shown']} 条（红 {s['red']} 黄 {s['yellow']} 绿 {s['green']} 未分级 {s['unclassified']}）")
    return 0 if s["sources_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
