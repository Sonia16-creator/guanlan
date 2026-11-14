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
SIMS = ROOT / "data" / "sims.json"                   # 局势推演档案：长期保留，不随 feed 过期

KEEP_DAYS = int(os.getenv("KEEP_DAYS", "14"))        # 库里保留多少天
SHOW_DAYS = int(os.getenv("SHOW_DAYS", "7"))         # 网页展示多少天
LOOKBACK = os.getenv("LOOKBACK", "2d")               # 每次向搜索类信源要多久的新闻
PER_SOURCE = int(os.getenv("PER_SOURCE", "40"))      # 每个信源最多取几条
MAX_AI = int(os.getenv("MAX_AI", "300"))             # 每次最多送多少条给 AI，控制成本
AI_BATCH = 10
MAX_SIM = int(os.getenv("MAX_SIM", "20"))         # 每次最多给多少条红色事件生成局势推演
MAX_REVIEW = int(os.getenv("MAX_REVIEW", "10"))   # 每次最多对账多少条到期推演
GH_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()   # GitHub Actions 自动提供，用来开「对账单」Issue
GH_REPO = os.getenv("GITHUB_REPOSITORY", "")
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


def fetch_gnews(q: str, lang: str, lookback: str | None = None) -> list[dict]:
    hl, gl, ceid = GNEWS_LANG.get(lang, GNEWS_LANG["en"])
    url = f"https://news.google.com/rss/search?q={quote_plus(q + ' when:' + (lookback or LOOKBACK))}&hl={hl}&gl={gl}&ceid={ceid}"
    items = fetch_rss(url)
    for it in items:
        it["snippet"] = ""  # Google News 的摘要只是重复标题
    return items


_last_gdelt = 0.0


def fetch_gdelt(q: str) -> list[dict]:
    global _last_gdelt
    wait = 10 - (time.time() - _last_gdelt)  # GDELT 要求两次请求间隔 5 秒以上，留足余量
    if wait > 0:
        time.sleep(wait)
    _last_gdelt = time.time()
    url = ("https://api.gdeltproject.org/api/v2/doc/doc?mode=artlist&format=json&sort=datedesc"
           f"&maxrecords={PER_SOURCE}&timespan={LOOKBACK}&query={quote_plus(q)}")
    # GDELT 自 2026-11 起限速很严（实测间隔 7 秒仍常返回 429，与 IP、UA 无关），只当补充信源：
    # 被限速就等 30 秒、60 秒各再试一次，仍不行就跳过，同类话题由 Google News 检索覆盖。
    r = None
    for attempt, backoff in enumerate([30, 60, 0]):
        try:
            r = session.get(url, timeout=TIMEOUT)
            if r.status_code != 429 and r.status_code < 500:
                break
            reason = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            reason = type(e).__name__
        if not backoff:
            break
        print(f"  GDELT {reason}，{backoff} 秒后重试（第 {attempt + 2} 次）", flush=True)
        time.sleep(backoff)
        _last_gdelt = time.time()
    if r is None:
        raise ValueError("GDELT 连续 3 次请求失败")
    if r.status_code == 429:
        raise ValueError("GDELT 限速（429），本次跳过，下次运行再试；同类话题有 Google News 覆盖")
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
    def day(v):  # 接口的日期字段曾是毫秒时间戳，现在是 "YYYY-MM-DD" 字符串，两种都兼容
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        return str(v)[:10]

    rows: dict[str, list[dict]] = {}
    for f in feats:
        a = f.get("attributes", {})
        if a.get("date") is not None:
            a["date"] = day(a["date"])
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
            "latest": rs[0]["date"],
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
                             layer=s.get("layer"), official=bool(s.get("official")),
                             asset=[s["asset"]] if s.get("asset") else [])
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
- relevant：与所属子目录是否相关，无关为 false
  · energy=智利 CGE、巴西 CPFL、菲律宾 NGCP 及三国电力监管与政策
  · shipping=航运、港口、要道、船舶、运价、船舶融资、造船、制裁船舶
  · securities=对 A 股、港股、美股、韩股、日股有影响的宏观政策、央行与金融监管、重要公司与行业事件
  · commodity=影响原油、黄金、白银、铜、LNG 价格或供需的事件
- title_zh：中文标题，25 字以内
- summary_zh：一句话中文摘要，60 字以内，写清谁、做了什么、影响什么
- type：从 {EVENT_TYPES} 里选一个
- level：r / y / g，AI 初判
  · energy：r=可能影响股权、特许权或重大财务结果，或直接点名中资股东；y=影响经营或收益但有时间应对；g=一般动态
  · shipping：r=要道封锁或袭船导致航线中断、新制裁或港口费等重大政策即时生效、重大违约；y=运价大幅波动、政策草案、地缘升级、重要融资或买卖船交易；g=一般动态
  · securities：r=超预期的利率或货币政策决定、重大监管处罚或市场规则突变、可能引发市场剧烈波动的事件；y=政策信号或表态、重要数据发布、行业政策草案；g=一般动态
  · commodity：r=供给突然中断（产油国减产超预期、要道封锁、主要矿山停产）或价格单日剧烈波动；y=库存与产量数据显著变化、央行购金、政策预期变化；g=一般动态
- persona：仅 shipping 填，数组，可多选 dry（干散货·海岬型）/ box（集运与货代）/ tank（油轮），都不相关则空数组
- asset：仅 commodity 填，数组，可多选 oil / gold / silver / copper / lng，都不相关则空数组
- country：energy 填 智利 / 巴西 / 菲律宾 / 其他；其余子目录留空字符串
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
            elif it["sector"] == "commodity":
                xs = [x for x in (r.get("asset") or []) if x in ("oil", "gold", "silver", "copper", "lng")]
                it["asset"] = sorted(set(xs) | set(it.get("asset") or []))
            elif it["sector"] == "energy" and r.get("country") and not it.get("country"):
                it["country"] = r["country"]
            done += 1
        print(f"  AI 已处理 {min(k + AI_BATCH, len(todo))}/{len(todo)}", flush=True)
    return done


# ---------------- 红色事件局势推演 ----------------

SIM_FOCUS = {
    "energy": "国网海外能源资产（智利 CGE、巴西 CPFL、菲律宾 NGCP）的特许权、股权、电价与监管、融资，以及对中资股东的影响",
    "shipping": "事件 → 通道 → 船型 → 运价与船东股价的传导，涉及干散货、集运、油轮三类船东和经纪人",
    "securities": "对 A 股、港股、美股、韩股、日股相关板块与公司的传导，以及利率、汇率、资金面",
    "commodity": "对原油、黄金、白银、铜、LNG 价格与供需的传导，以及持有这些敞口的企业",
}

SIM_PROMPT = """你是锋睿「观澜 Horizon」舆情雷达的局势推演分析员。输入是一条已被初判为红色的事件。
今天是 {today}。请输出简体中文的局势推演，重点关注：{focus}。

要求：
- 事实只能来自输入的标题和摘要，不要编造数字、人名、日期；输入里没有的写进「待补取」。
- 「已知事实」只写事件本身的内容，不要写「由某媒体报道」「发布时间」「被初判为红色」这类关于信源或系统的说明。
- 推演、历史可比属于分析判断，可以用你的背景知识，但要写得克制、可验证。
- 三种情景互斥，概率为整数且加起来等于 100，按概率从高到低排列。
- 每个情景给出可观察的好转信号和恶化信号，便于业务人员跟踪。

只输出 JSON，结构如下：
{{
 "conclusion": "一句话核心判断，80 字以内",
 "facts": ["已知事实，只来自输入，最多 4 条"],
 "unknowns": ["需要补取核实的信息，最多 4 条"],
 "chain": ["事件", "一级影响", "二级影响", "最终影响到的资产或标的"],
 "affected": "最可能受影响的对象（资产、板块、标的或敞口），50 字以内",
 "comparable": {{"case": "最接近的历史可比事件，没有就写空字符串", "lesson": "可借鉴的规律，60 字以内"}},
 "scenarios": [
  {{"name": "情景名，10 字以内", "prob": 50, "trigger": "触发条件", "impact": "影响", "window": "时间窗口", "better": "好转信号", "worse": "恶化信号"}}
 ],
 "watch": [{{"what": "要跟踪的验证节点", "when": "时间或频率", "good": "好转信号", "bad": "恶化信号"}}],
 "review_by": "到期对账日 YYYY-MM-DD：到这一天应该能看出哪种情景成立，按最可能情景的时间窗口估计，不早于今天后 7 天、不晚于 1 年",
 "track_query": "到期时用来检索后续进展的新闻关键词：用事件发生地的语言或英文，8 个词以内，可用 OR 和英文引号，必须包含事件主体名称",
 "track_lang": "检索语言，只能是 en / en-PH / es / pt / zh / ko 之一"
}}"""


def _txt(v, n):
    return clean(str(v or ""), n)


def simulate_one(it: dict, ts: datetime) -> dict | None:
    a = it.get("ai", {})
    focus = SIM_FOCUS.get(it["sector"], SIM_FOCUS["securities"])
    event = {"sector": it["sector"], "country": it.get("country") or "", "source": it.get("outlet") or it["source"],
             "published": it.get("published"), "title": it["title"], "title_zh": a.get("title_zh", ""),
             "summary_zh": a.get("summary_zh", ""), "snippet": it.get("snippet", "")[:400],
             "red_reason": a.get("reason_zh", "")}
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [{"role": "system", "content": SIM_PROMPT.format(today=ts.strftime("%Y-%m-%d"), focus=focus)},
                     {"role": "user", "content": json.dumps(event, ensure_ascii=False)}],
        "response_format": {"type": "json_object"},
        "temperature": 0.3,
    }
    for attempt in range(2):
        try:
            r = session.post(DEEPSEEK_URL, json=payload, timeout=180,
                             headers={"Authorization": f"Bearer {DEEPSEEK_KEY}"})
            r.raise_for_status()
            d = json.loads(r.json()["choices"][0]["message"]["content"])
            break
        except Exception as e:
            print(f"  推演失败（第 {attempt + 1} 次）：{str(e)[:160]}", flush=True)
            time.sleep(5)
    else:
        return None
    scen = []
    for x in (d.get("scenarios") or [])[:3]:
        try:
            prob = max(0, min(100, int(round(float(x.get("prob", 0))))))
        except (TypeError, ValueError):
            prob = 0
        scen.append({"name": _txt(x.get("name"), 30), "prob": prob, "trigger": _txt(x.get("trigger"), 160),
                     "impact": _txt(x.get("impact"), 160), "window": _txt(x.get("window"), 40),
                     "better": _txt(x.get("better"), 100), "worse": _txt(x.get("worse"), 100)})
    if not scen or not d.get("conclusion"):
        return None
    total = sum(x["prob"] for x in scen)
    if total and total != 100:  # 概率归一到 100
        for x in scen:
            x["prob"] = round(x["prob"] * 100 / total)
    scen.sort(key=lambda x: -x["prob"])
    comp = d.get("comparable") or {}
    return {
        "conclusion": _txt(d.get("conclusion"), 160),
        "facts": [_txt(x, 120) for x in (d.get("facts") or [])[:4] if x],
        "unknowns": [_txt(x, 120) for x in (d.get("unknowns") or [])[:4] if x],
        "chain": [_txt(x, 40) for x in (d.get("chain") or [])[:5] if x],
        "affected": _txt(d.get("affected"), 100),
        "comparable": {"case": _txt(comp.get("case"), 80), "lesson": _txt(comp.get("lesson"), 120)},
        "scenarios": scen,
        "watch": [{"what": _txt(w.get("what"), 80), "when": _txt(w.get("when"), 30),
                   "good": _txt(w.get("good"), 80), "bad": _txt(w.get("bad"), 80)}
                  for w in (d.get("watch") or [])[:4] if isinstance(w, dict) and w.get("what")],
        "review_by": _review_by(d.get("review_by"), ts),
        "track_query": _txt(d.get("track_query"), 120),
        "track_lang": d.get("track_lang") if d.get("track_lang") in GNEWS_LANG else "en",
        "model": DEEPSEEK_MODEL, "at": iso(ts),
    }


def _review_by(v, ts: datetime) -> str:
    """对账日限定在 7 天到 1 年之间；模型没给或格式不对时默认 30 天后。"""
    lo, hi = ts + timedelta(days=7), ts + timedelta(days=365)
    try:
        d = datetime.strptime(str(v)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        d = ts + timedelta(days=30)
    return min(max(d, lo), hi).strftime("%Y-%m-%d")


ARCHIVE_KEYS = ("id", "title", "link", "outlet", "source", "sector", "country", "persona", "asset", "official", "published", "ai", "sim")


def archive_sims(items: list[dict], ts: datetime) -> int:
    """把生成过推演的事件收进 data/sims.json。档案只增不删，对账结果（status/outcome）由人工或后续流程回填。"""
    arc = {"items": []}
    if SIMS.exists():
        try:
            arc = json.loads(SIMS.read_text(encoding="utf-8"))
        except ValueError:
            print("sims.json 损坏，未写入档案（请检查文件）")
            return 0
    have = {x["id"] for x in arc["items"]}
    seq = max((int(x["no"].split("-")[1]) for x in arc["items"] if x.get("no", "").startswith("A-")), default=0)
    added = 0
    for it in sorted((i for i in items if i.get("sim") and i["id"] not in have), key=lambda i: i["sim"].get("at", "")):
        seq += 1
        rec = json.loads(json.dumps({k: it[k] for k in ARCHIVE_KEYS if k in it}))  # 深拷贝，档案独立于 feed
        if not rec["sim"].get("review_by"):  # 早期推演没有对账日：默认推演后 30 天
            rec["sim"]["review_by"] = _review_by(None, parse_iso(rec["sim"].get("at")) or ts)
        rec.update(no=f"A-{seq:04d}", archived_at=iso(ts), status="open", outcome="")
        arc["items"].append(rec)
        added += 1
    arc["updated_at"] = iso(ts)
    SIMS.parent.mkdir(parents=True, exist_ok=True)
    tmp = SIMS.with_suffix(".tmp")
    tmp.write_text(json.dumps(arc, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(SIMS)
    return added


def simulate(items: list[dict], ts: datetime) -> int:
    if not DEEPSEEK_KEY:
        return 0
    recent = ts - timedelta(days=3)
    todo = [it for it in items if it.get("ai", {}).get("level") == "r" and it["ai"].get("relevant", True)
            and "sim" not in it and (parse_iso(it.get("published")) or ts) >= recent][:MAX_SIM]
    done = 0
    for k, it in enumerate(todo, 1):
        res = simulate_one(it, ts)
        if res:
            it["sim"] = res
            done += 1
        print(f"  推演 {k}/{len(todo)}：{'完成' if res else '失败'} · {it['ai'].get('title_zh') or it['title'][:40]}", flush=True)
    return done


# ---------------- 推演到期对账：AI 初判 + 人工在 GitHub Issue 上确认 ----------------
#
# 流程：推演到了 review_by 那天 → 用 track_query 检索这段时间的新闻 → DeepSeek 判断哪种情景发生
#   · 情景 A（概率最高）发生 = 成立 hit；B 或 C 发生 = 未成立 miss；证据不足 = 顺延 30 天，最多顺延 2 次，之后记为无法判定 unclear
#   · 每条判断自动开一个「对账单」Issue。负责人给 Issue 加标签：
#       确认 → 同意 AI 判断；改判-成立 / 改判-未成立 / 改判-无法判定 → 以人工为准
#   · 下次运行读到标签后写回 sims.json（confirmed=true），并关闭 Issue
# 档案只增不删、判断错了也留痕：这是观澜可被验证的命中率记录。

REVIEW_PROMPT = """你是锋睿「观澜 Horizon」的推演对账员。输入是一条历史推演（三种情景）和推演之后的新闻证据。
请只依据证据判断实际走向更接近哪种情景，不要用证据以外的信息。

判定规则：
- 证据清楚表明情景 A 的触发条件或好转/恶化信号已出现 → verdict = hit，scenario = A
- 证据清楚表明实际走向是情景 B 或 C → verdict = miss，scenario = B 或 C
- 证据不足、与事件无关、或局势仍未明朗 → verdict = unclear，scenario = none

只输出 JSON：{"verdict": "hit/miss/unclear", "scenario": "A/B/C/none", "explain": "中文判断理由，120 字以内，点明依据的证据序号", "evidence": [证据序号, 最多 5 个]}"""

GH_LABELS = {"对账": "1d4ed8", "确认": "15803d", "改判-成立": "15803d", "改判-未成立": "b91c1c", "改判-无法判定": "6b7280"}
_labels_ready = False


def gh(method: str, path: str, **kw):
    r = session.request(method, f"https://api.github.com/repos/{GH_REPO}{path}", timeout=TIMEOUT,
                        headers={"Authorization": f"Bearer {GH_TOKEN}", "Accept": "application/vnd.github+json"}, **kw)
    if r.status_code >= 400 and not (method == "POST" and path == "/labels" and r.status_code == 422):
        raise ValueError(f"GitHub {method} {path} → {r.status_code} {r.text[:120]}")
    return r.json() if r.content else {}


def gh_ready() -> bool:
    global _labels_ready
    if not (GH_TOKEN and GH_REPO):
        return False
    if not _labels_ready:
        for name, color in GH_LABELS.items():
            gh("POST", "/labels", json={"name": name, "color": color})
        _labels_ready = True
    return True


VERDICT_ZH = {"hit": "成立", "miss": "未成立", "unclear": "无法判定"}


def open_review_issue(rec: dict) -> int | None:
    if not gh_ready():
        return None
    m, rv, a = rec["sim"], rec["review"], rec.get("ai", {})
    scen = "\n".join(f"| {'ABC'[i]} · {x['name']} | {x['prob']}% | {x['trigger']} |" for i, x in enumerate(m.get("scenarios", [])))
    ev = "\n".join(f"{k + 1}. [{e['title']}]({e['link']}) · {e.get('outlet') or ''} · {(e.get('published') or '')[:10]}"
                   for k, e in enumerate(rv.get("evidence", []))) or "（没有检索到相关报道）"
    body = f"""**推演 {rec.get('no')}** · {a.get('title_zh') or rec['title']}
原文：{rec.get('link')}
推演日期：{m.get('at', '')[:10]} · 对账日：{m.get('review_by')}

**核心判断**：{m.get('conclusion')}

| 情景 | 概率 | 触发条件 |
| --- | --- | --- |
{scen}

### AI 对账初判：{VERDICT_ZH[rv['verdict']]}（实际更接近情景 {rv.get('scenario', 'none')}）
{rv.get('explain', '')}

**证据**（检索词：`{rv.get('query') or ''}`）
{ev}

---
**请负责人确认**：给这个 Issue 加一个标签即可，下次自动采集时写回档案并关闭本单。
- `确认`：同意 AI 初判
- `改判-成立` / `改判-未成立` / `改判-无法判定`：以人工判断为准（可在评论里写理由）
"""
    title = f"[对账] {rec.get('no')} {a.get('title_zh') or rec['title'][:40]} · AI 初判：{VERDICT_ZH[rv['verdict']]}"
    return gh("POST", "/issues", json={"title": title, "body": body, "labels": ["对账"]}).get("number")


def sync_confirmations(arc: dict) -> int:
    """读取对账单上的人工标签，写回档案。"""
    if not gh_ready():
        return 0
    n = 0
    for rec in arc["items"]:
        num = (rec.get("review") or {}).get("issue")
        if not num or rec.get("confirmed"):
            continue
        try:
            iss = gh("GET", f"/issues/{num}")
        except Exception as e:
            print(f"  读取对账单 #{num} 失败：{e}", flush=True)
            continue
        labels = {l["name"] for l in iss.get("labels", [])}
        new = None
        for lab, st in (("改判-成立", "hit"), ("改判-未成立", "miss"), ("改判-无法判定", "unclear")):
            if lab in labels:
                new = st
        if new is None and "确认" not in labels:
            continue
        if new and new != rec.get("status"):
            rec["status"], rec["override"] = new, True
        rec["confirmed"], rec["confirmed_at"] = True, iso(now_utc())
        try:
            gh("POST", f"/issues/{num}/comments", json={"body": f"已写回档案：{VERDICT_ZH.get(rec['status'], rec['status'])}（{'人工改判' if rec.get('override') else '同意 AI 初判'}）。"})
            gh("PATCH", f"/issues/{num}", json={"state": "closed"})
        except Exception as e:
            print(f"  关闭对账单 #{num} 失败：{e}", flush=True)
        n += 1
    return n


def collect_evidence(rec: dict, ts: datetime) -> tuple[str, list[dict]]:
    m = rec["sim"]
    # 没有跟踪检索词的旧推演，退回用原文标题（去掉「 - 媒体名」后缀）检索
    q = m.get("track_query") or re.sub(r"\s+[-–|]\s+[^-–|]{2,60}$", "", rec["title"])[:80]
    start = parse_iso(m.get("at")) or ts - timedelta(days=30)
    days = max(7, min(365, (ts - start).days + 1))
    try:
        got = fetch_gnews(q, m.get("track_lang") or "en", lookback=f"{days}d")
    except Exception as e:
        print(f"  对账检索失败：{e}", flush=True)
        return q, []
    got = [g for g in got if (parse_iso(g.get("published")) or ts) >= start and g["link"] != rec.get("link")]
    got.sort(key=lambda g: g.get("published") or "", reverse=True)
    return q, got[:15]


def judge(rec: dict, evidence: list[dict]) -> dict | None:
    m = rec["sim"]
    inp = {"event": rec.get("ai", {}).get("title_zh") or rec["title"], "simulated_at": m.get("at", "")[:10],
           "conclusion": m.get("conclusion"),
           "scenarios": [{"id": "ABC"[i], "name": x["name"], "prob": x["prob"], "trigger": x["trigger"],
                          "better": x["better"], "worse": x["worse"]} for i, x in enumerate(m.get("scenarios", []))],
           "evidence": [{"n": k + 1, "title": e["title"], "outlet": e.get("outlet"), "date": (e.get("published") or "")[:10]}
                        for k, e in enumerate(evidence)]}
    if not evidence:
        return {"verdict": "unclear", "scenario": "none", "explain": "未检索到推演之后的相关报道。", "evidence": []}
    payload = {"model": DEEPSEEK_MODEL, "temperature": 0.1, "response_format": {"type": "json_object"},
               "messages": [{"role": "system", "content": REVIEW_PROMPT},
                            {"role": "user", "content": json.dumps(inp, ensure_ascii=False)}]}
    try:
        r = session.post(DEEPSEEK_URL, json=payload, timeout=120, headers={"Authorization": f"Bearer {DEEPSEEK_KEY}"})
        r.raise_for_status()
        d = json.loads(r.json()["choices"][0]["message"]["content"])
    except Exception as e:
        print(f"  对账判断失败：{str(e)[:160]}", flush=True)
        return None
    v = d.get("verdict") if d.get("verdict") in VERDICT_ZH else "unclear"
    sc = d.get("scenario") if d.get("scenario") in ("A", "B", "C") else "none"
    if v == "hit" and sc != "A":
        v = "miss" if sc in ("B", "C") else "unclear"
    idx = [i for i in (d.get("evidence") or []) if isinstance(i, int) and 1 <= i <= len(evidence)][:5]
    return {"verdict": v, "scenario": sc, "explain": _txt(d.get("explain"), 200), "evidence": idx}


def review_due(ts: datetime) -> tuple[int, int]:
    if not SIMS.exists():
        return 0, 0
    arc = json.loads(SIMS.read_text(encoding="utf-8"))
    synced = sync_confirmations(arc)
    today = ts.strftime("%Y-%m-%d")
    todo = [r for r in arc["items"] if r.get("status", "open") == "open" and (r.get("sim") or {}).get("review_by", "9999") <= today]
    done = 0
    for rec in (todo[:MAX_REVIEW] if DEEPSEEK_KEY else []):
        q, ev = collect_evidence(rec, ts)
        res = judge(rec, ev)
        if not res:
            continue
        rnd = (rec.get("review") or {}).get("round", 0) + 1
        rec["review"] = {"verdict": res["verdict"], "scenario": res["scenario"], "explain": res["explain"], "round": rnd,
                         "query": q, "at": iso(ts), "model": DEEPSEEK_MODEL,
                         "evidence": [{k: ev[i - 1].get(k) for k in ("title", "link", "outlet", "published")} for i in res["evidence"]]
                                     or [{k: e.get(k) for k in ("title", "link", "outlet", "published")} for e in ev[:3]]}
        if res["verdict"] == "unclear" and rnd < 3:
            new_by = (datetime.strptime(rec["sim"]["review_by"], "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")
            rec["review"]["note"] = f"证据不足，对账日由 {rec['sim']['review_by']} 顺延至 {new_by}"
            rec["sim"]["review_by"] = new_by
        else:
            rec["status"], rec["confirmed"] = res["verdict"], False
            try:
                rec["review"]["issue"] = open_review_issue(rec)
            except Exception as e:
                print(f"  开对账单失败：{e}", flush=True)
        done += 1
        print(f"  对账 {rec.get('no')}：{VERDICT_ZH[res['verdict']]}（证据 {len(ev)} 条）", flush=True)
    arc["updated_at"] = iso(ts)
    tmp = SIMS.with_suffix(".tmp")
    tmp.write_text(json.dumps(arc, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(SIMS)
    return done, synced


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
    n_sim = simulate(items, ts)
    n_arc = archive_sims(items, ts)
    print(f"局势推演：本次生成 {n_sim} 条，新归档 {n_arc} 条")
    try:
        n_rev, n_conf = review_due(ts)
        print(f"推演对账：本次 AI 对账 {n_rev} 条，写回人工确认 {n_conf} 条")
    except Exception as e:  # 对账出错不影响采集结果
        print(f"推演对账出错：{type(e).__name__}: {str(e)[:160]}")

    show_cut = ts - timedelta(days=SHOW_DAYS)
    shown = [it for it in items if (parse_iso(it.get("published")) or ts) >= show_cut
             and it.get("ai", {}).get("relevant", True)]
    lv = lambda l: sum(1 for it in shown if it.get("ai", {}).get("level") == l)
    feed = {
        "generated_at": iso(ts),
        "ai": {"enabled": bool(DEEPSEEK_KEY), "model": DEEPSEEK_MODEL if DEEPSEEK_KEY else None, "processed_this_run": n_ai, "simulated_this_run": n_sim},
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
