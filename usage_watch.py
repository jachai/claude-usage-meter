#!/usr/bin/env python3
"""
Claude Code usage-pace watcher.

Reads ~/.claude/projects/**/*.jsonl incrementally (byte offsets), keeps a rolling
per-minute cost index, and alerts the moment the burn rate leaves normal range.

Zero LLM calls. Pure stdlib. Costs nothing to run.

Verbs:
  once        one poll cycle (what launchd runs)
  status      print current pace + top sessions, no alerting
  top         per-session breakdown for a window (--mins N)
  test        fire a synthetic CRITICAL alert to verify delivery
  install     write + load the launchd agent (every 2 min)
  uninstall   unload + remove the launchd agent
  calibrate   re-derive thresholds from your own history
"""
import argparse, collections, datetime, fcntl, glob, json, os, re, subprocess, sys, time

HOME = os.path.expanduser("~")
ROOT = os.path.join(HOME, ".claude", "projects")
DIR = os.path.join(HOME, ".claude", "usage-watch")
STATE = os.path.join(DIR, "state.json")
CONFIG = os.path.join(DIR, "config.json")
ALERT = os.path.join(DIR, "ALERT.txt")          # full alert text
PACE = os.path.join(DIR, "pace.txt")            # tiny one-liner the statusline reads
LOG = os.path.join(DIR, "alerts.log")
LABEL = "com.claude.usage-watch"
PLIST = os.path.join(HOME, "Library", "LaunchAgents", LABEL + ".plist")
MLABEL = "com.claude.usage-meter"
MPLIST = os.path.join(HOME, "Library", "LaunchAgents", MLABEL + ".plist")
METER_PORT = 7654

# Thresholds derived from this machine's own history (2026-07-20 .. 2026-09-08):
#   median active hour $30/hr | p90 $191 | p95 $277 | p99 $676 | max $1896
#   $600/hr fired 11x in 7 weeks - ALL 11 on the Sep 3-4 drain, 0 false positives.
DEFAULTS = {
    "weekly_budget": 9000,        # API-$-equiv. Highest week that did NOT exhaust the plan was $9,163; the week that did was $17,184.
    "week_starts": "monday",      # or "sunday", or "YYYY-MM-DD" for a rolling 7d anchor
    "fast_window_mins": 10,
    "slow_window_mins": 60,
    # $/hr, evaluated on both windows; severity = worst tier any window trips
    "fast_notice": 450, "fast_high": 750, "fast_crit": 1100,
    "slow_notice": 300, "slow_high": 450, "slow_crit": 600,
    # weekly-budget pace tiers (% of budget consumed vs % of week elapsed)
    "budget_notice": 1.30, "budget_high": 1.60, "budget_crit": 1.90,
    "budget_floor_pct": 0.25,     # ignore pace ratios until 25% of budget is spent
    "cooldown_mins": {"NOTICE": 60, "HIGH": 20, "CRITICAL": 10},
    "retain_days": 9,
    "sound": True,
    "speak": False,
    "modal_on_critical": True,    # blocking dialog you cannot miss
    "slack_webhook": "",
    "auto_stop_on_critical": False,   # DANGEROUS: SIGTERM the worst session. Off by default.
    "quiet_hours": [],            # e.g. [[1,7]] to mute NOTICE between 01:00-07:00 local
}
TIERS = ["OK", "NOTICE", "HIGH", "CRITICAL"]
AGENT_RE = re.compile(r"/subagents/agent-([0-9a-zA-Z]+)\.jsonl$")
RECOMMENDED_AGENTS = 3      # concurrent subagents; see orchestrate-plan-implementation


def load(path, default):
    try:
        with open(path) as f:
            d = json.load(f)
        return d if isinstance(d, dict) else default
    except Exception:
        return default


def save(path, obj):
    """Atomic write. The temp name MUST be unique per process: a shared '<path>.tmp'
    lets two concurrent writers rename each other's file away, and the loser dies with
    FileNotFoundError having persisted nothing."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(obj, f)
        os.replace(tmp, path)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


class ScanLock:
    """Advisory single-writer lock. Readers that cannot get it fall back to the
    last persisted state rather than racing a concurrent scan."""

    def __init__(self, timeout=8.0):
        self.timeout, self.fh, self.held = timeout, None, False

    def __enter__(self):
        os.makedirs(DIR, exist_ok=True)
        try:
            self.fh = open(os.path.join(DIR, "scan.lock"), "w")
        except OSError:
            return False
        deadline = time.time() + self.timeout
        while True:
            try:
                fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.held = True
                return True
            except OSError:
                if time.time() >= deadline:
                    return False
                time.sleep(0.15)

    def __exit__(self, *a):
        if self.fh:
            if self.held:
                try:
                    fcntl.flock(self.fh, fcntl.LOCK_UN)
                except OSError:
                    pass
            self.fh.close()
        return False


def save_meta(meta):
    """Persist small top-level keys without rewriting buckets - re-reads under the
    lock so a concurrent scan's data is never clobbered."""
    with ScanLock(5.0) as got:
        if not got:
            return False
        st = load(STATE, {})
        st.update(meta)
        save(STATE, st)
        return True


def refresh(c, timeout=8.0):
    """Load state, and scan+persist if we own the writer lock. Returns (state, scanned)."""
    lock = ScanLock(timeout)
    got = lock.__enter__()
    try:
        st = load(STATE, {})
        if not got:
            return st, False
        scan(st, c["retain_days"])
        save(STATE, st)
        return st, True
    finally:
        lock.__exit__()


def cfg():
    c = dict(DEFAULTS)
    c.update(load(CONFIG, {}))
    if not os.path.exists(CONFIG):
        save(CONFIG, DEFAULTS)
    st = load(STATE, {})
    ws = week_start(c)
    good = [o for o in st.get("syncs", [])
            if o.get("wstart") == ws and (o.get("pct") or 0) >= 5]
    if not good:
        c["_sync_note"] = ("budget is still LOW-CONFIDENCE - when the real meter reads "
                           "20-40%, run:  usage_watch.py sync --pct <N> --apply")
    return c


def price(model, inp, cw, cr, out, ctx):
    """API-$ equivalent. Mirrors ~/.claude/scripts/claude_usage_audit.py exactly."""
    m = model or ""
    i, o = (3, 15) if "sonnet" in m else (1, 5) if "haiku" in m else (15, 75)
    big = ctx > 200_000
    return ((inp * i + cw * i * 1.25 + cr * i * 0.1) / 1e6 * (2 if big else 1)
            + out * o / 1e6 * (1.5 if big else 1))


# ---------------------------------------------------------------- ingest

def scan(state, retain_days):
    """Incrementally read new assistant records. Returns count of new calls."""
    files = state.setdefault("files", {})
    buckets = state.setdefault("buckets", {})   # "minute_epoch|sid" -> row
    seen = state.setdefault("seen", {})         # dedupe key -> ts
    horizon = time.time() - retain_days * 86400
    new = 0

    for path in glob.glob(ROOT + "/**/*.jsonl", recursive=True):
        try:
            st = os.stat(path)
        except OSError:
            continue
        rec = files.get(path)
        off = 0
        if rec and rec.get("ino") == st.st_ino and rec.get("off", 0) <= st.st_size:
            off = rec["off"]
        if off == st.st_size:
            continue
        if st.st_mtime < horizon and off == 0:
            files[path] = {"ino": st.st_ino, "off": st.st_size}
            continue
        proj = os.path.relpath(path, ROOT).split(os.sep)[0]
        am = AGENT_RE.search(path)
        agent_id = am.group(1) if am else None
        if agent_id and agent_id not in state.setdefault("agent_info", {}):
            info = {}
            try:
                with open(path[:-6] + ".meta.json") as mf:
                    md = json.load(mf)
                info = {"t": md.get("agentType", "?"), "m": md.get("model", "?"),
                        "d": md.get("spawnDepth", 1),
                        "w": (md.get("description") or "")[:60]}
            except Exception:
                info = {"t": "?", "m": "?", "d": 1, "w": ""}
            state["agent_info"][agent_id] = info
        # binary: byte offsets must not land mid-codepoint on a text-mode seek
        try:
            with open(path, "rb") as f:
                f.seek(off)
                raw = f.read()
        except OSError:
            continue
        cut = raw.rfind(b"\n")
        if cut == -1:
            files[path] = {"ino": st.st_ino, "off": off}
            continue
        consumed = raw[:cut + 1]
        files[path] = {"ino": st.st_ino, "off": off + len(consumed)}

        for bline in consumed.split(b"\n"):
            if not bline:
                continue
            line = bline.decode("utf-8", errors="replace")
            if '"usage"' not in line or '"assistant"' not in line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("type") != "assistant":
                continue
            msg = d.get("message") or {}
            u = msg.get("usage")
            if not u:
                continue
            key = f"{msg.get('id')}|{d.get('requestId')}"
            if key in seen:
                continue
            ts = d.get("timestamp")
            if not ts:
                continue
            try:
                t = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
            except Exception:
                continue
            if t < horizon:
                continue
            seen[key] = t
            inp = u.get("input_tokens", 0) or 0
            cw = u.get("cache_creation_input_tokens", 0) or 0
            cr = u.get("cache_read_input_tokens", 0) or 0
            out = u.get("output_tokens", 0) or 0
            ctx = inp + cw + cr
            model = msg.get("model", "?")
            sid = d.get("sessionId") or os.path.basename(path)[:-6]
            bk = f"{int(t // 60)}|{sid}"
            b = buckets.get(bk)
            if not b:
                b = buckets[bk] = {"c": 0.0, "n": 0, "s": 0, "g": 0, "cr": 0,
                                   "o": 0, "x": 0, "m": model, "p": proj, "ag": []}
            b["c"] += price(model, inp, cw, cr, out, ctx)
            b["n"] += 1
            b["s"] += 1 if d.get("isSidechain") else 0
            b["g"] += 1 if ctx > 200_000 else 0
            b["cr"] += cr
            b["o"] += out
            b["x"] = max(b["x"], ctx)
            b["m"] = model
            if agent_id:
                ag = b.setdefault("ag", [])
                if agent_id not in ag and len(ag) < 80:
                    ag.append(agent_id)
            new += 1

    # prune
    cutmin = int(horizon // 60)
    for k in [k for k in buckets if int(k.split("|", 1)[0]) < cutmin]:
        del buckets[k]
    for k in [k for k, v in seen.items() if v < horizon]:
        del seen[k]
    for p in [p for p in files if not os.path.exists(p)]:
        del files[p]
    live = set()
    for b in buckets.values():
        live.update(b.get("ag") or [])
    ai = state.get("agent_info", {})
    for k in [k for k in ai if k not in live]:
        del ai[k]
    return new


# ---------------------------------------------------------------- analysis

def window(buckets, mins, now=None):
    """Aggregate the last `mins` minutes. Returns (total_cost, {sid: row})."""
    now = now or time.time()
    lo = int((now - mins * 60) // 60)
    per, total = {}, 0.0
    for k, b in buckets.items():
        mn, sid = k.split("|", 1)
        if int(mn) < lo:
            continue
        r = per.setdefault(sid, {"c": 0.0, "n": 0, "s": 0, "g": 0, "cr": 0,
                                 "o": 0, "x": 0, "m": b["m"], "p": b["p"],
                                 "first": int(mn), "last": int(mn)})
        r["c"] += b["c"]; r["n"] += b["n"]; r["s"] += b["s"]; r["g"] += b["g"]
        r["cr"] += b["cr"]; r["o"] += b["o"]; r["x"] = max(r["x"], b["x"])
        r["m"] = b["m"]; r["p"] = b["p"]
        r["first"] = min(r["first"], int(mn)); r["last"] = max(r["last"], int(mn))
        total += b["c"]
    return total, per


def session_span(buckets, sid):
    ms = [int(k.split("|", 1)[0]) for k in buckets if k.split("|", 1)[1] == sid]
    return (min(ms), max(ms)) if ms else (0, 0)


def week_start(c, now=None):
    """Start of the current plan week. Anchored on LOCAL wall-clock so it survives DST."""
    now = now or time.time()
    ws = str(c.get("week_starts", "monday")).strip()
    if ws.lower() not in ("monday", "sunday"):
        for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                anchor = datetime.datetime.strptime(ws, fmt)
            except ValueError:
                continue
            nowdt = datetime.datetime.fromtimestamp(now)
            weeks = (nowdt - anchor).days // 7
            start = anchor + datetime.timedelta(days=weeks * 7)
            while start > nowdt:
                start -= datetime.timedelta(days=7)
            while start + datetime.timedelta(days=7) <= nowdt:
                start += datetime.timedelta(days=7)
            return start.timestamp()
        ws = "monday"
    ws = ws.lower()
    lt = datetime.datetime.fromtimestamp(now)
    dow = lt.weekday() if ws == "monday" else (lt.weekday() + 1) % 7
    start = lt.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=dow)
    return start.timestamp()


def assess(state, c, now=None):
    """Returns the full pace picture."""
    now = now or time.time()
    b = state.get("buckets", {})
    fw, sw = c["fast_window_mins"], c["slow_window_mins"]
    fast_c, fast_per = window(b, fw, now)
    slow_c, slow_per = window(b, sw, now)
    fast_hr, slow_hr = fast_c * 60.0 / fw, slow_c * 60.0 / sw

    wstart = week_start(c, now)
    wk = sum(v["c"] for k, v in b.items() if int(k.split("|", 1)[0]) * 60 >= wstart)
    budget = max(1.0, float(c["weekly_budget"]))
    elapsed = max(0.02, min(1.0, (now - wstart) / (7 * 86400)))
    spent = wk / budget
    pace = spent / elapsed

    def tier(v, n, h, cr):
        return "CRITICAL" if v >= cr else "HIGH" if v >= h else "NOTICE" if v >= n else "OK"

    t_fast = tier(fast_hr, c["fast_notice"], c["fast_high"], c["fast_crit"])
    t_slow = tier(slow_hr, c["slow_notice"], c["slow_high"], c["slow_crit"])
    t_bud = "OK"
    if spent >= c["budget_floor_pct"]:
        t_bud = tier(pace, c["budget_notice"], c["budget_high"], c["budget_crit"])
    if spent >= 1.0:
        t_bud = "CRITICAL"
    tiermax = max([t_fast, t_slow, t_bud], key=TIERS.index)

    # burn-down: hours of budget left at the sustained rate
    left = max(0.0, budget - wk)
    hrs_left = (left / slow_hr) if slow_hr > 1 else None
    wk_end = wstart + 7 * 86400
    need_hr = left / max(0.5, (wk_end - now) / 3600.0)

    return {
        "now": now, "fast_hr": fast_hr, "slow_hr": slow_hr,
        "fast_c": fast_c, "slow_c": slow_c, "fast_per": fast_per, "slow_per": slow_per,
        "week_spent": wk, "budget": budget, "spent_frac": spent,
        "elapsed_frac": elapsed, "pace": pace,
        "t_fast": t_fast, "t_slow": t_slow, "t_budget": t_bud, "tier": tiermax,
        "hrs_left": hrs_left, "need_hr": need_hr, "wstart": wstart,
        "sessions_active": len([s for s, r in fast_per.items() if r["c"] > 0.5]),
    }


# ---------------------------------------------------------------- diagnosis

def procs():
    try:
        out = subprocess.run(["ps", "-eo", "pid,ppid,etime,command"],
                             capture_output=True, text=True, timeout=10).stdout
    except Exception:
        return []
    return out.splitlines()


def find_pid(sid, plines):
    """Direct hit: the session id appears in the process command line."""
    for ln in plines:
        if sid in ln and "usage_watch" not in ln:
            p = ln.split(None, 3)
            if len(p) >= 4:
                return p[0], p[2]
    return None, None


def transcript(sid):
    hits = glob.glob(os.path.join(ROOT, "*", sid + ".jsonl"))
    return hits[0] if hits else None


def project_cwd(proj):
    """'-Users-jachai-Dev-tetra-hub-cloud-api' -> '/Users/jachai/Dev/tetra/hub_cloud_api'.

    The encoding is lossy (both '/' and '-' become '-'), so verify against live cwds
    rather than trusting the decode."""
    return "/" + proj.lstrip("-").replace("-", "/")


def claude_pids_in(proj, plines):
    """Best-effort: claude processes whose cwd plausibly matches this project."""
    want = project_cwd(proj).replace("/", "").lower()
    out = []
    for ln in plines:
        parts = ln.split(None, 3)
        if len(parts) < 4 or "usage_watch" not in ln and "claude" not in parts[3]:
            continue
        if "usage_watch" in ln or "bg-pty-host" in ln:
            continue
        pid = parts[0]
        try:
            r = subprocess.run(["lsof", "-a", "-p", pid, "-d", "cwd", "-Fn"],
                               capture_output=True, text=True, timeout=5).stdout
        except Exception:
            continue
        for l2 in r.splitlines():
            if l2.startswith("n"):
                cwd = l2[1:]
                if cwd.replace("/", "").lower().startswith(want[:24]):
                    out.append((pid, cwd, parts[2]))
    return out[:6]


def first_prompt(sid, limit=400_000):
    """What this session was asked to do - the fastest way for a human to recognise it."""
    path = transcript(sid)
    if not path:
        return ""
    try:
        with open(path, "rb") as f:
            raw = f.read(limit)
    except OSError:
        return ""
    for bline in raw.split(b"\n"):
        if b'"type":"user"' not in bline and b'"type": "user"' not in bline:
            continue
        try:
            d = json.loads(bline.decode("utf-8", "replace"))
        except Exception:
            continue
        if d.get("type") != "user" or d.get("isSidechain"):
            continue
        cont = (d.get("message") or {}).get("content")
        txt = ""
        if isinstance(cont, str):
            txt = cont
        elif isinstance(cont, list):
            for b in cont:
                if isinstance(b, dict) and b.get("type") == "text":
                    txt = b.get("text", "")
                    break
        txt = " ".join(txt.split())
        if txt and not txt.startswith("<"):
            return txt[:160]
    return ""


def diagnose(a, c):
    """Rank the causes and produce concrete remedies for the current burn."""
    per = a["fast_per"] if a["fast_per"] else a["slow_per"]
    if not per:
        return [], []
    ranked = sorted(per.items(), key=lambda kv: -kv[1]["c"])
    plines = procs()
    causes, remedies = [], []
    top_sid, top = ranked[0]
    fw = c["fast_window_mins"]
    share = top["c"] / max(0.01, sum(v["c"] for v in per.values()))
    side_rate = top["s"] / max(1.0, fw)
    big_share = top["g"] / max(1, top["n"])
    cr_per_call = top["cr"] / max(1, top["n"])
    age_h = (a["now"] / 60 - top["first"]) / 60.0
    pid, etime = find_pid(top_sid, plines)
    kill = (f"`kill {pid}`" if pid else
            "find it with `ps -ef | grep '[c]laude'` and kill it")

    causes.append(f"worst session {top_sid[:8]} = ${top['c']:.0f} of ${sum(v['c'] for v in per.values()):.0f} "
                  f"({share:.0%}) in the last {fw}m, model {top['m']}, project {top['p'][-40:]}")
    label = first_prompt(top_sid)
    if label:
        causes.append(f'it was asked: "{label}"')
    tp = transcript(top_sid)
    if tp:
        causes.append(f"transcript: {tp}")
    if not pid:
        cands = claude_pids_in(top["p"], plines)
        if cands:
            causes.append("candidate pids in that project: "
                          + ", ".join(f"{p}(up {e})" for p, _, e in cands))
            kill = (f"`kill {cands[0][0]}` (verify first: `ps -p {cands[0][0]} -o command=`)")

    if side_rate >= 6:
        causes.append(f"subagent fan-out: {top['s']} sidechain calls in {fw}m ({side_rate:.0f}/min)")
        remedies.append(
            f"STOP THE FAN-OUT FIRST - it is the multiplier. Session {top_sid[:8]}: "
            f"press Ctrl-C in its window, or {kill}. "
            "Re-run in waves of <=3 agents, each with an explicit narrow file list.")
    if big_share >= 0.35:
        causes.append(f"context premium: {big_share:.0%} of calls above 200k ctx "
                      f"(peak {top['x']/1000:.0f}k) -> 2x input / 1.5x output surcharge")
        remedies.append(
            "GET UNDER 200k. Above 200k every input token costs 2x and output 1.5x. "
            f"Session peaked at {top['x']/1000:.0f}k. `/clear` and restart this work package in a fresh "
            "session, or hand it to a subagent with a narrow file list. (Your own "
            "orchestrate-plan-implementation skill measures this at ~6x.)")
    if cr_per_call >= 150_000 and top["n"] >= 20:
        causes.append(f"cache-read churn: {cr_per_call/1000:.0f}k cached tokens re-read per call "
                      f"x {top['n']} calls = {top['cr']/1e6:.0f}M tokens in {fw}m")
        remedies.append(
            "This is a re-reading bill, not a thinking bill - cache reads are ~90% of it. "
            "A long session pays for its whole transcript on EVERY turn. Compact or `/clear`; "
            "split long work into fresh short sessions.")
    if age_h >= 6:
        causes.append(f"long-lived session: {age_h:.1f}h of continuous burn")
        remedies.append(f"Session {top_sid[:8]} has run {age_h:.1f}h. Land the work, commit, and start fresh - "
                        "cost per turn grows with transcript length.")
    if a["sessions_active"] >= 4:
        others = ", ".join(f"{s[:8]}=${r['c']:.0f}" for s, r in ranked[:6])
        causes.append(f"{a['sessions_active']} sessions burning concurrently: {others}")
        remedies.append(f"{a['sessions_active']} sessions are burning at once. Close all but the one you are "
                        "actually watching; background jobs keep spending while you read.")
    if "opus" in (top["m"] or "") and top["c"] >= 40:
        remedies.append(f"Model: {top['m']} at ${top['c']:.0f}/{fw}m. Opus is 5x Sonnet on input. "
                        "Move mechanical chunks (test runs, mass edits, greps, rebases) to Sonnet; "
                        "keep Opus for design and review.")
    if "monitoring" in (top["p"] or "") or any("proposal-monitor" in l for l in plines):
        remedies.append("The launchd proposal-monitor is running (historically ~55% of your weekly drain). "
                        "Pause it: `launchctl bootout gui/$(id -u)/com.tetra.proposal-monitor`")
    if not remedies and a["tier"] != "OK":
        remedies.append(f"No single structural cause stands out - the spend is spread across "
                        f"{len(per)} sessions. Close what you are not watching, and check `/usage`.")
    if a["spent_frac"] >= 0.85:
        remedies.append(
            f"YOU ARE NEAR THE PLAN CEILING ({a['spent_frac']:.0%} of the weekly budget). "
            "Past the plan limit, spend falls through to usage credits - that is real money, "
            "not plan quota. Either stop until the weekly reset, or decide deliberately that "
            "the remaining work is worth cash.")
    if a["t_budget"] != "OK":
        remedies.append(
            f"Weekly budget: ${a['week_spent']:.0f} of ${a['budget']:.0f} ({a['spent_frac']:.0%}) with "
            f"{a['elapsed_frac']:.0%} of the week gone. To finish the week you must average "
            f"${a['need_hr']:.0f}/hr; you are at ${a['slow_hr']:.0f}/hr.")
    return causes, remedies


# ---------------------------------------------------------------- delivery

def notify(title, msg, tier, c):
    def osa(script):
        try:
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=15)
        except Exception:
            pass
    safe = msg.replace('"', "'").replace("\\", "/")[:240]
    st = title.replace('"', "'")[:100]
    sound = "Basso" if tier == "CRITICAL" else "Sosumi" if tier == "HIGH" else "Tink"
    osa(f'display notification "{safe}" with title "{st}" sound name "{sound}"')
    if c.get("sound") and tier in ("HIGH", "CRITICAL"):
        for _ in range(3 if tier == "CRITICAL" else 1):
            try:
                subprocess.run(["afplay", f"/System/Library/Sounds/{sound}.aiff"], timeout=8)
            except Exception:
                break
    if c.get("speak") and tier == "CRITICAL":
        try:
            subprocess.run(["say", "-r", "220", "Claude usage critical. Check the alert."], timeout=15)
        except Exception:
            pass
    if tier == "CRITICAL" and c.get("modal_on_critical"):
        osa(f'display dialog "{safe}" with title "{st}" buttons {{"Open alert","Dismiss"}} '
            f'default button "Open alert" with icon stop giving up after 120')
    hook = c.get("slack_webhook")
    if hook:
        import urllib.request
        try:
            req = urllib.request.Request(
                hook, data=json.dumps({"text": f"*{title}*\n```{msg[:2500]}```"}).encode(),
                headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            pass


def render(a, causes, remedies, c):
    L = []
    L.append(f"CLAUDE USAGE {a['tier']}  -  {datetime.datetime.now():%a %H:%M:%S}")
    L.append("")
    L.append(f"  burn now   ${a['fast_hr']:>6.0f}/hr   (last {c['fast_window_mins']}m, ${a['fast_c']:.0f})   [{a['t_fast']}]")
    L.append(f"  sustained  ${a['slow_hr']:>6.0f}/hr   (last {c['slow_window_mins']}m, ${a['slow_c']:.0f})   [{a['t_slow']}]")
    L.append(f"  normal is  $    30/hr median, $191 p90, $277 p95  (your own 7-week history)")
    L.append("")
    L.append(f"  week       ${a['week_spent']:.0f} / ${a['budget']:.0f}  ({a['spent_frac']:.0%} spent, "
             f"{a['elapsed_frac']:.0%} of week elapsed)  pace x{a['pace']:.2f}  [{a['t_budget']}]")
    if a["hrs_left"] is not None:
        L.append(f"  at this rate the weekly budget is gone in {a['hrs_left']:.1f}h")
    L.append("")
    if causes:
        L.append("WHY:")
        for x in causes:
            L.append(f"  - {x}")
        L.append("")
    L.append("DO THIS NOW:" if a["tier"] != "OK" else "PACE IS NORMAL - but worth fixing anyway:")
    for i, r in enumerate(remedies, 1):
        L.append(f"  {i}. {r}")
    L.append("")
    L.append("  verify against the real meter with  /usage   (this is an API-$ proxy, not your bill)")
    sy = c.get("_sync_note")
    if sy:
        L.append("  " + sy)
    return "\n".join(L)


def quiet(c, tier):
    if tier != "NOTICE":
        return False
    h = datetime.datetime.now().hour
    for span in c.get("quiet_hours") or []:
        try:
            lo, hi = int(span[0]), int(span[1])
        except Exception:
            continue
        if (lo <= h < hi) if lo < hi else (h >= lo or h < hi):
            return True
    return False


def maybe_alert(state, a, c):
    tier = a["tier"]
    last_tier = state.get("last_tier", "OK")
    last_at = state.get("last_alert_at", 0)
    state["last_tier"] = tier

    if tier == "OK":
        if last_tier != "OK":
            try:
                os.path.exists(ALERT) and os.remove(ALERT)
            except OSError:
                pass
            line = f"{datetime.datetime.now():%F %T}  RECOVERED  ${a['slow_hr']:.0f}/hr"
            with open(LOG, "a") as f:
                f.write(line + "\n")
        return False, ""

    escalated = TIERS.index(tier) > TIERS.index(last_tier)
    cool = c["cooldown_mins"].get(tier, 30) * 60
    if not escalated and (time.time() - last_at) < cool:
        return False, ""
    if quiet(c, tier):
        return False, ""

    causes, remedies = diagnose(a, c)
    body = render(a, causes, remedies, c)
    with open(ALERT, "w") as f:
        f.write(body + "\n")
    try:
        if os.path.getsize(LOG) > 2_000_000:
            os.replace(LOG, LOG + ".1")
    except OSError:
        pass
    with open(LOG, "a") as f:
        f.write(f"\n{'='*78}\n{body}\n")
    headline = (f"${a['fast_hr']:.0f}/hr now, ${a['slow_hr']:.0f}/hr sustained. "
                f"Week ${a['week_spent']:.0f}/${a['budget']:.0f}. "
                + (remedies[0][:150] if remedies else ""))
    notify(f"Claude usage {tier}", headline, tier, c)
    state["last_alert_at"] = time.time()

    if tier == "CRITICAL" and c.get("auto_stop_on_critical"):
        per = a["fast_per"] or a["slow_per"]
        if per:
            sid = max(per.items(), key=lambda kv: kv[1]["c"])[0]
            pid, _ = find_pid(sid, procs())
            if pid:
                try:
                    subprocess.run(["kill", "-TERM", pid], timeout=5)
                    with open(LOG, "a") as f:
                        f.write(f"AUTO-STOP: sent SIGTERM to pid {pid} (session {sid[:8]})\n")
                except Exception:
                    pass
    return True, body


# ---------------------------------------------------------------- verbs

def cmd_once(args):
    c = cfg()
    st, scanned = refresh(c, timeout=20.0)
    n = 0 if not scanned else 1
    a = assess(st, c)
    fired, body = maybe_alert(st, a, c)
    meta = {"last_run": time.time(),
            "last_fast_hr": round(a["fast_hr"], 1),
            "last_slow_hr": round(a["slow_hr"], 1),
            "last_week": round(a["week_spent"], 1),
            "last_tier": st.get("last_tier", "OK"),
            "last_alert_at": st.get("last_alert_at", 0)}
    # one tiny line so the statusline never has to parse the 3MB state file
    try:
        with open(PACE, "w") as f:
            f.write(f"{a['tier']}|{a['fast_hr']:.0f}|{a['slow_hr']:.0f}|"
                    f"{a['week_spent']:.0f}|{a['budget']:.0f}|{a['spent_frac']*100:.0f}|"
                    f"{a['elapsed_frac']*100:.0f}|{int(time.time())}\n")
    except OSError:
        pass
    save_meta(meta)
    if args.verbose or fired:
        print(body or f"OK  +{n} calls  ${a['fast_hr']:.0f}/hr now  ${a['slow_hr']:.0f}/hr sustained  "
                      f"week ${a['week_spent']:.0f}/${a['budget']:.0f}")
    return 0


def cmd_status(args):
    c = cfg()
    st, _ = refresh(c)
    a = assess(st, c)
    causes, remedies = diagnose(a, c)
    if not remedies:
        remedies = ["Nothing to do - pace is normal."]
    print(render(a, causes, remedies, c))
    return 0


def cmd_top(args):
    c = cfg()
    st, _ = refresh(c)
    tot, per = window(st.get("buckets", {}), args.mins)
    print(f"last {args.mins}m: ${tot:.0f}  (= ${tot*60/args.mins:.0f}/hr)")
    plines = procs()
    for sid, r in sorted(per.items(), key=lambda kv: -kv[1]["c"])[:15]:
        pid, et = find_pid(sid, plines)
        print(f"  ${r['c']:7.1f}  {sid[:8]}  calls={r['n']:5d} side={r['s']:5d} "
              f">200k={r['g']:4d} ctxmax={r['x']/1000:5.0f}k cr={r['cr']/1e6:6.1f}M "
              f"{(r['m'] or '')[:18]:18s} {r['p'][-34:]:34s} pid={pid or '-'}")
    return 0


def cmd_test(args):
    c = cfg()
    fake = {"now": time.time(), "fast_hr": 1896, "slow_hr": 812, "fast_c": 316, "slow_c": 812,
            "fast_per": {"7bce2fe9-test": {"c": 316.0, "n": 420, "s": 190, "g": 300, "cr": 90_000_000,
                                           "o": 30000, "x": 775_000, "m": "claude-opus-5",
                                           "p": "-Users-jachai-Dev-tetra-hub-cloud-api",
                                           "first": time.time()/60 - 500, "last": time.time()/60}},
            "slow_per": {}, "week_spent": 11200, "budget": c["weekly_budget"], "spent_frac": 1.24,
            "elapsed_frac": 0.42, "pace": 2.95, "t_fast": "CRITICAL", "t_slow": "CRITICAL",
            "t_budget": "CRITICAL", "tier": "CRITICAL", "hrs_left": 0.0, "need_hr": 0.0,
            "wstart": time.time() - 3 * 86400, "sessions_active": 5}
    causes, remedies = diagnose(fake, c)
    body = render(fake, causes, remedies, c)
    print(body)
    notify("Claude usage CRITICAL (TEST)",
           "TEST ALERT - $1896/hr. This is what a real drain looks like.", "CRITICAL", c)
    return 0


PLIST_XML = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>{label}</string>
  <key>ProgramArguments</key>
  <array><string>/usr/bin/python3</string><string>{script}</string><string>once</string></array>
  <key>StartInterval</key><integer>60</integer>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>{dir}/watch.out</string>
  <key>StandardErrorPath</key><string>{dir}/watch.err</string>
  <key>ProcessType</key><string>Background</string>
  <key>LowPriorityIO</key><true/>
</dict></plist>
"""


METER_PLIST_XML = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>{label}</string>
  <key>ProgramArguments</key>
  <array><string>/usr/bin/python3</string><string>{script}</string><string>serve</string>
         <string>--port</string><string>{port}</string></array>
  <key>KeepAlive</key><true/>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>{dir}/meter.out</string>
  <key>StandardErrorPath</key><string>{dir}/meter.err</string>
  <key>ProcessType</key><string>Background</string>
  <key>LowPriorityIO</key><true/>
</dict></plist>
"""


def cmd_install(args):
    os.makedirs(os.path.dirname(PLIST), exist_ok=True)
    os.makedirs(DIR, exist_ok=True)
    with open(PLIST, "w") as f:
        f.write(PLIST_XML.format(label=LABEL, script=os.path.abspath(__file__), dir=DIR))
    uid = os.getuid()
    subprocess.run(["launchctl", "bootout", f"gui/{uid}/{LABEL}"], capture_output=True)
    r = subprocess.run(["launchctl", "bootstrap", f"gui/{uid}", PLIST], capture_output=True, text=True)
    print(f"plist: {PLIST}")
    print("bootstrap:", (r.stdout + r.stderr).strip() or "ok")
    # the always-on live meter
    with open(MPLIST, "w") as f:
        f.write(METER_PLIST_XML.format(label=MLABEL, script=os.path.abspath(__file__),
                                       dir=DIR, port=METER_PORT))
    subprocess.run(["launchctl", "bootout", f"gui/{uid}/{MLABEL}"], capture_output=True)
    r2 = subprocess.run(["launchctl", "bootstrap", f"gui/{uid}", MPLIST],
                        capture_output=True, text=True)
    print("meter    :", (r2.stdout + r2.stderr).strip() or "ok")
    cfg()
    print(f"config: {CONFIG}")
    print("watcher polls every 60s. `usage_watch.py status` for a live read.")
    print(f"meter   http://127.0.0.1:{METER_PORT}   (floating window: ~/.claude/scripts/usage-meter.command)")
    return 0


def cmd_uninstall(args):
    uid = os.getuid()
    out = []
    for lab, pl in ((LABEL, PLIST), (MLABEL, MPLIST)):
        r = subprocess.run(["launchctl", "bootout", f"gui/{uid}/{lab}"],
                           capture_output=True, text=True)
        out.append(f"{lab}: {(r.stdout + r.stderr).strip() or 'ok'}")
        try:
            os.remove(pl)
        except OSError:
            pass
    print("\n".join(out))
    return 0


def cmd_calibrate(args):
    """Re-derive thresholds from history so they stay tuned to how you actually work."""
    H = collections.defaultdict(float)
    T = collections.defaultdict(float)
    W = collections.defaultdict(float)
    seen = set()
    cut = time.time() - args.days * 86400
    for path in glob.glob(ROOT + "/**/*.jsonl", recursive=True):
        try:
            if os.path.getmtime(path) < cut:
                continue
        except OSError:
            continue
        for line in open(path, encoding="utf-8", errors="replace"):
            if '"usage"' not in line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("type") != "assistant":
                continue
            m = d.get("message") or {}
            u = m.get("usage")
            ts = d.get("timestamp")
            if not u or not ts:
                continue
            k = (m.get("id"), d.get("requestId"))
            if k in seen:
                continue
            seen.add(k)
            try:
                t = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                continue
            if t.timestamp() < cut:
                continue
            inp, cw, cr, out = (u.get(x, 0) or 0 for x in
                                ("input_tokens", "cache_creation_input_tokens",
                                 "cache_read_input_tokens", "output_tokens"))
            c = price(m.get("model", "?"), inp, cw, cr, out, inp + cw + cr)
            H[t.strftime("%Y-%m-%d %H")] += c
            T[t.strftime("%Y-%m-%d %H") + f":{t.minute//10}"] += c
            y, w, _ = t.isocalendar()
            W[f"{y}-W{w:02d}"] += c
    hs = sorted(H.values()) or [0]
    ts_ = sorted(T.values()) or [0]
    def p(a, q): return a[min(len(a) - 1, int(len(a) * q))]
    print(f"{len(hs)} active hours over {args.days}d")
    print(f"  hourly  p50 ${p(hs,.5):.0f}  p90 ${p(hs,.9):.0f}  p95 ${p(hs,.95):.0f}  "
          f"p99 ${p(hs,.99):.0f}  max ${hs[-1]:.0f}")
    print(f"  10-min  p90 ${p(ts_,.9)*6:.0f}/hr  p95 ${p(ts_,.95)*6:.0f}/hr  "
          f"p99 ${p(ts_,.99)*6:.0f}/hr  max ${ts_[-1]*6:.0f}/hr")
    print("  weekly: " + "  ".join(f"{k}=${v:.0f}" for k, v in sorted(W.items())))
    print("\nsuggested config:")
    print(json.dumps({"slow_notice": round(p(hs, .90)), "slow_high": round(p(hs, .96)),
                      "slow_crit": round(p(hs, .99)),
                      "fast_notice": round(p(ts_, .92) * 6), "fast_high": round(p(ts_, .97) * 6),
                      "fast_crit": round(p(ts_, .995) * 6),
                      "weekly_budget": round(sorted(W.values())[len(W)//2] * 1.3) if W else 9000},
                     indent=2))
    return 0



def cmd_sync(args):
    """Calibrate the proxy against the REAL meter at claude.ai/settings/usage.

    You read the percentage off the page; this back-solves what your weekly limit is
    worth in proxy-dollars, so the budget stops being a guess."""
    c = cfg()
    st, _ = refresh(c)
    a = assess(st, c)
    obs = st.setdefault("syncs", [])
    rec = {"t": time.time(), "pct": args.pct, "spent": round(a["week_spent"], 1),
           "wstart": a["wstart"]}
    if args.credits_spent is not None:
        rec["credits_spent"] = args.credits_spent
    if args.balance is not None:
        rec["balance"] = args.balance
    obs.append(rec)
    st["syncs"] = obs[-40:]

    ws = datetime.datetime.fromtimestamp(a["wstart"])
    print(f"plan week started {ws:%a %d %b %H:%M} local; {a['elapsed_frac']:.0%} elapsed")
    print(f"proxy spend since reset: ${a['week_spent']:.0f}")
    print(f"real meter you reported : {args.pct}%")

    implied = None
    if args.pct and args.pct > 0:
        implied = a["week_spent"] / (args.pct / 100.0)
        print(f"\n=> implied weekly limit ~= ${implied:,.0f} of proxy-$")
        if args.pct < 5:
            print("   WARNING: below 5% the extrapolation is very noisy. Re-sync at 20-40%")
            print("   for a number you can trust.")
    same = [o for o in obs if o.get("wstart") == a["wstart"] and o.get("pct")]
    if len(same) >= 2:
        print("\n   this week's observations:")
        for o in same:
            imp = o["spent"] / (o["pct"] / 100.0) if o["pct"] else 0
            print(f"     {datetime.datetime.fromtimestamp(o['t']):%a %H:%M}  "
                  f"{o['pct']:>3}%  ${o['spent']:>7.0f}  -> limit ~${imp:,.0f}")
    if args.credits_spent is not None:
        print(f"\nusage credits spent this month: ${args.credits_spent:,.2f}"
              + (f"  (balance ${args.balance:,.2f})" if args.balance is not None else ""))
        print("   credits are REAL MONEY - they are what the weekly plan overflows into.")

    if args.apply and implied and args.pct >= 5:
        c["weekly_budget"] = round(implied)
        save(CONFIG, {k: v for k, v in c.items() if not k.startswith("_")})
        print(f"\napplied: weekly_budget = ${round(implied):,}")
    elif args.apply:
        print("\nNOT applied - need --pct >= 5 to trust the extrapolation.")
    else:
        print("\n(add --apply to write this into config.json)")
    save_meta({"syncs": st["syncs"]})
    return 0



# ---------------------------------------------------------------- live meter

METER_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Usage meter</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap">
<style>
:root{--bg:#0E1417;--surf:#161E22;--ink:#E9EEF0;--body:#C3CFD4;--mut:#8FA0A7;
 --faint:#6B7C83;--rule:#26333A;--teal:#5FBEC2;--amber:#E9A93F;--crit:#E0625A;--good:#5BB98C;
 --mono:"IBM Plex Mono",ui-monospace,Menlo,monospace;--sans:"IBM Plex Sans",system-ui,sans-serif}
*{box-sizing:border-box;margin:0}
body{background:var(--bg);color:var(--body);font-family:var(--sans);font-size:13px;
 padding:13px;-webkit-font-smoothing:antialiased;min-width:300px}
.lbl{font-family:var(--mono);font-size:9.5px;letter-spacing:.12em;text-transform:uppercase;color:var(--mut)}
.rate{font-family:var(--mono);font-variant-numeric:tabular-nums;font-weight:600;
 font-size:44px;line-height:1;color:var(--ink);letter-spacing:-.02em}
.rate .u{font-size:15px;color:var(--mut);font-weight:400}
.hd{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.pill{font-family:var(--mono);font-size:10px;font-weight:600;letter-spacing:.1em;
 padding:4px 9px;border-radius:2px;white-space:nowrap}
.OK{background:#12312F;color:var(--good)}.NOTICE{background:#2B2114;color:var(--amber)}
.HIGH{background:#3A1D1B;color:var(--crit)}.CRITICAL{background:var(--crit);color:#14090a}
.DOWN{background:#2A2A2A;color:var(--faint)}
.row{display:flex;justify-content:space-between;align-items:baseline;gap:8px;padding:4px 0}
.row b{font-family:var(--mono);font-variant-numeric:tabular-nums;font-weight:500;color:var(--ink)}
.box{background:var(--surf);border:1px solid var(--rule);padding:11px 12px;margin-top:10px}
.boxhd{display:flex;justify-content:space-between;align-items:center;gap:8px;padding-bottom:9px}
.boxhd h4{font-family:var(--mono);font-size:9.5px;letter-spacing:.12em;text-transform:uppercase;
 color:var(--mut);font-weight:500}
.wins{display:flex;gap:3px}
.wins button{font-family:var(--mono);font-size:10px;color:var(--mut);background:#0b1013;
 border:1px solid var(--rule);padding:3px 7px;cursor:pointer;border-radius:2px;line-height:1.3}
.wins button:hover{color:var(--ink);border-color:var(--faint)}
.wins button.on{background:var(--teal);border-color:var(--teal);color:#08201f;font-weight:600}
.wins button:focus-visible{outline:2px solid var(--amber);outline-offset:1px}
canvas{display:block;width:100%}
.trk{height:8px;background:#0b1013;border:1px solid var(--rule);position:relative;margin:7px 0 5px}
.fill{position:absolute;left:0;top:0;bottom:0;background:var(--teal)}
.fill.warn{background:var(--amber)}.fill.bad{background:var(--crit)}
.mark{position:absolute;top:-3px;bottom:-3px;width:2px;background:var(--ink);opacity:.85}
.sess{display:grid;grid-template-columns:1fr auto;gap:1px 10px;padding:6px 0;border-top:1px solid var(--rule)}
.sess:first-of-type{border-top:none}
.sess .n{font-family:var(--mono);font-size:11px;color:var(--ink)}
.sess .c{font-family:var(--mono);font-variant-numeric:tabular-nums;color:var(--amber);font-size:12px}
.sess .w{grid-column:1/-1;font-size:11px;color:var(--faint);overflow:hidden;
 text-overflow:ellipsis;white-space:nowrap}
.duo{display:flex;gap:20px;align-items:flex-end;padding-bottom:3px}
.duo .big{font-family:var(--mono);font-size:28px;font-weight:600;line-height:1}
.duo .cap{font-family:var(--mono);font-size:9.5px;letter-spacing:.1em;text-transform:uppercase;
 color:var(--mut);padding-top:3px}
.sw{display:inline-block;width:8px;height:8px;margin-right:4px;vertical-align:baseline}
.verdict{font-size:11.5px;padding-top:8px;line-height:1.5}
.v-ok{color:var(--good)}.v-warn{color:var(--amber)}.v-bad{color:var(--crit)}
.chip{display:inline-block;font-family:var(--mono);font-size:10px;color:var(--body);
 background:#0b1013;border:1px solid var(--rule);padding:1px 5px;margin:3px 3px 0 0}
.ft{color:var(--faint);font-family:var(--mono);font-size:10px;padding-top:10px;line-height:1.6}
.stale{color:var(--crit)}
</style></head><body>
<div class="hd">
  <div><div class="lbl">burn rate &middot; 60 min</div>
  <div class="rate"><span class="u">$</span><span id="slow">&mdash;</span><span class="u">/hr</span></div></div>
  <div class="pill DOWN" id="tier">&mdash;</div>
</div>

<div class="box" style="margin-top:11px">
  <div class="boxhd"><h4>spend rate &middot; <span id="wlab">1h</span></h4>
    <div class="wins" id="wins"></div></div>
  <canvas id="spend" height="150"></canvas>
</div>

<div class="row"><span class="lbl">last 10 min</span><b id="fast">&mdash;</b></div>
<div class="row"><span class="lbl">normal for you</span><b>$30/hr</b></div>

<div class="box">
  <div class="boxhd"><h4>what is running</h4>
    <span class="lbl"><span class="sw" style="background:#5FBEC2"></span>sessions
      <span class="sw" style="background:#E9A93F;margin-left:8px"></span>subagents</span></div>
  <div class="duo">
    <div><div class="big" id="sessnow" style="color:#5FBEC2">&mdash;</div>
      <div class="cap">sessions</div></div>
    <div><div class="big" id="agnow" style="color:#E9A93F">&mdash;</div>
      <div class="cap">subagents &middot; max <span id="agrec">3</span></div></div>
  </div>
  <canvas id="agents" height="120"></canvas>
  <div class="verdict" id="agverdict"></div>
  <div id="agchips"></div>
</div>

<div class="box">
  <h4 class="lbl" style="display:block;padding-bottom:8px">plan week</h4>
  <div class="row"><span class="lbl">spent</span><b id="wk">&mdash;</b></div>
  <div class="trk"><div class="fill" id="wkfill"></div><div class="mark" id="wkmark"></div></div>
  <div class="row"><span class="lbl" id="wklbl">&mdash;</span><b id="eta">&mdash;</b></div>
</div>

<div class="box"><h4 class="lbl" style="display:block;padding-bottom:8px">burning now</h4>
  <div id="sess"></div></div>

<div class="ft" id="ft">connecting&hellip;</div>
<script>
var $=function(i){return document.getElementById(i)};
var CSS=getComputedStyle(document.documentElement);
function tok(n){return CSS.getPropertyValue(n).trim()||'#888'}
function fin(v){return (typeof v==='number'&&isFinite(v))?v:null}
function money(n){if(n===null||n===undefined)return '--';
  return n>=1000?'$'+(n/1000).toFixed(1)+'k':'$'+Math.round(n)}
var WINS=[[60,'1h'],[360,'6h'],[1440,'24h'],[10080,'7d']];
var WIN=Number(localStorage.getItem('meterWin')||60);
if(!WINS.some(function(w){return w[0]===WIN}))WIN=60;
(function(){
  var box=$('wins');
  WINS.forEach(function(w){
    var btn=document.createElement('button');
    btn.textContent=w[1]; btn.setAttribute('type','button');
    btn.onclick=function(){WIN=w[0];try{localStorage.setItem('meterWin',WIN)}catch(e){}
      paint();tick()};
    box.appendChild(btn);
  });
  paint();
})();
function paint(){
  var bs=$('wins').children;
  for(var i=0;i<bs.length;i++){bs[i].className=(WINS[i][0]===WIN)?'on':''}
}
/* label for "how long ago", given minutes */
function ago(m){
  if(m<=0)return 'now';
  if(m>=1440)return '-'+(m%1440?(m/1440).toFixed(1):(m/1440))+'d';
  if(m>=60)return '-'+(m%60?(m/60).toFixed(1):(m/60))+'h';
  return '-'+Math.round(m)+'m';
}
function setup(c,cssH){
  var r=window.devicePixelRatio||1, W=c.clientWidth||340;
  c.style.height=cssH+'px';
  c.width=Math.round(W*r); c.height=Math.round(cssH*r);
  var x=c.getContext('2d');
  x.setTransform(r,0,0,r,0,0);
  x.clearRect(0,0,W,cssH);
  return {x:x,W:W,H:cssH};
}
function axisX(x,L,PW,yy,win){
  x.strokeStyle=tok('--rule'); x.beginPath(); x.moveTo(L,yy+0.5); x.lineTo(L+PW,yy+0.5); x.stroke();
  x.fillStyle=tok('--faint'); x.textAlign='center';
  [0,0.25,0.5,0.75,1].forEach(function(f){
    x.fillText(ago(win*(1-f)),L+f*PW,yy+11);
  });
}
/* spend chart: $/hr, with avg, max and "normal" reference lines */
function drawSpend(pts,win,normal){
  var o=setup($('spend'),140), x=o.x, W=o.W, H=o.H;
  var L=48, R=8, T=10, B=26, PW=W-L-R, PH=H-T-B;
  if(!pts||!pts.length)return;
  var mx=0,sum=0;
  for(var i=0;i<pts.length;i++){if(pts[i]>mx)mx=pts[i];sum+=pts[i]}
  var avg=sum/pts.length, top=Math.max(mx*1.18,normal*1.5,10);
  var y=function(v){return T+PH-(v/top)*PH};
  var px=function(i){return L+(i/(pts.length-1))*PW};
  x.font='9px "IBM Plex Mono",monospace'; x.textBaseline='middle';
  x.strokeStyle=tok('--rule'); x.lineWidth=1; x.fillStyle=tok('--faint'); x.textAlign='right';
  [0,top/2,top].forEach(function(v){
    var yy=Math.round(y(v))+0.5;
    x.beginPath(); x.moveTo(L,yy); x.lineTo(L+PW,yy); x.stroke();
    x.fillText(money(v),L-6,yy);
  });
  x.beginPath(); x.moveTo(px(0),T+PH);
  for(var i=0;i<pts.length;i++){x.lineTo(px(i),y(pts[i]))}
  x.lineTo(px(pts.length-1),T+PH); x.closePath();
  x.fillStyle='rgba(95,190,194,.15)'; x.fill();
  x.beginPath();
  for(var i=0;i<pts.length;i++){var yy=y(pts[i]); if(i){x.lineTo(px(i),yy)}else{x.moveTo(px(i),yy)}}
  x.strokeStyle=tok('--teal'); x.lineWidth=1.8; x.stroke();
  x.lineWidth=1; x.textAlign='left';
  /* normal reference */
  x.setLineDash([2,3]); x.strokeStyle=tok('--good');
  x.beginPath(); x.moveTo(L,y(normal)); x.lineTo(L+PW,y(normal)); x.stroke();
  x.fillStyle=tok('--good'); x.fillText('normal '+money(normal),L+4,y(normal)+8);
  /* max */
  x.setLineDash([4,3]); x.strokeStyle=tok('--crit');
  x.beginPath(); x.moveTo(L,y(mx)); x.lineTo(L+PW,y(mx)); x.stroke();
  x.fillStyle=tok('--crit'); x.fillText('max '+money(mx),L+4,y(mx)-7);
  /* avg */
  x.strokeStyle=tok('--amber');
  x.beginPath(); x.moveTo(L,y(avg)); x.lineTo(L+PW,y(avg)); x.stroke();
  x.fillStyle=tok('--amber'); x.fillText('avg '+money(avg),L+PW-70,y(avg)-7);
  x.setLineDash([]);
  x.beginPath(); x.arc(px(pts.length-1),y(pts[pts.length-1]),3,0,7);
  x.fillStyle=tok('--amber'); x.fill();
  axisX(x,L,PW,T+PH,win);
  x.fillStyle=tok('--faint'); x.textAlign='left'; x.fillText('$ / hr',2,T+PH+22);
}
/* sessions (bars) + subagents (line) + the recommended ceiling */
function drawAgents(sess,ags,rec,win){
  var o=setup($('agents'),110), x=o.x, W=o.W, H=o.H;
  var L=30, R=8, T=10, B=24, PW=W-L-R, PH=H-T-B;
  sess=sess&&sess.length?sess:[0]; ags=ags&&ags.length?ags:[0];
  var mx=rec+1;
  for(var i=0;i<sess.length;i++){if(sess[i]>mx)mx=sess[i]}
  for(var i=0;i<ags.length;i++){if(ags[i]>mx)mx=ags[i]}
  var y=function(v){return T+PH-(v/mx)*PH};
  x.font='9px "IBM Plex Mono",monospace'; x.textBaseline='middle';
  x.strokeStyle=tok('--rule'); x.fillStyle=tok('--faint'); x.textAlign='right'; x.lineWidth=1;
  [0,mx].forEach(function(v){
    var yy=Math.round(y(v))+0.5;
    x.beginPath(); x.moveTo(L,yy); x.lineTo(L+PW,yy); x.stroke();
    x.fillText(String(Math.round(v)),L-6,yy);
  });
  var bw=Math.max(1.5,PW/sess.length-1);
  x.fillStyle='rgba(95,190,194,.55)';
  for(var i=0;i<sess.length;i++){
    if(!sess[i])continue;
    x.fillRect(L+(i/sess.length)*PW,y(sess[i]),bw,(T+PH)-y(sess[i]));
  }
  x.beginPath();
  for(var i=0;i<ags.length;i++){
    var xx=L+(i/ags.length)*PW+bw/2, yy=y(ags[i]);
    if(i){x.lineTo(xx,yy)}else{x.moveTo(xx,yy)}
  }
  x.strokeStyle=tok('--amber'); x.lineWidth=1.6; x.stroke(); x.lineWidth=1;
  x.setLineDash([4,3]); x.strokeStyle=tok('--crit');
  x.beginPath(); x.moveTo(L,y(rec)); x.lineTo(L+PW,y(rec)); x.stroke(); x.setLineDash([]);
  x.fillStyle=tok('--crit'); x.textAlign='left';
  x.fillText('max subagents '+rec,L+4,y(rec)-7);
  axisX(x,L,PW,T+PH,win);
  x.fillStyle=tok('--faint'); x.fillText('count',2,T+PH+21);
}
function down(msg){
  $('tier').textContent='NO DATA'; $('tier').className='pill DOWN';
  $('ft').innerHTML='<span class="stale">'+msg+'</span>';
}
function tick(){
  fetch('/api?win='+WIN,{cache:'no-store'}).then(function(r){return r.json()}).then(function(d){
    if(!d||d.error){down('watcher error: '+((d&&d.error)||'unknown'));return}
    var slow=fin(d.slow_hr), fast=fin(d.fast_hr);
    $('slow').textContent=slow===null?'--':Math.round(slow).toLocaleString();
    $('fast').textContent=fast===null?'--':'$'+Math.round(fast).toLocaleString()+'/hr';
    var t=$('tier'); t.textContent=d.tier||'--'; t.className='pill '+(d.tier||'DOWN');
    $('wlab').textContent=d.win_label||'1h';
    var ws=fin(d.week_spent), bg=fin(d.budget);
    $('wk').textContent=money(ws)+' / '+money(bg);
    var sp=fin(d.spent_frac), el=fin(d.elapsed_frac);
    sp=sp===null?0:Math.min(100,sp*100); el=el===null?0:Math.min(100,el*100);
    var f=$('wkfill'); f.style.width=sp+'%';
    f.className='fill'+(sp>el*1.6?' bad':sp>el*1.3?' warn':'');
    $('wkmark').style.left=el+'%';
    $('wklbl').textContent=Math.round(sp)+'% spent / '+Math.round(el)+'% of week';
    var hl=fin(d.hrs_left);
    $('eta').textContent=hl===null?'--':(hl>168?'safe':'gone in '+hl.toFixed(1)+'h');
    drawSpend(d.rate||[],d.win||60,d.normal_hr||30);
    var rec=d.rec_agents||3, now=d.agents_now||0, sn=d.sessions_now||0;
    $('sessnow').textContent=sn; $('agnow').textContent=now; $('agrec').textContent=rec;
    drawAgents(d.session_series||[],d.agent_series||[],rec,d.win||60);
    var v=$('agverdict');
    if(now===0){
      v.className='verdict';
      v.innerHTML='<span style="color:'+tok('--faint')+'">'+sn+
        (sn===1?' session':' sessions')+' active, no subagents. Peak this window: '+
        (d.agents_peak||0)+' subagents.</span>';
    } else if(now<=rec){v.className='verdict v-ok';
      v.textContent=now+' subagents - within the recommended wave of '+rec+'.';
    } else if(now<=rec*2){v.className='verdict v-warn';
      v.textContent=now+' subagents, above the recommended '+rec+
        '. Each keeps its own context and re-reads it every turn.';
    } else {v.className='verdict v-bad';
      v.textContent=now+' subagents. Fan-out is the multiplier - let this wave finish first.';}
    if(d.max_depth>1){v.innerHTML+=' <span class="v-bad">Agents are spawning agents (depth '+
      d.max_depth+').</span>'}
    $('agchips').innerHTML=(d.agent_kinds||[]).map(function(a){
      return '<span class="chip">'+a.n+'x '+a.t+' &middot; '+a.m+'</span>'}).join('');
    $('sess').innerHTML=(d.sessions||[]).map(function(s){
      return '<div class="sess"><span class="n">'+s.id+'</span><span class="c">$'+
        (fin(s.cost)||0).toFixed(0)+'</span><span class="w">'+(s.what||s.proj||'')+'</span></div>'
      }).join('')||'<div class="sess"><span class="w" style="color:'+tok('--faint')+'">idle</span></div>';
    $('ft').innerHTML=(d.stale?'<span class="stale">WATCHER STALE - data '+d.age+'s old</span>'
      :'data '+d.age+'s old &middot; '+new Date().toLocaleTimeString())
      +(d.sync_note?'<br>'+d.sync_note:'');
  }).catch(function(){down('no data - is the watcher running?')});
}
tick(); setInterval(tick,5000);
window.addEventListener('resize',function(){tick()});
</script></body></html>"""

_scache = {"mtime": -1, "data": None}

# window -> (number of points on the chart, label)
WINDOWS = {60: (60, "1h"), 360: (72, "6h"), 1440: (96, "24h"), 10080: (84, "7d")}


def read_state_ro():
    """Read-only view of the watcher's state. The watcher is the ONLY writer -
    the meter must never scan or save, or the two race and corrupt each other."""
    try:
        m = os.path.getmtime(STATE)
    except OSError:
        return {}
    if _scache["mtime"] != m:
        d = load(STATE, {})
        if d:
            _scache["mtime"], _scache["data"] = m, d
    return _scache["data"] or {}


def build_series(b, minutes, points, now):
    """Aggregate per-minute buckets into `points` slots.

    Spend is normalised to $/hr so the y-axis means the same thing at every window
    and lines up with the alert thresholds."""
    lo = int((now - minutes * 60) // 60)
    span = minutes / float(points)                       # minutes per slot
    spend = [0.0] * points
    ags = [set() for _ in range(points)]
    sess = [set() for _ in range(points)]
    for k, v in b.items():
        mn_s, sid = k.split("|", 1)
        mn = int(mn_s)
        if mn < lo:
            continue
        i = int((mn - lo) / span)
        if 0 <= i < points:
            spend[i] += v["c"]
            ags[i].update(v.get("ag") or [])
            sess[i].add(sid)
    rate = [round(x / (span / 60.0), 2) for x in spend]   # $/hr
    return rate, [len(a) for a in ags], [len(x) for x in sess]


def meter_payload(win=60):
    c = cfg()
    st = read_state_ro()
    if not st.get("buckets"):
        raise RuntimeError("watcher has not written state yet")
    a = assess(st, c)
    b = st["buckets"]
    now = time.time()
    if win not in WINDOWS:
        win = 60
    points, wlabel = WINDOWS[win]
    rate, agent_series, session_series = build_series(b, win, points, now)

    # "now" = the last 5 minutes of real activity
    lo5 = int((now - 300) // 60)
    recent_ag, recent_sess = set(), set()
    for k, v in b.items():
        mn_s, sid = k.split("|", 1)
        if int(mn_s) >= lo5:
            recent_ag.update(v.get("ag") or [])
            recent_sess.add(sid)
    info = st.get("agent_info", {})
    kinds = collections.Counter((info.get(x, {}).get("t", "?"),
                                 info.get(x, {}).get("m", "?")) for x in recent_ag)
    depth = max([info.get(x, {}).get("d", 1) for x in recent_ag] or [1])

    per = a["fast_per"] or a["slow_per"]
    top = sorted(per.items(), key=lambda kv: -kv[1]["c"])[:5]
    sessions = [{"id": sid[:8], "cost": r["c"], "proj": r["p"][-30:],
                 "what": first_prompt(sid)[:70]} for sid, r in top if r["c"] > 0.05]
    age = int(now - st.get("last_run", 0))
    return {"fast_hr": a["fast_hr"], "slow_hr": a["slow_hr"], "tier": a["tier"],
            "week_spent": a["week_spent"], "budget": a["budget"],
            "spent_frac": a["spent_frac"], "elapsed_frac": a["elapsed_frac"],
            "hrs_left": a["hrs_left"],
            "win": win, "win_label": wlabel, "rate": rate,
            "agent_series": agent_series, "session_series": session_series,
            "sessions_now": len(recent_sess), "agents_now": len(recent_ag),
            "agents_peak": max(agent_series or [0]),
            "sessions_peak": max(session_series or [0]),
            "rec_agents": RECOMMENDED_AGENTS, "max_depth": depth,
            "agent_kinds": [{"t": t, "m": m, "n": n} for (t, m), n in kinds.most_common(4)],
            "normal_hr": 30, "sessions": sessions,
            "age": age, "stale": age > 300,
            "sync_note": c.get("_sync_note", "")}


def cmd_serve(args):
    import http.server
    payload_lock = [0.0, None, 60]

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            try:
                self.wfile.write(body)
            except Exception:
                pass

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path == "/api":
                win = 60
                q = self.path.split("?", 1)
                if len(q) > 1:
                    for part in q[1].split("&"):
                        if part.startswith("win="):
                            try:
                                win = int(part[4:])
                            except ValueError:
                                pass
                # throttle per window: recompute at most every 2s however many tabs poll
                key = payload_lock[2] if len(payload_lock) > 2 else None
                if (time.time() - payload_lock[0] > 2 or payload_lock[1] is None
                        or key != win):
                    try:
                        payload_lock[1] = meter_payload(win)
                    except Exception as e:
                        payload_lock[1] = {"error": str(e)}
                    payload_lock[0] = time.time()
                    if len(payload_lock) > 2:
                        payload_lock[2] = win
                self._send(200, json.dumps(payload_lock[1]).encode(), "application/json")
            elif path in ("/", "/index.html"):
                self._send(200, METER_HTML.encode(), "text/html; charset=utf-8")
            else:
                self._send(404, b"not found", "text/plain")

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), H)
    print(f"usage meter on http://127.0.0.1:{args.port}  (Ctrl-C to stop)")
    print(f"floating window:\n  open -na 'Google Chrome' --args --app=http://127.0.0.1:{args.port} "
          f"--window-size=380,620")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    o = sub.add_parser("once"); o.add_argument("-v", "--verbose", action="store_true"); o.set_defaults(f=cmd_once)
    s = sub.add_parser("status"); s.set_defaults(f=cmd_status)
    t = sub.add_parser("top"); t.add_argument("--mins", type=int, default=60); t.set_defaults(f=cmd_top)
    x = sub.add_parser("test"); x.set_defaults(f=cmd_test)
    i = sub.add_parser("install"); i.set_defaults(f=cmd_install)
    u = sub.add_parser("uninstall"); u.set_defaults(f=cmd_uninstall)
    y = sub.add_parser("sync")
    y.add_argument("--pct", type=float, required=True,
                   help="the 'All models' %% used from claude.ai/settings/usage")
    y.add_argument("--credits-spent", type=float, default=None)
    y.add_argument("--balance", type=float, default=None)
    y.add_argument("--apply", action="store_true", help="write the implied budget to config")
    y.set_defaults(f=cmd_sync)
    v = sub.add_parser("serve")
    v.add_argument("--port", type=int, default=7654)
    v.set_defaults(f=cmd_serve)
    k = sub.add_parser("calibrate"); k.add_argument("--days", type=int, default=50); k.set_defaults(f=cmd_calibrate)
    a = ap.parse_args()
    if not getattr(a, "f", None):
        ap.print_help(); return 1
    os.makedirs(DIR, exist_ok=True)
    return a.f(a)


if __name__ == "__main__":
    sys.exit(main())
