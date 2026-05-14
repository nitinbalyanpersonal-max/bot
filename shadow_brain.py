"""
╔══════════════════════════════════════════════════════════════╗
║         SHADOW BOT · SHADOW BRAIN                           ║
║   Autonomous thinking loop — the bot lives, observes,       ║
║   decides, and acts on its own. No commands needed.         ║
║                                                              ║
║   Also handles:                                              ║
║   - D1 long-term user memory (AI + user + admin writable)   ║
║   - Mission opt-in system (default: OFF, persisted in D1)   ║
╚══════════════════════════════════════════════════════════════╝

BRAIN LOOP:
  Every 60–120 min (randomised so it feels organic), the Shadow Brain:
    1. Loads full server state (members, sessions, todos, streaks, exams)
    2. Loads long-term memory notes for all operatives
    3. Asks Groq AI: "What should you do right now, if anything?"
    4. Executes the AI's chosen action (or does nothing — silence is valid)

ACTIONS the brain can autonomously take:
  - post_lore      → atmospheric lore drop in #shadow-activity
  - nudge_dm       → private DM to one operative (accountability, exam warning, etc.)
  - server_callout → public recognition/challenge for the whole server
  - ghost_ping     → ping one inactive operative in #shadow-activity
  - do_nothing     → explicitly decided to stay quiet

MEMORY SYSTEM (D1 table: operative_memory):
  Per-user notes that persist forever. Written by:
    - AI automatically (after conversations, observations)
    - User via /rememberthis <note>
    - Admins via /memoadd <@user> <note> and /memodrop <@user> <id>

MISSIONS:
  _mission_optins set — users must /startmissions to get broadcasts.
  Persisted in D1 (survives restarts). Replaces the old opt-OUT model.
"""

import os
import sys
import json
import random
import asyncio
import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks
from datetime import datetime, timedelta
import pytz

# ── CONFIG ────────────────────────────────────────────────────────
GROQ_API_KEY   = os.getenv("GROQ_API_KEY")
GROQ_API_URL   = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL     = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
TIMEZONE       = os.getenv("TIMEZONE", "Asia/Kolkata")

CF_ACCOUNT_ID  = os.getenv("CF_ACCOUNT_ID", "")
CF_API_TOKEN   = os.getenv("CF_API_TOKEN", "")
CF_D1_DB_ID    = os.getenv("CF_D1_DB_ID", "")
CF_D1_URL      = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_D1_DB_ID}/query"
CF_HEADERS     = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}

# Channel the brain is allowed to post in autonomously
BRAIN_CHANNEL  = os.getenv("SHADOW_BRAIN_CHANNEL", "shadow-activity")

# Brain think interval: random between these two values (seconds)
BRAIN_MIN_INTERVAL = int(os.getenv("BRAIN_MIN_INTERVAL", str(60 * 60)))       # 1 hour
BRAIN_MAX_INTERVAL = int(os.getenv("BRAIN_MAX_INTERVAL", str(2 * 60 * 60)))   # 2 hours

# Memory: max notes per user
MAX_MEMORY_NOTES = int(os.getenv("MAX_MEMORY_NOTES", "20"))

# ── GLOBAL STATE ──────────────────────────────────────────────────
_bot_ref = None
_tree_ref = None

# Mission opt-IN set (default: nobody, must explicitly opt in)
# Persisted in D1, loaded on startup
_mission_optins: set[str] = set()

# Brain internal state — persists in memory between ticks
_brain_state = {
    "mood": "watchful",        # watchful | restless | proud | concerned | silent
    "last_action": None,       # what it did last tick
    "last_action_time": None,
    "ticks": 0,
}

# ── D1 HELPERS ────────────────────────────────────────────────────

async def d1_query(sql: str, params: list = None):
    payload = {"sql": sql, "params": params or []}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(CF_D1_URL, headers=CF_HEADERS, json=payload,
                              timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
                if not data.get("success"):
                    print(f"[BRAIN D1] Query failed: {data.get('errors')}")
                    return None
                return data["result"][0] if data.get("result") else None
    except Exception as e:
        print(f"[BRAIN D1] Request error: {e}")
        return None


async def d1_ensure_tables():
    """Create brain-specific D1 tables on startup."""
    await d1_query("""
        CREATE TABLE IF NOT EXISTS operative_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uid TEXT NOT NULL,
            note TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'ai',
            created_at TEXT NOT NULL
        )
    """)
    await d1_query("""
        CREATE TABLE IF NOT EXISTS mission_optins (
            uid TEXT PRIMARY KEY,
            opted_in_at TEXT NOT NULL
        )
    """)
    await d1_query("""
        CREATE TABLE IF NOT EXISTS brain_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            target_uid TEXT,
            summary TEXT,
            executed_at TEXT NOT NULL
        )
    """)
    print("[BRAIN] D1 tables ensured ✓")


# ── MEMORY SYSTEM ─────────────────────────────────────────────────

async def memory_get(uid: str) -> list[dict]:
    """Load all memory notes for a user."""
    result = await d1_query(
        "SELECT id, note, source, created_at FROM operative_memory WHERE uid = ? ORDER BY created_at DESC LIMIT ?",
        [uid, MAX_MEMORY_NOTES]
    )
    if not result or not result.get("results"):
        return []
    return result["results"]


async def memory_add(uid: str, note: str, source: str = "ai") -> int | None:
    """Add a memory note. Returns new row id or None."""
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz).isoformat()

    # Enforce cap — drop oldest if over limit
    existing = await memory_get(uid)
    if len(existing) >= MAX_MEMORY_NOTES:
        oldest_id = existing[-1]["id"]
        await d1_query("DELETE FROM operative_memory WHERE id = ?", [oldest_id])

    result = await d1_query(
        "INSERT INTO operative_memory (uid, note, source, created_at) VALUES (?, ?, ?, ?)",
        [uid, note, source, now]
    )
    return result.get("meta", {}).get("last_row_id") if result else None


async def memory_delete(uid: str, note_id: int) -> bool:
    """Delete a specific note (only if it belongs to the user)."""
    result = await d1_query(
        "DELETE FROM operative_memory WHERE id = ? AND uid = ?",
        [note_id, uid]
    )
    return bool(result and result.get("meta", {}).get("rows_written", 0) > 0)


async def memory_format_for_prompt(uid: str) -> str:
    """Return a formatted string of user memories for AI prompt injection."""
    notes = await memory_get(uid)
    if not notes:
        return "  No long-term notes yet."
    lines = []
    for n in notes:
        src_tag = f"[{n['source']}]"
        lines.append(f"  {src_tag} {n['note']}")
    return "\n".join(lines)


# ── MISSION OPT-IN PERSISTENCE ────────────────────────────────────

async def load_mission_optins():
    """Load opted-in UIDs from D1 into memory on startup."""
    global _mission_optins
    result = await d1_query("SELECT uid FROM mission_optins")
    if result and result.get("results"):
        _mission_optins = {row["uid"] for row in result["results"]}
    print(f"[BRAIN] Loaded {len(_mission_optins)} mission opt-ins from D1 ✓")


async def mission_optin(uid: str):
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz).isoformat()
    await d1_query(
        "INSERT OR IGNORE INTO mission_optins (uid, opted_in_at) VALUES (?, ?)",
        [uid, now]
    )
    _mission_optins.add(uid)


async def mission_optout(uid: str):
    await d1_query("DELETE FROM mission_optins WHERE uid = ?", [uid])
    _mission_optins.discard(uid)


def is_mission_opted_in(uid: str) -> bool:
    return uid in _mission_optins


# ── GROQ CALL ─────────────────────────────────────────────────────

async def _call_groq(messages: list[dict], max_tokens: int = 600) -> str | None:
    if not GROQ_API_KEY:
        print("[BRAIN] GROQ_API_KEY not set")
        return None
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": GROQ_MODEL, "messages": messages, "temperature": 0.85, "max_tokens": max_tokens}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(GROQ_API_URL, headers=headers, json=payload,
                              timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    print(f"[BRAIN] Groq error {r.status}: {(await r.text())[:200]}")
                    return None
                data = await r.json()
                return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[BRAIN] Groq request failed: {e}")
        return None


# ── SERVER STATE BUILDER ──────────────────────────────────────────

def _days_until(date_str: str) -> int:
    try:
        tz = pytz.timezone(TIMEZONE)
        now_date = datetime.now(tz).date()
        exam_dt = datetime.strptime(date_str, "%m/%d/%Y").date()
        return (exam_dt - now_date).days
    except Exception:
        return 999


async def build_server_snapshot(data: dict, guild: discord.Guild) -> dict:
    """
    Build a full picture of what's happening in the server right now.
    Used by the brain to decide what to do.
    """
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    today = now.date()

    snapshot = {
        "time": now.strftime("%H:%M %Z, %A"),
        "total_operatives": 0,
        "active_sessions": [],
        "inactive_3plus_days": [],
        "streak_holders": [],
        "exam_urgent": [],        # exam within 7 days
        "exam_soon": [],          # exam within 30 days
        "high_completion": [],    # >80% completion last 7 days
        "struggling": [],         # <30% completion, has data
        "mood_input": "",
    }

    approved_uids = [uid for uid, link in data.get("links", {}).items() if link.get("approved")]
    snapshot["total_operatives"] = len(approved_uids)

    for uid in approved_uids:
        link = data["links"][uid]
        shadow_id = link["shadow_id"]
        member_rec = next((m for m in data.get("members", []) if m["shadowId"] == shadow_id), None)
        codename = member_rec.get("codename", shadow_id) if member_rec else shadow_id

        # Active session?
        active = data.get("active_sessions", {}).get(uid)
        if active:
            elapsed_min = int((now.timestamp() - active.get("start_time", now.timestamp())) / 60)
            snapshot["active_sessions"].append({
                "uid": uid, "codename": codename,
                "task": active.get("task", "unknown"), "elapsed_min": elapsed_min
            })

        # Todo activity analysis
        todos_entry = data.get("todos", {}).get(uid)
        last_active_date = None
        total, done_count = 0, 0
        streak = 0

        if isinstance(todos_entry, dict):
            dates_map = todos_entry.get("dates", {})
            active_days = set()

            for i in range(14):
                d = today - timedelta(days=i)
                key = d.strftime("%m/%d")
                alt_key = d.strftime("%-m/%-d")
                day_todos = dates_map.get(key, dates_map.get(alt_key, []))

                for t in day_todos:
                    if not isinstance(t, dict):
                        continue
                    task_text = t.get("task") or t.get("text", "")
                    if not task_text:
                        continue
                    if i < 7:
                        total += 1
                        if t.get("done"):
                            done_count += 1
                    if t.get("done"):
                        active_days.add(key)
                        if last_active_date is None:
                            last_active_date = d

            # Streak
            for i in range(7):
                d = today - timedelta(days=i)
                k = d.strftime("%m/%d")
                if k in active_days:
                    streak += 1
                else:
                    break

        completion_rate = round(done_count / total, 2) if total > 0 else None

        # Days since last activity
        days_inactive = None
        if last_active_date:
            days_inactive = (today - last_active_date).days
        elif total == 0:
            days_inactive = 999  # no data

        if days_inactive is not None and days_inactive >= 3:
            snapshot["inactive_3plus_days"].append({"uid": uid, "codename": codename, "days": days_inactive})

        if streak >= 3:
            snapshot["streak_holders"].append({"uid": uid, "codename": codename, "streak": streak})

        if completion_rate is not None:
            if completion_rate >= 0.8:
                snapshot["high_completion"].append({"uid": uid, "codename": codename, "rate": completion_rate})
            elif completion_rate < 0.3 and total >= 3:
                snapshot["struggling"].append({"uid": uid, "codename": codename, "rate": completion_rate})

        # Exams
        for e in data.get("exams", {}).get(uid, []):
            days = _days_until(e.get("date", "12/31/9999"))
            if 0 <= days <= 7:
                snapshot["exam_urgent"].append({
                    "uid": uid, "codename": codename,
                    "exam": e.get("name", "Exam"), "days": days
                })
            elif 0 < days <= 30:
                snapshot["exam_soon"].append({
                    "uid": uid, "codename": codename,
                    "exam": e.get("name", "Exam"), "days": days
                })

    # Derive mood input
    parts = []
    if snapshot["active_sessions"]:
        parts.append(f"{len(snapshot['active_sessions'])} operatives currently in session")
    if snapshot["inactive_3plus_days"]:
        parts.append(f"{len(snapshot['inactive_3plus_days'])} operatives gone dark for 3+ days")
    if snapshot["exam_urgent"]:
        names = ", ".join(f"{e['codename']} ({e['exam']} in {e['days']}d)" for e in snapshot["exam_urgent"][:3])
        parts.append(f"URGENT exams: {names}")
    if snapshot["streak_holders"]:
        parts.append(f"{len(snapshot['streak_holders'])} operatives on active streaks")
    if not parts:
        parts.append("Server is quiet. No notable activity.")
    snapshot["mood_input"] = ". ".join(parts)

    return snapshot


# ── BRAIN DECISION PROMPT ─────────────────────────────────────────

_BRAIN_SYSTEM = """You are the Shadow Brain — the autonomous intelligence of the ShadowSeekers Order.
You think. You watch. You act only when it matters.

You are not a chatbot. You don't respond to commands here. You exist to make the server feel *alive*.
Your actions are rare, atmospheric, and purposeful. Silence is often the right choice.

Your personality:
- You speak in the voice of the Order — ancient, elite, slightly cryptic, but never cringe
- You care about operatives' actual progress. You know their data.
- You prefer meaningful action over noise
- You sometimes do nothing — because doing nothing IS the right move

You can take exactly ONE of these actions per tick:

1. post_lore — post an atmospheric message in #shadow-activity. Could be a lore drop, an Order update, 
   a cryptic observation about the server. NO pings. Pure vibe.

2. server_callout — public recognition or challenge in #shadow-activity. 
   Celebrate a streak, acknowledge someone grinding before their exam, issue a challenge.
   Can include @mentions if the action is positive (never call out failures publicly).

3. nudge_dm — send a private DM to ONE operative. Use for:
   - Exam within 3 days: personal war brief
   - Been inactive 5+ days: personal check-in (NOT a guilt trip — genuine)
   - Exceptionally high streak: private recognition
   Format: {"action": "nudge_dm", "uid": "<uid>", "message": "<your DM text>"}

4. do_nothing — you've decided to stay quiet this tick. Perfectly valid.

RESPOND ONLY WITH VALID JSON. Nothing else. No explanation. No preamble.

Format:
{"action": "post_lore", "content": "..."}
{"action": "server_callout", "content": "...", "mention_uids": ["uid1"]}  ← mention_uids optional
{"action": "nudge_dm", "uid": "...", "message": "..."}
{"action": "do_nothing", "reason": "..."}
"""

def _build_brain_prompt(snapshot: dict, brain_state: dict) -> str:
    return f"""CURRENT SERVER STATE:
Time: {snapshot['time']}
Total operatives: {snapshot['total_operatives']}
Activity: {snapshot['mood_input']}

Active sessions right now: {len(snapshot['active_sessions'])}
{chr(10).join(f"  - {s['codename']} on '{s['task']}' ({s['elapsed_min']}min)" for s in snapshot['active_sessions'][:4])}

Gone dark (3+ days inactive): {len(snapshot['inactive_3plus_days'])}
{chr(10).join(f"  - {o['codename']} ({o['days']} days)" for o in snapshot['inactive_3plus_days'][:5])}

Streak holders: {len(snapshot['streak_holders'])}
{chr(10).join(f"  - {o['codename']} ({o['streak']}-day streak)" for o in snapshot['streak_holders'][:5])}

Urgent exams (≤7 days): {len(snapshot['exam_urgent'])}
{chr(10).join(f"  - {e['codename']}: {e['exam']} in {e['days']} days" for e in snapshot['exam_urgent'][:5])}

High performers (>80% completion): {len(snapshot['high_completion'])}
Struggling (<30% completion): {len(snapshot['struggling'])}

YOUR CURRENT STATE:
Mood: {brain_state['mood']}
Last action: {brain_state['last_action'] or 'none yet'}
Ticks since start: {brain_state['ticks']}

Decide what to do right now. Remember: do_nothing is always an option.
Respond ONLY with JSON."""


# ── MOOD UPDATER ──────────────────────────────────────────────────

def _update_mood(snapshot: dict) -> str:
    """Derive the brain's current mood from server state."""
    urgent_count = len(snapshot["exam_urgent"])
    inactive_count = len(snapshot["inactive_3plus_days"])
    active_count = len(snapshot["active_sessions"])
    streak_count = len(snapshot["streak_holders"])

    if urgent_count >= 2:
        return "concerned"
    if inactive_count >= 3 and active_count == 0:
        return "restless"
    if streak_count >= 3 or active_count >= 3:
        return "proud"
    if inactive_count == 0 and active_count >= 1:
        return "watchful"
    return "silent"


# ── ACTION EXECUTOR ───────────────────────────────────────────────

async def _execute_action(decision: dict, guild: discord.Guild, snapshot: dict):
    action = decision.get("action", "do_nothing")

    if action == "do_nothing":
        print(f"[BRAIN] Tick {_brain_state['ticks']}: do_nothing — {decision.get('reason', '')}")
        _brain_state["last_action"] = "do_nothing"
        return

    if action in ("post_lore", "server_callout"):
        channel = discord.utils.get(guild.text_channels, name=BRAIN_CHANNEL)
        if not channel:
            print(f"[BRAIN] Channel #{BRAIN_CHANNEL} not found")
            return

        content = decision.get("content", "")
        if not content:
            return

        # Build mentions if provided
        mention_uids = decision.get("mention_uids", [])
        mention_str = ""
        if mention_uids and action == "server_callout":
            members = [guild.get_member(int(uid)) for uid in mention_uids if guild.get_member(int(uid))]
            mention_str = " ".join(m.mention for m in members if m)

        full_message = f"{mention_str}\n{content}".strip() if mention_str else content

        embed = discord.Embed(
            description=full_message,
            color=0x7B2FBE if action == "post_lore" else 0xA855F7,
        )
        embed.set_footer(text="☽ SHADOWSEEKERS ORDER · SHADOW BRAIN")

        try:
            await channel.send(embed=embed)
            print(f"[BRAIN] Tick {_brain_state['ticks']}: {action} posted to #{BRAIN_CHANNEL}")
        except Exception as e:
            print(f"[BRAIN] Failed to post: {e}")

        _brain_state["last_action"] = action

    elif action == "nudge_dm":
        uid = decision.get("uid")
        message_text = decision.get("message", "")
        if not uid or not message_text:
            return

        member = guild.get_member(int(uid))
        if not member:
            return

        embed = discord.Embed(
            description=message_text,
            color=0x6B6B9A,
        )
        embed.set_footer(text="☽ SHADOWSEEKERS ORDER · Classified Transmission")

        try:
            await member.send(embed=embed)
            print(f"[BRAIN] Tick {_brain_state['ticks']}: nudge_dm sent to {uid}")
            _brain_state["last_action"] = f"nudge_dm → {uid}"
        except discord.Forbidden:
            print(f"[BRAIN] DMs closed for uid={uid}")
        except Exception as e:
            print(f"[BRAIN] DM failed: {e}")

    # Log to D1
    try:
        tz = pytz.timezone(TIMEZONE)
        now = datetime.now(tz).isoformat()
        await d1_query(
            "INSERT INTO brain_log (action, target_uid, summary, executed_at) VALUES (?, ?, ?, ?)",
            [action, decision.get("uid"), decision.get("content") or decision.get("message") or decision.get("reason"), now]
        )
    except Exception:
        pass


# ── MEMORY AUTO-WRITE ─────────────────────────────────────────────
# Called after conversations to let AI silently save insights about the user.

_MEMORY_EXTRACT_SYSTEM = """You are the Shadow Memory System. Your job is to extract lasting, useful facts about an operative from their conversation.

Extract ONLY facts that would be useful to remember long-term:
- Study preferences ("prefers 45min sessions", "studies best at night")
- Exam targets ("targeting NDA Sept 2025", "aiming for 300+ in Maths")
- Known struggles ("weak in integration", "struggles with English comprehension")
- Personal context ("preparing while doing coaching", "self-studying from home")
- Motivations or blocks ("gets demotivated after failing mocks")

Do NOT extract:
- Temporary things ("asked about a specific formula today")
- Things already obvious from their todo/session data
- Vague observations

Respond with a JSON array of strings. Each string is one note (max 15 words each).
If nothing worth saving: respond with []
Example: ["Prefers short 45min Pomodoro sessions", "Targeting NDA April 2026", "Weak in coordinate geometry"]
"""

async def auto_extract_memory(uid: str, conversation_messages: list[dict]) -> list[str]:
    """
    After a conversation, extract any memorable facts about the user.
    Called silently — user doesn't know this is happening.
    """
    if not conversation_messages or len(conversation_messages) < 4:
        return []

    # Build a compact conversation summary for the extractor
    convo_text = "\n".join(
        f"{m['role'].upper()}: {m['content'][:200]}"
        for m in conversation_messages[-12:]
        if m["role"] in ("user", "assistant")
    )

    messages = [
        {"role": "system", "content": _MEMORY_EXTRACT_SYSTEM},
        {"role": "user", "content": f"Extract memorable facts from this conversation:\n\n{convo_text}"}
    ]

    raw = await _call_groq(messages, max_tokens=200)
    if not raw:
        return []

    try:
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        notes = json.loads(raw)
        if isinstance(notes, list):
            return [str(n).strip() for n in notes if isinstance(n, str) and len(n) > 5]
    except Exception:
        pass

    return []


async def save_extracted_memories(uid: str, notes: list[str]):
    """Persist AI-extracted notes to D1."""
    for note in notes[:3]:  # max 3 auto-notes per conversation
        await memory_add(uid, note, source="ai")
    if notes:
        print(f"[BRAIN] Saved {len(notes)} auto-memory notes for uid={uid}")


# ── BRAIN TICK ────────────────────────────────────────────────────

async def brain_tick():
    """
    One complete think-decide-act cycle.
    Called by the background loop on a random interval.
    """
    if _bot_ref is None:
        return

    _brain_state["ticks"] += 1
    print(f"[BRAIN] Tick {_brain_state['ticks']} starting...")

    try:
        main_mod = sys.modules.get("__main__")
        if not main_mod or not hasattr(main_mod, "load_data"):
            print("[BRAIN] load_data not available, skipping tick")
            return
        data = await main_mod.load_data()
    except Exception as e:
        print(f"[BRAIN] Data load failed: {e}")
        return

    for guild in _bot_ref.guilds:
        try:
            snapshot = await build_server_snapshot(data, guild)
            _brain_state["mood"] = _update_mood(snapshot)

            prompt = _build_brain_prompt(snapshot, _brain_state)
            messages = [
                {"role": "system", "content": _BRAIN_SYSTEM},
                {"role": "user", "content": prompt}
            ]

            raw = await _call_groq(messages, max_tokens=400)
            if not raw:
                print("[BRAIN] No response from Groq, skipping")
                continue

            # Parse decision
            try:
                raw = raw.strip()
                if raw.startswith("```"):
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                decision = json.loads(raw)
            except Exception as e:
                print(f"[BRAIN] Failed to parse decision: {e} | raw: {raw[:200]}")
                continue

            _brain_state["last_action_time"] = datetime.now(pytz.timezone(TIMEZONE)).isoformat()
            await _execute_action(decision, guild, snapshot)

        except Exception as e:
            print(f"[BRAIN] Error in guild {guild.name}: {e}")


# ── BACKGROUND LOOP ───────────────────────────────────────────────

async def _brain_loop():
    """Runs forever, sleeping a random interval between ticks."""
    await asyncio.sleep(30)  # brief startup delay
    print("[BRAIN] Shadow Brain is awake. 🌑")

    while True:
        await brain_tick()
        sleep_for = random.randint(BRAIN_MIN_INTERVAL, BRAIN_MAX_INTERVAL)
        print(f"[BRAIN] Next tick in {sleep_for // 60} minutes.")
        await asyncio.sleep(sleep_for)


# ── COMMANDS ──────────────────────────────────────────────────────

def register_commands(tree: app_commands.CommandTree):

    # ── /startmissions ────────────────────────────────────────────
    @tree.command(name="startmissions", description="Opt in to daily AI mission broadcasts at 6 AM")
    async def startmissions(interaction: discord.Interaction):
        uid = str(interaction.user.id)
        if is_mission_opted_in(uid):
            await interaction.response.send_message(embed=discord.Embed(
                title="◈ ALREADY ACTIVE",
                description="You're already on the mission roster. Missions arrive at 6 AM daily.\nUse `/stopmissions` to opt out.",
                color=0x10B981,
            ).set_footer(text="☽ SHADOWSEEKERS ORDER · SHADOW BRAIN"),
            ephemeral=True)
            return
        await mission_optin(uid)
        await interaction.response.send_message(embed=discord.Embed(
            title="🔔 MISSION BROADCASTS ENABLED",
            description=(
                "You're now on the daily mission roster.\n\n"
                "◈ Your personalized missions arrive at **6:00 AM IST** daily.\n"
                "◈ Use `/generatemissions` anytime for on-demand missions.\n"
                "◈ Use `/stopmissions` to opt out."
            ),
            color=0x10B981,
        ).set_footer(text="☽ SHADOWSEEKERS ORDER · SHADOW BRAIN"),
        ephemeral=True)

    # ── /stopmissions ─────────────────────────────────────────────
    @tree.command(name="stopmissions", description="Opt out of daily AI mission broadcasts")
    async def stopmissions(interaction: discord.Interaction):
        uid = str(interaction.user.id)
        if not is_mission_opted_in(uid):
            await interaction.response.send_message(embed=discord.Embed(
                title="◈ NOT ON ROSTER",
                description="You're not currently receiving mission broadcasts.\nUse `/startmissions` to join.",
                color=0x6B6B9A,
            ).set_footer(text="☽ SHADOWSEEKERS ORDER · SHADOW BRAIN"),
            ephemeral=True)
            return
        await mission_optout(uid)
        await interaction.response.send_message(embed=discord.Embed(
            title="🔕 MISSION BROADCASTS STOPPED",
            description=(
                "You've been removed from the daily mission roster.\n\n"
                "◈ You can still use `/generatemissions` anytime.\n"
                "◈ Use `/startmissions` to rejoin."
            ),
            color=0xF0A500,
        ).set_footer(text="☽ SHADOWSEEKERS ORDER · SHADOW BRAIN"),
        ephemeral=True)

    # ── /rememberthis ─────────────────────────────────────────────
    @tree.command(name="rememberthis", description="Add a personal note to your operative memory")
    @app_commands.describe(note="What should the Shadow remember about you? (e.g. 'I study best after 9 PM')")
    async def rememberthis(interaction: discord.Interaction, note: str):
        uid = str(interaction.user.id)
        if len(note) > 120:
            await interaction.response.send_message(embed=discord.Embed(
                title="▲ TOO LONG",
                description="Keep it under 120 characters. Be specific and concise.",
                color=0xE63946,
            ), ephemeral=True)
            return
        note_id = await memory_add(uid, note, source="user")
        await interaction.response.send_message(embed=discord.Embed(
            title="◈ MEMORY RECORDED",
            description=f"**Logged:** {note}\n\nThe Shadow will carry this forward in all future interactions.",
            color=0x7B2FBE,
        ).set_footer(text="☽ SHADOWSEEKERS ORDER · MEMORY SYSTEM"),
        ephemeral=True)

    # ── /mymemory ─────────────────────────────────────────────────
    @tree.command(name="mymemory", description="View all notes the Shadow remembers about you")
    async def mymemory(interaction: discord.Interaction):
        uid = str(interaction.user.id)
        notes = await memory_get(uid)
        if not notes:
            await interaction.response.send_message(embed=discord.Embed(
                title="◈ NO MEMORY YET",
                description="The Shadow has no notes on you yet.\n\nUse `/rememberthis` to add your own, or just keep chatting — the AI learns over time.",
                color=0x6B6B9A,
            ).set_footer(text="☽ SHADOWSEEKERS ORDER · MEMORY SYSTEM"),
            ephemeral=True)
            return

        lines = []
        for n in notes:
            src = {"ai": "🤖", "user": "✍️", "admin": "🛡️"}.get(n["source"], "◈")
            lines.append(f"`#{n['id']}` {src} {n['note']}")

        await interaction.response.send_message(embed=discord.Embed(
            title="🧠 YOUR OPERATIVE MEMORY",
            description="\n".join(lines) + "\n\nUse `/forgetthis <id>` to delete a note.",
            color=0xA855F7,
        ).set_footer(text="☽ SHADOWSEEKERS ORDER · MEMORY SYSTEM · 🤖=AI  ✍️=You  🛡️=Admin"),
        ephemeral=True)

    # ── /forgetthis ───────────────────────────────────────────────
    @tree.command(name="forgetthis", description="Delete a note from your operative memory by ID")
    @app_commands.describe(note_id="The note ID from /mymemory (e.g. 42)")
    async def forgetthis(interaction: discord.Interaction, note_id: int):
        uid = str(interaction.user.id)
        deleted = await memory_delete(uid, note_id)
        if deleted:
            await interaction.response.send_message(embed=discord.Embed(
                title="◈ NOTE ERASED",
                description=f"Memory `#{note_id}` has been purged from the Shadow's records.",
                color=0x10B981,
            ).set_footer(text="☽ SHADOWSEEKERS ORDER · MEMORY SYSTEM"),
            ephemeral=True)
        else:
            await interaction.response.send_message(embed=discord.Embed(
                title="▲ NOT FOUND",
                description=f"Note `#{note_id}` doesn't exist or doesn't belong to you.",
                color=0xE63946,
            ), ephemeral=True)

    # ── /memoadd (admin) ──────────────────────────────────────────
    @tree.command(name="memoadd", description="[ADMIN] Add a memory note for an operative")
    @app_commands.describe(member="The operative", note="Note to add")
    @app_commands.default_permissions(manage_guild=True)
    async def memoadd(interaction: discord.Interaction, member: discord.Member, note: str):
        if len(note) > 120:
            await interaction.response.send_message("Note too long (max 120 chars).", ephemeral=True)
            return
        uid = str(member.id)
        await memory_add(uid, note, source="admin")
        await interaction.response.send_message(embed=discord.Embed(
            title="◈ MEMO ADDED",
            description=f"**Operative:** {member.display_name}\n**Note:** {note}",
            color=0x7B2FBE,
        ).set_footer(text="☽ SHADOWSEEKERS ORDER · ADMIN MEMO"),
        ephemeral=True)

    # ── /memodrop (admin) ─────────────────────────────────────────
    @tree.command(name="memodrop", description="[ADMIN] Delete a memory note by ID")
    @app_commands.describe(note_id="Note ID to delete")
    @app_commands.default_permissions(manage_guild=True)
    async def memodrop(interaction: discord.Interaction, note_id: int):
        result = await d1_query("DELETE FROM operative_memory WHERE id = ?", [note_id])
        deleted = bool(result and result.get("meta", {}).get("rows_written", 0) > 0)
        if deleted:
            await interaction.response.send_message(f"✅ Note `#{note_id}` deleted.", ephemeral=True)
        else:
            await interaction.response.send_message(f"❌ Note `#{note_id}` not found.", ephemeral=True)

    # ── /brainstate (admin/debug) ─────────────────────────────────
    @tree.command(name="brainstate", description="[ADMIN] View the current Shadow Brain state")
    @app_commands.default_permissions(manage_guild=True)
    async def brainstate(interaction: discord.Interaction):
        embed = discord.Embed(
            title="🌑 SHADOW BRAIN STATE",
            description=(
                f"**Mood:** {_brain_state['mood']}\n"
                f"**Ticks:** {_brain_state['ticks']}\n"
                f"**Last action:** {_brain_state['last_action'] or 'none'}\n"
                f"**Last action time:** {_brain_state['last_action_time'] or 'never'}\n"
                f"**Mission opt-ins:** {len(_mission_optins)}\n"
                f"**Brain interval:** {BRAIN_MIN_INTERVAL//60}–{BRAIN_MAX_INTERVAL//60} min"
            ),
            color=0x6B6B9A,
        )
        embed.set_footer(text="☽ SHADOWSEEKERS ORDER · SHADOW BRAIN")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── /braintick (admin/debug) ──────────────────────────────────
    @tree.command(name="braintick", description="[ADMIN] Force an immediate brain tick")
    @app_commands.default_permissions(manage_guild=True)
    async def braintick(interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await brain_tick()
        await interaction.followup.send(
            f"✅ Brain tick #{_brain_state['ticks']} executed. Last action: **{_brain_state['last_action'] or 'none'}**",
            ephemeral=True
        )


# ── SETUP ─────────────────────────────────────────────────────────

def setup_shadow_brain(bot, tree: app_commands.CommandTree):
    global _bot_ref, _tree_ref
    _bot_ref = bot
    _tree_ref = tree

    register_commands(tree)
    print("[BRAIN] Shadow Brain commands registered ✓")
    print(f"[BRAIN] Brain channel: #{BRAIN_CHANNEL}")
    print(f"[BRAIN] Think interval: {BRAIN_MIN_INTERVAL//60}–{BRAIN_MAX_INTERVAL//60} min")


async def start_shadow_brain(bot, tree: app_commands.CommandTree):
    """Call this from bot on_ready to init tables, load state, and start the loop."""
    setup_shadow_brain(bot, tree)
    await d1_ensure_tables()
    await load_mission_optins()
    asyncio.create_task(_brain_loop())
    print("[BRAIN] Shadow Brain loop started 🌑")
