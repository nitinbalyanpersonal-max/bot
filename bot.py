"""
╔══════════════════════════════════════════════════════╗
║         SHADOW BOT · ShadowSeekers Order             ║
║   Objective tracking · Echo management · GAS sync    ║
║   Study sessions · VC tracking · Shadow Grind badge  ║
║   Exam countdown · /exam command                     ║
╚══════════════════════════════════════════════════════╝

COMMANDS (all slash commands):
  /todo add <objective>        — log a new objective
  /todo multiadd <objectives>  — log multiple objectives at once
  /todo done <number(s)>       — mark objective(s) as fulfilled (comma-separated)
  /todo remove <number>        — remove an objective from your dossier
  /todo list                   — view your active dossier
  /todo clear                  — purge your dossier
  /todo date <MM/DD>           — switch active date (default: today)
  /op add <obj#> <op>          — add an op under an objective
  /op multiadd <obj#> <ops>    — add multiple ops (comma-separated)
  /op done <obj#> <op#s>       — complete op(s) under an objective (comma-separated)
  /op remove <obj#> <op#>      — remove an op from an objective
  /op move <obj#s> <target#>   — convert objectives into ops under target
  /study [task]                — start a focus session (detects VC automatically)
  /pomodoro [task]             — start a 25-min pomodoro session
  /endsession                  — end your active session and submit proof
  /sessions                    — weekly analytics: VC hours, completion rates, prime window
  /setfocuswindow <hour> [min] — set daily focus window for Phantom Alerts (15 min DM warning)
  /echoes                      — reveal your echo count + rank
  /leaderboard                 — top 10 operatives by echo power
  /link <shadow_id> <n>        — bind your identity to a Shadow ID
  /exam add <name> [date]      — add an exam to your profile (auto-fetches date if blank)
  /exam list                   — view all your upcoming exams with countdowns
  /exam remove <number>        — remove an exam
  /exams                       — view server-wide upcoming exams

COMMAND ONLY (HIGH CLEARANCE):
  /approve @operative  — authorize an identity bind request
  /give @operative <amount> — manually channel echoes
  /setbase <number>    — recalibrate the daily echo threshold
  /forceday            — force the midnight echo reckoning
"""

import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import asyncio
import aiohttp
from datetime import datetime, time, date, timedelta
import pytz
import motor.motor_asyncio
import time as time_module
import re
from ai_missions import setup_ai_missions, ai_mission_task
from shadow_ai import (
    handle_mention, setup_shadow_ai,
    ensure_plan_ttl_index, d1_set_tokens, d1_get_tokens,
    STARTING_TOKENS, LINKED_BONUS,
    # Ghost Guide
    ghost_send_welcome, ghost_handle_dm, ghost_is_active,
    # /train
    train_start, train_stop, train_list, train_delete, train_handle_message, train_is_active,
    # /setwelcome
    setwelcome_format, setwelcome_tone, setwelcome_title_override,
    setwelcome_color, setwelcome_banner, setwelcome_preview, setwelcome_formats,
    setwelcome_custom_start, setwelcome_custom_handle_message, welcome_custom_is_active,
    setwelcome_dm_start, setwelcome_dm_handle_message, dm_design_is_active,
    WELCOME_FORMATS,
)

# ── CONFIG ────────────────────────────────────────────────────────
TOKEN        = os.getenv("DISCORD_TOKEN")
GAS_URL      = os.getenv("GAS_URL", "https://script.google.com/macros/s/AKfycbyTadW-WF4vnpaciFv8Qv58ahWSQ7KVmQfxJA75_z5fZN3UEBunnDPAeq_i5jiu35sYjQ/exec")
ADMIN_ROLE   = os.getenv("ADMIN_ROLE", "Admin")
APPROVE_CH   = os.getenv("APPROVE_CHANNEL", "admin-log")
TIMEZONE     = os.getenv("TIMEZONE", "Asia/Kolkata")
EOD_HOUR     = int(os.getenv("EOD_HOUR", "23"))
EOD_MINUTE   = int(os.getenv("EOD_MINUTE", "55"))
MONGO_URI    = os.getenv("MONGO_URI")

# Study session config
ECHO_PER_HOUR        = 3
MILESTONE_BONUSES    = {3: 2, 5: 3, 7: 5}   # hours -> bonus echoes
MAX_SESSION_HOURS    = 7
DAILY_SESSION_CAP    = 31                     # 7*3 + 2+3+5
FOCUS_LOG_CHANNEL    = os.getenv("FOCUS_LOG_CHANNEL", "focus-log")
DEEP_WORK_LOG_CHANNEL = os.getenv("DEEP_WORK_LOG_CHANNEL", "deep-work-logs")
GENERAL_CHANNEL      = os.getenv("GENERAL_CHANNEL", "general")
POMODORO_MINUTES     = 25

# ── LEADERBOARD CONFIG ────────────────────────────────────────────
# All live leaderboards + grind board post in LEADERBOARD_CHANNEL
LEADERBOARD_CHANNEL    = os.getenv("LEADERBOARD_CHANNEL", "leaderboard")
VC_LEADERBOARD_CHANNEL = os.getenv("VC_LEADERBOARD_CHANNEL", "leaderboard")
SHADOWCARD_CHANNEL     = os.getenv("SHADOWCARD_CHANNEL", "shadowcard")

# In-memory cache of pinned message IDs per guild.
# Keys: "echo_{guild_id}", "vc_{guild_id}", "weekly_{guild_id}", "daily_{guild_id}", "grind_{guild_id}"
# Populated from MongoDB on startup so restarts don't cause duplicate posts.
_lb_message_ids: dict = {}


async def _load_lb_message_ids():
    """Load persisted leaderboard message IDs from MongoDB into _lb_message_ids."""
    db = get_db()
    if db is None:
        return
    try:
        doc = await db["lb_state"].find_one({"_id": "message_ids"})
        if doc:
            for k, v in doc.items():
                if k != "_id":
                    _lb_message_ids[k] = v
            print(f"[LB STATE] Loaded {len(_lb_message_ids)} message IDs from MongoDB")
    except Exception as e:
        print(f"[LB STATE] Load error: {e}")


async def _save_lb_message_ids():
    """Persist current _lb_message_ids to MongoDB."""
    db = get_db()
    if db is None:
        return
    try:
        await db["lb_state"].update_one(
            {"_id": "message_ids"},
            {"$set": {k: v for k, v in _lb_message_ids.items()}},
            upsert=True,
        )
    except Exception as e:
        print(f"[LB STATE] Save error: {e}")

# ── MONGODB SETUP ─────────────────────────────────────────────────
_mongo_client = None
_db = None

def get_db():
    global _mongo_client, _db
    if MONGO_URI and _db is None:
        _mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
        _db = _mongo_client["shadowbot"]
    return _db

# ── DATA LOAD/SAVE ────────────────────────────────────────────────
DATA_FILE = "data.json"

def _sanitize_members(raw: list) -> list:
    """Ensure every element in the members list is a dict, not a JSON string."""
    result = []
    for m in raw:
        if isinstance(m, str):
            try:
                parsed = json.loads(m)
                if isinstance(parsed, dict):
                    result.append(parsed)
            except Exception:
                pass
        elif isinstance(m, dict):
            result.append(m)
    return result

def _sanitize_sessions(raw: dict) -> dict:
    """Ensure every session value is a dict, not a JSON string."""
    result = {}
    for uid, sess in raw.items():
        if isinstance(sess, str):
            try:
                parsed = json.loads(sess)
                if isinstance(parsed, dict):
                    result[uid] = parsed
            except Exception:
                pass
        elif isinstance(sess, dict):
            result[uid] = sess
    return result

async def load_data():
    db = get_db()
    if db is not None:
        doc          = await db["config"].find_one({"_id": "main"}) or {}
        members_doc  = await db["members"].find_one({"_id": "list"}) or {}
        sessions_doc = await db["sessions"].find_one({"_id": "active"}) or {}
        raw_members  = members_doc.get("members", [])
        sess_history_doc = await db["session_history"].find_one({"_id": "log"}) or {}
        focus_windows_doc = await db["focus_windows"].find_one({"_id": "windows"}) or {}
        exams_doc    = await db["exams"].find_one({"_id": "list"}) or {}
        vc_time_doc  = await db["vc_time"].find_one({"_id": "totals"}) or {}
        return {
            "base_echo_rate":       doc.get("base_echo_rate", 10),
            "links":                doc.get("links", {}),
            "pending_links":        doc.get("pending_links", {}),
            "todos":                doc.get("todos", {}),
            "members":              _sanitize_members(raw_members),
            "active_sessions":      _sanitize_sessions(sessions_doc.get("sessions", {})),
            "daily_session_echoes": doc.get("daily_session_echoes", {}),
            "session_history":      sess_history_doc.get("history", {}),
            "focus_windows":        focus_windows_doc.get("windows", {}),
            "exams":                exams_doc.get("exams", {}),
            "vc_time":              vc_time_doc.get("totals", {}),
        }
    else:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r") as f:
                raw = json.load(f)
                if "exams" not in raw:
                    raw["exams"] = {}
                return raw
        return {
            "base_echo_rate": 10,
            "links": {},
            "pending_links": {},
            "todos": {},
            "members": [],
            "active_sessions": {},
            "daily_session_echoes": {},
            "session_history": {},
            "focus_windows": {},
            "exams": {},
            "vc_time": {},
        }

async def save_data(data):
    db = get_db()
    if db is not None:
        await db["config"].update_one(
            {"_id": "main"},
            {"$set": {
                "base_echo_rate": data.get("base_echo_rate", 10),
                "links":          data.get("links", {}),
                "pending_links":  data.get("pending_links", {}),
                "todos":          data.get("todos", {}),
                "daily_session_echoes": data.get("daily_session_echoes", {}),
            }},
            upsert=True
        )
        await db["members"].update_one(
            {"_id": "list"},
            {"$set": {"members": data.get("members", [])}},
            upsert=True
        )
        await db["sessions"].update_one(
            {"_id": "active"},
            {"$set": {"sessions": data.get("active_sessions", {})}},
            upsert=True
        )
        await db["session_history"].update_one(
            {"_id": "log"},
            {"$set": {"history": data.get("session_history", {})}},
            upsert=True
        )
        await db["focus_windows"].update_one(
            {"_id": "windows"},
            {"$set": {"windows": data.get("focus_windows", {})}},
            upsert=True
        )
        await db["exams"].update_one(
            {"_id": "list"},
            {"$set": {"exams": data.get("exams", {})}},
            upsert=True
        )
        await db["vc_time"].update_one(
            {"_id": "totals"},
            {"$set": {"totals": data.get("vc_time", {})}},
            upsert=True
        )
    else:
        with open(DATA_FILE, "w") as f:
            json.dump(data, f, indent=2)

# ── BOT SETUP ─────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.voice_states = True

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree
_bot_ref = bot

# ── HELPERS ───────────────────────────────────────────────────────
def is_admin(interaction: discord.Interaction) -> bool:
    if interaction.user.guild_permissions.administrator:
        return True
    return any(r.name == ADMIN_ROLE for r in interaction.user.roles)

def get_shadow_id(user_id: str, data: dict):
    link = data["links"].get(str(user_id))
    if link and link.get("approved"):
        return link["shadow_id"]
    return None

def get_member(shadow_id: str, data: dict):
    return next((m for m in data["members"] if m["shadowId"] == shadow_id), None)

def today_str() -> str:
    tz  = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    return now.strftime("%m/%d")

def get_active_date(uid: str, data: dict) -> str:
    entry = data["todos"].get(uid)
    if isinstance(entry, dict):
        stored = entry.get("active_date", today_str())
        today = today_str()
        try:
            tz = pytz.timezone(TIMEZONE)
            current_year = datetime.now(tz).year
            stored_dt = datetime.strptime(f"{stored}/{current_year}", "%m/%d/%Y")
            today_dt  = datetime.strptime(f"{today}/{current_year}", "%m/%d/%Y")
            if stored_dt.date() < today_dt.date():
                data["todos"][uid]["active_date"] = today
                return today
        except Exception:
            pass
        return stored
    return today_str()

def get_todos_for_date(uid: str, date_key: str, data: dict) -> list:
    entry = data["todos"].get(uid)
    if isinstance(entry, list):
        data["todos"][uid] = {
            "active_date": today_str(),
            "dates": {today_str(): entry}
        }
        return data["todos"][uid]["dates"].get(date_key, [])
    if isinstance(entry, dict):
        return entry.get("dates", {}).get(date_key, [])
    return []

def set_todos_for_date(uid: str, date_key: str, todos: list, data: dict):
    if not isinstance(data["todos"].get(uid), dict):
        data["todos"][uid] = {"active_date": today_str(), "dates": {}}
    data["todos"][uid].setdefault("dates", {})[date_key] = todos

ECHO_TIERS = [
    {"name": "Initiate",  "min": 0,    "color": 0x6B6B9A},
    {"name": "Seeker",    "min": 500,  "color": 0x7B2FBE},
    {"name": "Phantom",   "min": 1500, "color": 0xA855F7},
    {"name": "Wraith",    "min": 3000, "color": 0xE63946},
    {"name": "Voidborn",  "min": 5000, "color": 0xF0A500},
]

PRIORITY_EMOJI = {
    "p1": "♦️",
    "p2": "🔸",
    "p3": "🏷️",
}
DONE_EMOJI   = "☘️"
UNDONE_EMOJI = "○"

def get_tier(echo_count: int):
    tier = ECHO_TIERS[0]
    for t in ECHO_TIERS:
        if echo_count >= t["min"]:
            tier = t
    return tier

def make_embed(title, description="", color=0x7B2FBE):
    e = discord.Embed(title=title, description=description, color=color)
    e.set_footer(text="☽ SHADOWSEEKERS ORDER · DEEP IN THE DARK, I DON'T NEED THE LIGHT")
    return e

def format_duration(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}h {m}min"
    elif m > 0:
        return f"{m}min {s}s"
    return f"{s}s"

def make_progress_bar(elapsed_seconds: int, total_seconds: int, width: int = 10) -> str:
    pct = min(elapsed_seconds / total_seconds, 1.0) if total_seconds > 0 else 0
    filled = round(pct * width)
    return "▓" * filled + "░" * (width - filled)

# ── SESSION ECHO CALCULATOR ───────────────────────────────────────
def calculate_session_echoes(duration_seconds: int, daily_earned_so_far: int) -> dict:
    hours_completed = int(duration_seconds // 3600)
    hours_completed = min(hours_completed, MAX_SESSION_HOURS)

    base_echoes = hours_completed * ECHO_PER_HOUR
    bonus_echoes = 0
    milestones_hit = []

    for milestone_hr, bonus in MILESTONE_BONUSES.items():
        if hours_completed >= milestone_hr:
            bonus_echoes += bonus
            milestones_hit.append((milestone_hr, bonus))

    total = base_echoes + bonus_echoes
    remaining_cap = max(0, DAILY_SESSION_CAP - daily_earned_so_far)
    awarded = min(total, remaining_cap)

    return {
        "hours": hours_completed,
        "base": base_echoes,
        "bonus": bonus_echoes,
        "total": total,
        "awarded": awarded,
        "milestones": milestones_hit,
        "capped": awarded < total,
    }

# ── GAS SYNC ──────────────────────────────────────────────────────
async def pull_from_gas(data: dict):
    """MongoDB is primary — GAS is push-only backup."""
    db = get_db()
    if db is not None:
        members_doc = await db["members"].find_one({"_id": "list"}) or {}
        raw = members_doc.get("members", [])
        if raw:
            data["members"] = _sanitize_members(raw)
            return True
    return bool(data.get("members"))


async def _push_members_to_gas_bg(data: dict):
    """Push members to GAS in background — fire and forget backup."""
    if not GAS_URL:
        return
    try:
        payload = json.dumps({
            "action": "write",
            "members": [
                {**m, "shadowCardImage": None,
                 "passphrase": data.get("credentials", {}).get(m["shadowId"], "")}
                for m in data["members"]
            ]
        })
        async with aiohttp.ClientSession() as session:
            async with session.post(
                GAS_URL, data=payload,
                headers={"Content-Type": "text/plain"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                print(f"[GAS PUSH BG] Status: {resp.status}")
    except Exception as e:
        print(f"[GAS PUSH BG ERROR] {e}")

async def push_to_gas(data: dict):
    """GAS backup — fire and forget, non-blocking."""
    asyncio.create_task(_push_members_to_gas_bg(data))
    return True

async def push_proof_to_gas(session_data: dict) -> bool:
    """Push session proof to GAS in background — fire and forget."""
    asyncio.create_task(_push_proof_to_gas_bg(session_data))
    return True


async def _push_proof_to_gas_bg(session_data: dict):
    if not GAS_URL:
        return
    try:
        payload = json.dumps({
            "action": "submitProof",
            "shadowId":    session_data["shadow_id"],
            "codename":    session_data["codename"],
            "task":        session_data["task"],
            "duration":    format_duration(session_data["duration_seconds"]),
            "hours":       session_data["hours"],
            "echoes":      session_data["awarded"],
            "proofLink":   session_data.get("proof_link", ""),
            "sessionType": session_data.get("session_type", "study"),
            "date":        today_str(),
            "timestamp":   datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d %H:%M"),
        })
        async with aiohttp.ClientSession() as session:
            async with session.post(
                GAS_URL, data=payload,
                headers={"Content-Type": "text/plain"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                print(f"[GAS PROOF BG] Status: {resp.status}")
    except Exception as e:
        print(f"[GAS PROOF BG ERROR] {e}")

async def create_member_on_gas(member: dict) -> bool:
    """Push new member to GAS in background — fire and forget."""
    asyncio.create_task(_create_member_on_gas_bg(member))
    return True


async def _create_member_on_gas_bg(member: dict):
    if not GAS_URL:
        return
    try:
        payload = json.dumps({
            "action":    "create",
            "shadowId":  member["shadowId"],
            "codename":  member["codename"],
            "discordId": member["discordId"],
            "echoCount": member.get("echoCount", 0),
        })
        async with aiohttp.ClientSession() as session:
            async with session.post(
                GAS_URL, data=payload,
                headers={"Content-Type": "text/plain"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                text = await resp.text()
                print(f"[GAS CREATE BG] Status: {resp.status} | {text[:100]}")
    except Exception as e:
        print(f"[GAS CREATE BG ERROR] {e}")

# ── ACTIVE SESSION TIMER LOOP ─────────────────────────────────────
_session_messages = {}   # uid -> discord.Message
# NOTE: grind board now lives in LEADERBOARD_CHANNEL, edited in-place via _lb_message_ids["grind_{guild_id}"]

async def update_live_board(guild: discord.Guild):
    """Edit the live grind board in-place inside the leaderboard channel."""
    data     = await load_data()
    now      = time_module.time()
    sessions = data.get("active_sessions", {})

    lb_ch = discord.utils.get(guild.text_channels, name=LEADERBOARD_CHANNEL)
    if not lb_ch:
        return

    key = f"grind_{guild.id}"

    if not sessions:
        # No one studying — clear the board message if it exists
        msg_id = _lb_message_ids.get(key)
        if msg_id:
            try:
                msg = await lb_ch.fetch_message(msg_id)
                await msg.delete()
            except Exception:
                pass
            _lb_message_ids.pop(key, None)
            await _save_lb_message_ids()
        return

    ist      = pytz.timezone(TIMEZONE)
    now_ist  = datetime.fromtimestamp(now, tz=ist)
    updated  = now_ist.strftime("%I:%M %p")
    count    = len(sessions)

    header_embed = make_embed(
        f"🦇 GRIND BOARD · {count} OPERATIVE{'S' if count != 1 else ''} LOCKED IN",
        "",
        color=0x7B2FBE
    )
    header_embed.set_footer(text=f"Updates every 20s · Last updated {updated} IST")
    embeds = [header_embed]

    for uid, sess in sorted(sessions.items(), key=lambda x: x[1].get("start_time", 0)):
        elapsed   = int(now - sess.get("start_time", now))
        codename  = sess.get("codename", "Unknown")
        task      = sess.get("task", "Unknown task")
        in_vc     = sess.get("in_vc", False)
        sess_type = sess.get("session_type", "study")

        type_icon = "🍅" if sess_type == "pomodoro" else "🦇"
        vc_badge  = " · 🎙️ VC" if in_vc else ""
        time_str  = format_duration(elapsed)
        bar       = make_progress_bar(elapsed % 3600, 3600)

        avatar_url = None
        try:
            member_obj = guild.get_member(int(uid))
            if member_obj and member_obj.display_avatar:
                avatar_url = member_obj.display_avatar.url
        except Exception:
            pass

        op_embed = discord.Embed(
            description=(
                f"┕ *{task}*{vc_badge}\n"
                f"┕ `[{bar}]` {time_str}"
            ),
            color=0xA855F7 if in_vc else 0x7B2FBE,
        )
        op_embed.set_author(name=f"{type_icon} {codename}", icon_url=avatar_url)
        embeds.append(op_embed)

    embeds = embeds[:10]

    msg_id = _lb_message_ids.get(key)
    try:
        if msg_id:
            msg = await lb_ch.fetch_message(msg_id)
            await msg.edit(embeds=embeds)
        else:
            msg = await lb_ch.send(embeds=embeds)
            _lb_message_ids[key] = msg.id
            await _save_lb_message_ids()
    except (discord.NotFound, discord.HTTPException):
        # Message was deleted — send fresh
        try:
            msg = await lb_ch.send(embeds=embeds)
            _lb_message_ids[key] = msg.id
            await _save_lb_message_ids()
        except Exception as e:
            print(f"[GRIND BOARD] Failed to send: {e}")


@tasks.loop(seconds=20)
async def session_ticker():
    """Every 20 seconds: update live timer embeds + check pomodoro end."""
    data = await load_data()
    now  = time_module.time()

    for uid, sess in list(data["active_sessions"].items()):
        try:
            elapsed  = int(now - sess["start_time"])
            is_pomo  = sess["session_type"] == "pomodoro"
            pomo_end = sess.get("pomodoro_end")
            timer_total = sess.get("timer_total")

            # ── Timed session (pomodoro OR study with duration) ──
            if pomo_end and timer_total:
                remaining = max(0, int(pomo_end - now))
                bar       = make_progress_bar(timer_total - remaining, timer_total)
                time_str  = format_duration(remaining)

                if is_pomo:
                    status = "⏰ POMODORO ENDING SOON" if remaining < 120 else "🍅 POMODORO IN PROGRESS"
                    done_label = "🍅 POMODORO COMPLETE"
                    done_body  = "25 minutes locked in. Use `/endsession` to submit proof and claim your echoes."
                else:
                    mins = timer_total // 60
                    status = "⏰ SESSION ENDING SOON" if remaining < 120 else f"⏱️ TIMED SESSION — {mins}min"
                    done_label = "⏱️ TIMER COMPLETE"
                    done_body  = f"{mins}-minute session done. Use `/endsession` to submit proof and claim echoes."

                if remaining == 0:
                    embed = make_embed(
                        done_label,
                        f"**{sess['task']}**\n\n{done_body}",
                        color=0x10B981
                    )
                else:
                    embed = make_embed(
                        status,
                        f"**{sess['task']}**\n\n"
                        f"`[{bar}]` **{time_str} left**\n"
                        f"Elapsed: {format_duration(elapsed)}\n\n"
                        f"{'🔔 Time is almost up! Use `/endsession` to submit proof.' if remaining < 120 else 'Stay locked in. Use `/endsession` when done.'}",
                        color=0xF0A500 if remaining < 120 else 0xA855F7
                    )
                embed.set_author(name=f"Operative: {sess.get('codename', uid)}")

            else:
                # ── FIX: Open-ended study session — full rich embed every tick ──
                hours_done = elapsed // 3600
                next_milestone = None
                for mhr in sorted(MILESTONE_BONUSES.keys()):
                    if hours_done < mhr:
                        next_milestone = mhr
                        break

                secs_to_next_hr = 3600 - (elapsed % 3600)
                bar = make_progress_bar(elapsed % 3600, 3600)

                milestone_note = ""
                if next_milestone:
                    milestone_note = f"\n🎯 Next milestone: **{next_milestone}h** (+{MILESTONE_BONUSES[next_milestone]} bonus echoes)"
                elif hours_done >= MAX_SESSION_HOURS:
                    milestone_note = "\n🏆 **MAX SESSION REACHED** — Submit proof to claim all echoes."

                vc_note = " · 🎙️ In VC" if sess.get("in_vc") else ""
                echoes_so_far = hours_done * ECHO_PER_HOUR

                embed = make_embed(
                    "☽ FOCUS SESSION IN PROGRESS",
                    f"**{sess['task']}**\n\n"
                    f"`[{bar}]` **{format_duration(elapsed)} elapsed**{vc_note}\n"
                    f"Next hour milestone in: **{format_duration(secs_to_next_hr)}**\n"
                    f"Echoes earned so far: **~{echoes_so_far}**{milestone_note}\n\n"
                    f"Use `/endsession` to submit proof and claim echoes.",
                    color=0x7B2FBE
                )
                embed.set_author(name=f"Operative: {sess.get('codename', uid)}")

            # ── FIX: Only EDIT the existing message — never send new ones ──
            msg = _session_messages.get(uid)
            if msg:
                try:
                    await msg.edit(embed=embed)
                except discord.NotFound:
                    # Message was deleted — clear ref but don't spam channel
                    _session_messages.pop(uid, None)
                except Exception as e:
                    print(f"[SESSION TICKER] Edit failed uid={uid}: {e}")
            # If no message ref exists, we simply skip — don't spam the channel

        except Exception as e:
            print(f"[SESSION TICKER ERROR] uid={uid}: {e}")

    # ── Update live group board in general ──
    for guild in bot.guilds:
        try:
            await update_live_board(guild)
        except Exception as e:
            print(f"[LIVE BOARD] update error: {e}")

# ── END OF DAY CALCULATION ────────────────────────────────────────
async def run_end_of_day(guild: discord.Guild, announce=True):
    data    = await load_data()
    base    = data.get("base_echo_rate", 10)
    today   = today_str()
    results = []

    data["daily_session_echoes"] = {}

    for discord_id, link in data["links"].items():
        if not link.get("approved"):
            continue
        shadow_id = link["shadow_id"]
        todos     = get_todos_for_date(discord_id, today, data)

        if not todos:
            earned = 0
            pct    = 0
        else:
            total_weight = len(todos)
            done_weight  = 0.0
            for t in todos:
                ops = t.get("ops", [])
                if ops:
                    done_ops = sum(1 for op in ops if op.get("done"))
                    done_weight += done_ops / len(ops)
                else:
                    if t["done"]:
                        done_weight += 1
            pct    = done_weight / total_weight
            earned = round(base * pct)

        for i, m in enumerate(data["members"]):
            if m["shadowId"] == shadow_id:
                old = int(m.get("echoCount", 0))
                data["members"][i]["echoCount"] = old + earned
                results.append({
                    "shadow_id": shadow_id,
                    "codename":  m.get("codename", shadow_id),
                    "earned":    earned,
                    "pct":       pct,
                    "total":     len(todos),
                    "done":      sum(1 for t in todos if t["done"]),
                    "new_total": old + earned,
                })
                break

        set_todos_for_date(discord_id, today, [], data)

    await save_data(data)
    await push_to_gas(data)

    if announce and results:
        ch = discord.utils.get(guild.text_channels, name="echo-log")
        if not ch:
            ch = discord.utils.get(guild.text_channels, name="general")
        if ch:
            lines = []
            for r in sorted(results, key=lambda x: -x["earned"]):
                bar_filled = round(r["pct"] * 10)
                bar = "█" * bar_filled + "░" * (10 - bar_filled)
                lines.append(
                    f"`{r['shadow_id']}` **{r['codename']}**\n"
                    f"`[{bar}]` {r['done']}/{r['total']} objectives · **+{r['earned']} echoes**"
                )
            embed = make_embed(
                "☽ NIGHTLY ECHO RECKONING",
                "\n\n".join(lines) or "The void recorded no activity this cycle.",
                color=0xF0A500
            )
            embed.set_footer(text=f"☽ SHADOWSEEKERS ORDER · Base resonance: {base} · {datetime.now().strftime('%d %b %Y')}")
            await ch.send(embed=embed)

    return results

# ── SCHEDULED TASK ────────────────────────────────────────────────
@tasks.loop(time=time(hour=EOD_HOUR, minute=EOD_MINUTE, tzinfo=pytz.timezone(TIMEZONE)))
async def daily_echo_task():
    for guild in bot.guilds:
        await run_end_of_day(guild)

# ══════════════════════════════════════════════════════════════════
#  STUDY SESSION COMMANDS
# ══════════════════════════════════════════════════════════════════

async def _start_session(interaction: discord.Interaction, task: str, session_type: str, duration_minutes: int = None):
    """Shared logic for /study and /pomodoro."""
    # FIX: Defer immediately — load_data() can take >3s and expire the interaction
    await interaction.response.defer()

    data      = await load_data()
    uid       = str(interaction.user.id)
    shadow_id = get_shadow_id(uid, data)

    if not shadow_id:
        await interaction.followup.send(
            embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first — use `/link <shadow_id> <n>`.", color=0xE63946)
        )
        return

    if uid in data["active_sessions"]:
        await interaction.followup.send(
            embed=make_embed("▲ SESSION ACTIVE", "You already have an active session. Use `/endsession` to close it first.", color=0xE63946)
        )
        return

    member   = get_member(shadow_id, data)
    codename = member.get("codename", shadow_id) if member else shadow_id

    # Check if user is in VC
    in_vc      = False
    vc_channel = None
    if interaction.guild:
        member_obj = interaction.guild.get_member(interaction.user.id)
        if member_obj and member_obj.voice and member_obj.voice.channel:
            in_vc      = True
            vc_channel = member_obj.voice.channel.name

    now = time_module.time()

    # Determine timer end
    if session_type == "pomodoro":
        pomo_mins   = duration_minutes or POMODORO_MINUTES
        timer_end   = now + (pomo_mins * 60)
        timer_total = pomo_mins * 60
    elif duration_minutes:
        timer_end   = now + (duration_minutes * 60)
        timer_total = duration_minutes * 60
    else:
        timer_end   = None
        timer_total = None

    session = {
        "task":         task,
        "start_time":   now,
        "session_type": session_type,
        "in_vc":        in_vc,
        "vc_channel":   vc_channel or "",
        "channel_id":   str(interaction.channel_id),
        "guild_id":     str(interaction.guild_id),
        "shadow_id":    shadow_id,
        "codename":     codename,
        "pomodoro_end": timer_end,
        "timer_total":  timer_total,
    }
    data["active_sessions"][uid] = session
    await save_data(data)

    vc_note = f"\n🎙️ Detected in **{vc_channel}** — VC bonus active!" if in_vc else "\n💡 Join a VC channel for a higher echo rate."

    if session_type == "pomodoro":
        type_note = f"🍅 **POMODORO** — {duration_minutes or 25} minutes locked."
    elif duration_minutes:
        type_note = f"⏱️ **TIMED SESSION** — {duration_minutes} minutes set."
    else:
        type_note = "☽ **STUDY SESSION** — open-ended."

    bar = make_progress_bar(0, timer_total or 3600)

    embed = make_embed(
        "◉ SESSION STARTED",
        f"**{task}**\n\n"
        f"{type_note}{vc_note}\n\n"
        f"`[{bar}]` **0m elapsed**\n\n"
        f"Echo rate: **{ECHO_PER_HOUR} echoes/hr**\n"
        f"Milestones: 3h +2 · 5h +3 · 7h +5 🏆\n\n"
        f"Use `/endsession` when done to submit proof and claim echoes.",
        color=0x10B981
    )
    embed.set_author(name=f"Operative: {codename}")

    # FIX: use followup.send since we deferred above; original_response() still works for message ref
    msg = await interaction.followup.send(embed=embed, wait=True)
    _session_messages[uid] = msg

    # Post in focus-log channel if different
    focus_ch = discord.utils.get(interaction.guild.text_channels, name=FOCUS_LOG_CHANNEL)
    if focus_ch and focus_ch.id != interaction.channel_id:
        log_embed = make_embed(
            "☽ OPERATIVE LOCKED IN",
            f"{interaction.user.mention} started a {session_type} session\n**{task}**{vc_note}",
            color=0x7B2FBE
        )
        await focus_ch.send(embed=log_embed)


@tree.command(name="study", description="Start a focus session — open-ended or with a timer")
@app_commands.describe(
    task="What are you working on?",
    duration="Timer in minutes (e.g. 30 = 30 minutes, 60 = 1 hour). Leave blank for open-ended."
)
async def study(interaction: discord.Interaction, task: str, duration: int = None):
    if duration is not None and duration < 1:
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID DURATION", "Duration must be at least 1 minute.", color=0xE63946)
        )
        return
    await _start_session(interaction, task, "study", duration_minutes=duration)


@tree.command(name="pomodoro", description="Start a timed Pomodoro session — 25, 50, 90 min or any custom duration")
@app_commands.describe(
    task="What are you working on?",
    duration="Minutes — choose 25/50/90 or type any number up to 300 (default: 25)",
)
@app_commands.choices(duration=[
    app_commands.Choice(name="25 min — Classic Pomodoro", value=25),
    app_commands.Choice(name="50 min — Deep Work",        value=50),
    app_commands.Choice(name="90 min — Flow State",       value=90),
])
async def pomodoro(interaction: discord.Interaction, task: str, duration: int = 25):
    mins = int(duration)
    mins = max(1, min(mins, 300))
    await _start_session(interaction, task, "pomodoro", duration_minutes=mins)


@tree.command(name="endsession", description="End your active session, submit proof, and claim echoes")
@app_commands.describe(
    proof="Upload an image OR paste a link/text as proof of your session",
    attachment="Upload a screenshot or photo as proof (renders in the embed)"
)
async def endsession(interaction: discord.Interaction, proof: str = None, attachment: discord.Attachment = None):
    # FIX: Defer immediately — load_data() can expire the 3s interaction window
    await interaction.response.defer()

    data      = await load_data()
    uid       = str(interaction.user.id)
    shadow_id = get_shadow_id(uid, data)

    if not shadow_id:
        await interaction.followup.send(
            embed=make_embed("▲ NOT LINKED", "No Shadow ID linked.", color=0xE63946)
        )
        return

    sess = data["active_sessions"].get(uid)
    if not sess:
        await interaction.followup.send(
            embed=make_embed("▲ NO ACTIVE SESSION", "You don't have an active session. Start one with `/study` or `/pomodoro`.", color=0xE63946)
        )
        return

    if isinstance(sess, str):
        try:
            sess = json.loads(sess)
            data["active_sessions"][uid] = sess
        except Exception:
            del data["active_sessions"][uid]
            await save_data(data)
            await interaction.followup.send(
                embed=make_embed("▲ SESSION CORRUPTED", "Your session data was corrupted and has been cleared. Start a fresh session with `/study`.", color=0xE63946)
            )
            return

    if not proof and not attachment:
        await interaction.followup.send(
            embed=make_embed("▲ PROOF REQUIRED", "Submit proof to end your session — upload an image or describe what you accomplished.", color=0xE63946)
        )
        return

    try:
        now              = time_module.time()
        duration_seconds = int(now - float(sess["start_time"]))
        today            = today_str()

        daily_key     = f"{uid}_{today}"
        daily_earned  = data.get("daily_session_echoes", {}).get(daily_key, 0)

        echo_info = calculate_session_echoes(duration_seconds, daily_earned)

        sg_count = 0
        new_badge = False
        for i, m in enumerate(data["members"]):
            if m["shadowId"] == shadow_id:
                old = int(m.get("echoCount", 0))
                data["members"][i]["echoCount"] = old + echo_info["awarded"]

                badges = data["members"][i].get("badges", {})
                if isinstance(badges, str):
                    try:
                        badges = json.loads(badges)
                    except Exception:
                        badges = {}
                if not isinstance(badges, dict):
                    badges = {}
                sg_count = badges.get("shadow_grind", 0)
                new_badge = echo_info["hours"] >= MAX_SESSION_HOURS
                if new_badge:
                    sg_count += 1
                    badges["shadow_grind"] = sg_count
                    data["members"][i]["badges"] = badges
                break

        if "daily_session_echoes" not in data:
            data["daily_session_echoes"] = {}
        data["daily_session_echoes"][daily_key] = daily_earned + echo_info["awarded"]

        tz       = pytz.timezone(TIMEZONE)
        now_dt   = datetime.now(tz)
        date_key = now_dt.strftime("%m/%d")
        hour_key = now_dt.hour

        if "session_history" not in data:
            data["session_history"] = {}
        if uid not in data["session_history"]:
            data["session_history"][uid] = []

        history_entry = {
            "date":             date_key,
            "hour":             hour_key,
            "task":             sess.get("task", ""),
            "session_type":     sess.get("session_type", "study"),
            "duration_seconds": duration_seconds,
            "awarded":          echo_info["awarded"],
            "in_vc":            sess.get("in_vc", False),
        }
        data["session_history"][uid].append(history_entry)
        data["session_history"][uid] = data["session_history"][uid][-90:]

        del data["active_sessions"][uid]
        await save_data(data)

        _session_messages.pop(uid, None)

    except Exception as e:
        import traceback
        err_detail = traceback.format_exc()
        print(f"[ENDSESSION ERROR] uid={uid}: {e}\n{err_detail}")
        short_err = type(e).__name__ + ": " + str(e)[:300]
        await interaction.followup.send(
            embed=make_embed(
                "▲ SESSION END ERROR",
                f"Something went wrong ending your session.\n```{short_err}```\nPlease screenshot this and report it.",
                color=0xE63946
            )
        )
        return

    proof_image_url = None
    proof_display   = proof or ""

    if attachment:
        proof_image_url = attachment.url
        proof_display   = attachment.url
    elif proof:
        is_image_url = (
            proof.startswith("http") and (
                any(proof.lower().endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif", ".webp"])
                or "cdn.discordapp.com" in proof
                or "imgur.com" in proof
                or "i.ibb.co" in proof
                or "media.discordapp.net" in proof
            )
        )
        if is_image_url:
            proof_image_url = proof

    milestone_lines = ""
    if echo_info["milestones"]:
        milestone_lines = "\n" + "\n".join(
            f"🏆 **{hr}h milestone** → +{bonus} echoes" for hr, bonus in echo_info["milestones"]
        )

    badge_line = f"\n\n🏅 **SHADOW GRIND BADGE EARNED** — You now hold **{sg_count}** badge(s)." if new_badge else ""
    cap_note   = "\n⚠️ Daily echo cap hit — some echoes withheld." if echo_info["capped"] else ""

    # ── Build task/ops breakdown ──────────────────────────────────
    task_header = f"**`{sess['task']}`**"
    todos_snap  = sess.get("todos_snapshot", [])
    task_block  = task_header
    if todos_snap:
        for t in todos_snap[:8]:
            icon = "✅" if t.get("done") else "◻️"
            task_block += f"\n{icon} {t['text']}"

    # ── Stats block ───────────────────────────────────────────────
    stats_rows  = f"  DURATION    {format_duration(duration_seconds)}  ({echo_info['hours']}h)\n"
    stats_rows += f"  BASE        {echo_info['base']} echoes  ({echo_info['hours']} × {ECHO_PER_HOUR}/h)\n"
    if echo_info.get("bonus"):
        stats_rows += f"  MILESTONE   +{echo_info['bonus']} bonus echoes\n"
    stats_rows += f"  ─────────────────────────────\n"
    stats_rows += f"  AWARDED     {echo_info['awarded']} echoes\n"
    if echo_info["capped"]:
        stats_rows += f"  ⚠ CAP HIT   daily limit reached\n"

    vc_line = "\n🎙️ **VC Bonus active** during this session." if sess.get("in_vc") else ""

    embed = make_embed(
        "☽ SESSION SEALED — ECHOES CLAIMED",
        f"{task_block}\n\n"
        f"```\n{stats_rows}```"
        f"{vc_line}"
        f"{badge_line}",
        color=0xA855F7
    )
    embed.set_author(
        name=f"Operative: {sess['codename']}",
        icon_url=interaction.user.display_avatar.url if interaction.user.display_avatar else None
    )
    embed.set_footer(text="☽ SHADOWSEEKERS ORDER · mission sealed")

    if proof_image_url:
        embed.set_image(url=proof_image_url)
        embed.add_field(name="◈ PROOF SUBMITTED", value=f"[View Image]({proof_image_url})", inline=False)
    elif proof_display:
        embed.add_field(name="◈ PROOF SUBMITTED", value=proof_display[:1024], inline=False)

    await interaction.followup.send(embed=embed)
    asyncio.create_task(refresh_realtime_leaderboards(interaction.guild))

    sess_data = {
        **sess,
        "duration_seconds": duration_seconds,
        "hours":            echo_info["hours"],
        "awarded":          echo_info["awarded"],
        "proof_link":       proof_image_url or "",
        "proof_text":       proof_display if not proof_image_url else "",
        "shadow_id":        shadow_id,
        "codename":         sess.get("codename", shadow_id),
    }

    async def _background_push():
        try:
            await push_proof_to_gas(sess_data)
        except Exception as e:
            print(f"[BG PROOF PUSH ERROR] {e}")
        try:
            await push_to_gas(data)
        except Exception as e:
            print(f"[BG GAS PUSH ERROR] {e}")

    asyncio.create_task(_background_push())

    focus_ch     = discord.utils.get(interaction.guild.text_channels, name=FOCUS_LOG_CHANNEL)
    deep_work_ch = discord.utils.get(interaction.guild.text_channels, name=DEEP_WORK_LOG_CHANNEL)
    general_ch   = discord.utils.get(interaction.guild.text_channels, name=GENERAL_CHANNEL)

    def make_session_log_embed():
        # Task + todos breakdown
        task_val   = f"**`{sess['task']}`**"
        todos_snap = sess.get("todos_snapshot", [])
        if todos_snap:
            for t in todos_snap[:6]:
                icon     = "✅" if t.get("done") else "◻️"
                task_val += f"\n{icon} {t['text']}"

        stype    = sess.get("session_type", "study").upper()
        vc_badge = " · 🎙️ VC" if sess.get("in_vc") else ""
        stats    = (
            f"  TYPE      {stype}{vc_badge}\n"
            f"  TIME      {format_duration(duration_seconds)}\n"
            f"  ECHOES    +{echo_info['awarded']}\n"
        )
        if echo_info.get("bonus"):
            stats += f"  MILESTONE +{echo_info['bonus']} bonus\n"
        if echo_info["capped"]:
            stats += f"  ⚠ CAP HIT  daily limit\n"

        avatar_url = interaction.user.display_avatar.url if interaction.user.display_avatar else None
        e = make_embed(
            "☽ OPERATIVE DEBRIEFED",
            f"{task_val}\n\n```\n{stats}```"
            + (f"\n🏅 **SHADOW GRIND BADGE EARNED** — {sg_count} badge(s) total." if new_badge else ""),
            color=0x7B2FBE
        )
        e.set_author(name=f"Operative: {sess['codename']}", icon_url=avatar_url)
        if avatar_url:
            e.set_thumbnail(url=avatar_url)
        if proof_image_url:
            e.set_image(url=proof_image_url)
        elif proof_display:
            e.add_field(name="◈ PROOF", value=proof_display[:1024], inline=False)
        e.set_footer(text="☽ SHADOWSEEKERS ORDER · mission sealed")
        return e

    announced_channels = set()
    for ch in [focus_ch, deep_work_ch, general_ch]:
        if ch and ch.id != interaction.channel_id and ch.id not in announced_channels:
            announced_channels.add(ch.id)
            await ch.send(embed=make_session_log_embed())


@tree.command(name="sessions", description="View your session stats and weekly analytics")
async def sessions_cmd(interaction: discord.Interaction):
    data      = await load_data()
    uid       = str(interaction.user.id)
    shadow_id = get_shadow_id(uid, data)

    if not shadow_id:
        await interaction.response.send_message(embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first.", color=0xE63946))
        return

    member = get_member(shadow_id, data)
    if not member:
        await interaction.response.send_message(embed=make_embed("▲ NOT FOUND", "No member record found.", color=0xE63946))
        return

    codename   = member.get("codename", shadow_id)
    sg_count   = member.get("badges", {})
    if isinstance(sg_count, str):
        try: sg_count = json.loads(sg_count)
        except: sg_count = {}
    sg_count   = sg_count.get("shadow_grind", 0) if isinstance(sg_count, dict) else 0
    echo_count = int(member.get("echoCount", 0))
    today      = today_str()
    daily_key  = f"{uid}_{today}"
    daily_sess = data.get("daily_session_echoes", {}).get(daily_key, 0)

    history = data.get("session_history", {}).get(uid, [])

    tz = pytz.timezone(TIMEZONE)
    now_dt = datetime.now(tz)
    week_dates = set()
    for i in range(7):
        d = now_dt - timedelta(days=i)
        week_dates.add(d.strftime("%m/%d"))

    week_sessions  = [s for s in history if s.get("date") in week_dates]
    total_secs     = sum(s.get("duration_seconds", 0) for s in week_sessions)
    total_echoes   = sum(s.get("awarded", 0) for s in week_sessions)
    total_sessions = len(week_sessions)
    vc_sessions    = sum(1 for s in week_sessions if s.get("in_vc"))

    if history:
        from collections import Counter
        hour_counts = Counter(s.get("hour", 0) for s in history)
        peak_hour   = hour_counts.most_common(1)[0][0]
        def fmt_hour(h):
            suffix = "AM" if h < 12 else "PM"
            h12    = h % 12 or 12
            return f"{h12}:00 {suffix}"
        prime_window = f"{fmt_hour(peak_hour)} – {fmt_hour((peak_hour + 1) % 24)}"
    else:
        prime_window = "Not enough data yet"

    session_dates = {s.get("date") for s in week_sessions}
    todos_data    = data.get("todos", {}).get(uid)
    if isinstance(todos_data, dict):
        dates_map = todos_data.get("dates", {})
        in_sess_done = out_sess_done = in_sess_total = out_sess_total = 0
        for date_k, todos in dates_map.items():
            if date_k not in week_dates:
                continue
            for t in todos:
                if date_k in session_dates:
                    in_sess_total += 1
                    if t.get("done"): in_sess_done += 1
                else:
                    out_sess_total += 1
                    if t.get("done"): out_sess_done += 1
        in_pct  = f"{round(in_sess_done/in_sess_total*100)}%" if in_sess_total else "N/A"
        out_pct = f"{round(out_sess_done/out_sess_total*100)}%" if out_sess_total else "N/A"
    else:
        in_pct = out_pct = "N/A"

    active = data["active_sessions"].get(uid)
    active_note = ""
    if active:
        elapsed     = int(time_module.time() - active["start_time"])
        active_note = f"\n\n🔴 **Active:** {active['task']} · {format_duration(elapsed)} elapsed"

    fw = data.get("focus_windows", {}).get(uid)
    if fw:
        fw_note = f"\n🔔 **Phantom Alert:** {fw['hour']:02d}:{fw['minute']:02d} daily (15 min warning)"
    else:
        fw_note = "\n🔔 **Phantom Alert:** Not set — use `/setfocuswindow` to enable"

    hours_str = f"{total_secs // 3600}h {(total_secs % 3600) // 60}m"

    embed = make_embed(
        f"📊 {codename} · SESSION ANALYTICS",
        f"**This week:** {total_sessions} sessions · {hours_str} · {total_echoes} echoes"
        f"{'  ·  🎙️ ' + str(vc_sessions) + ' in VC' if vc_sessions else ''}"
        f"{active_note}{fw_note}",
        color=0xA855F7
    )
    embed.add_field(
        name="🎯 Task Completion",
        value=f"On session days: **{in_pct}**\nOff session days: **{out_pct}**",
        inline=True
    )
    embed.add_field(
        name="⚡ Prime Window",
        value=prime_window,
        inline=True
    )
    embed.add_field(
        name="🏅 Shadow Grind",
        value=f"{'🏅 × ' + str(sg_count) if sg_count else 'None yet — hit 7h in one session!'}",
        inline=True
    )
    embed.add_field(
        name="Echo Structure",
        value=f"3/hr · 3h +2 · 5h +3 · 7h +5 🏆 · Cap: {DAILY_SESSION_CAP}/day\nToday: **{daily_sess}/{DAILY_SESSION_CAP}**",
        inline=False
    )
    embed.set_footer(text="☽ SHADOWSEEKERS ORDER · SESSION INTELLIGENCE")
    await interaction.response.send_message(embed=embed)


# ── /setfocuswindow ───────────────────────────────────────────────
@tree.command(name="setfocuswindow", description="Set your daily focus window — bot pings you 15 min before it starts")
@app_commands.describe(
    hour="Hour in 24h format (0–23)",
    minute="Minute (0 or 30)"
)
async def setfocuswindow(interaction: discord.Interaction, hour: int, minute: int = 0):
    uid = str(interaction.user.id)
    if not (0 <= hour <= 23) or minute not in (0, 15, 30, 45):
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID TIME", "Hour must be 0–23. Minute must be 0, 15, 30, or 45.", color=0xE63946)
        )
        return

    data = await load_data()
    if "focus_windows" not in data:
        data["focus_windows"] = {}

    data["focus_windows"][uid] = {"hour": hour, "minute": minute}
    await save_data(data)

    def fmt_h(h, m):
        suffix = "AM" if h < 12 else "PM"
        h12    = h % 12 or 12
        return f"{h12}:{m:02d} {suffix}"

    alert_total = hour * 60 + minute - 15
    if alert_total < 0:
        alert_total += 1440
    alert_h, alert_m = divmod(alert_total, 60)

    await interaction.response.send_message(
        embed=make_embed(
            "🔔 PHANTOM ALERT SET",
            f"Your focus window is locked in at **{fmt_h(hour, minute)}** daily.\n"
            f"You'll be pinged at **{fmt_h(alert_h, alert_m)}** — 15 minutes before it begins.\n\n"
            f"*Use `/setfocuswindow` again to update anytime.*",
            color=0xA855F7
        ).set_footer(text="☽ SHADOWSEEKERS ORDER · PHANTOM ALERT SYSTEM")
    )


# ── PHANTOM ALERT TASK ────────────────────────────────────────────
@tasks.loop(minutes=1)
async def phantom_alert_task():
    tz     = pytz.timezone(TIMEZONE)
    now_dt = datetime.now(tz)
    now_h  = now_dt.hour
    now_m  = now_dt.minute

    try:
        data = await load_data()
    except Exception:
        return

    windows = data.get("focus_windows", {})
    if not windows:
        return

    for uid, fw in windows.items():
        fw_h = fw.get("hour", 0)
        fw_m = fw.get("minute", 0)

        alert_total = fw_h * 60 + fw_m - 15
        if alert_total < 0:
            alert_total += 1440
        alert_h, alert_m = divmod(alert_total, 60)

        if now_h == alert_h and now_m == alert_m:
            def fmt_h(h, m):
                suffix = "AM" if h < 12 else "PM"
                h12    = h % 12 or 12
                return f"{h12}:{m:02d} {suffix}"

            for guild in _bot_ref.guilds:
                member_obj = guild.get_member(int(uid))
                if member_obj:
                    try:
                        embed = discord.Embed(
                            title="👻 PHANTOM ALERT — FOCUS WINDOW IN 15 MIN",
                            description=(
                                f"Your focus window begins at **{fmt_h(fw_h, fw_m)}**.\n\n"
                                f"Prepare your workspace. Clear distractions.\n"
                                f"Use `/study <task>` or `/pomodoro <task>` when you're ready to lock in.\n\n"
                                f"*The shadow doesn't wait.*"
                            ),
                            color=0xA855F7
                        )
                        embed.set_footer(text="☽ SHADOWSEEKERS ORDER · PHANTOM ALERT SYSTEM")
                        await member_obj.send(embed=embed)
                    except Exception as e:
                        print(f"[PHANTOM ALERT] Could not DM uid={uid}: {e}")
                    break

# ══════════════════════════════════════════════════════════════════
#  VC TRACKING  (FIXED)
# ══════════════════════════════════════════════════════════════════


# ── TTS: speak a greeting when someone joins VC ────────────────────────────
_vc_join_times: dict = {}

# ── REALTIME LEADERBOARD — ANIMATED PULSE ─────────────────────────
_PULSE_FRAMES = [
    ["◉ ○ ○", "○ ◉ ○", "○ ○ ◉"],
    ["○ ◉ ○", "○ ○ ◉", "◉ ○ ○"],
    ["○ ○ ◉", "◉ ○ ○", "○ ◉ ○"],
]
_LIVE_FRAMES  = ["◉ ◉ ○", "○ ◉ ◉", "◉ ○ ◉"]
_lb_tick      = 0


def _pulse(rank_idx: int, live: bool) -> str:
    frame = _lb_tick % 3
    if live:
        return _LIVE_FRAMES[frame]
    return _PULSE_FRAMES[rank_idx % 3][frame]


def _echo_bar(count: int, max_count: int, width: int = 8) -> str:
    if max_count == 0:
        return "░" * width
    filled = round((count / max_count) * width)
    return "█" * filled + "░" * (width - filled)


async def _upsert_lb_message(lb_ch: discord.TextChannel, key: str, embed: discord.Embed):
    """Edit existing leaderboard message in-place, or send a new one and persist its ID."""
    msg_id = _lb_message_ids.get(key)
    try:
        if msg_id:
            msg = await lb_ch.fetch_message(msg_id)
            await msg.edit(embed=embed)
            return
    except (discord.NotFound, discord.HTTPException):
        pass
    # Send fresh and persist
    try:
        msg = await lb_ch.send(embed=embed)
        _lb_message_ids[key] = msg.id
        await _save_lb_message_ids()
    except Exception as e:
        print(f"[REALTIME LB] Send failed key={key}: {e}")


async def refresh_realtime_leaderboards(guild: discord.Guild = None):
    """Edit all 4 live leaderboard messages in-place (echo · vc · weekly · daily)."""
    global _lb_tick
    _lb_tick += 1

    if guild is None:
        for g in bot.guilds:
            await refresh_realtime_leaderboards(g)
        return

    data   = await load_data()
    medals = ["🥇", "🥈", "🥉"]
    now_ts = discord.utils.utcnow().strftime("%d %b %Y · %H:%M UTC")
    tz     = pytz.timezone(TIMEZONE)
    now_dt = datetime.now(tz)
    EMBED_DESC_LIMIT = 4000

    lb_ch = discord.utils.get(guild.text_channels, name=LEADERBOARD_CHANNEL)
    if not lb_ch:
        return

    # ── 1. ECHO LEADERBOARD (all-time) ────────────────────────────
    sorted_m  = sorted(data["members"], key=lambda m: int(m.get("echoCount", 0)), reverse=True)[:10]
    max_count = int(sorted_m[0].get("echoCount", 1)) if sorted_m else 1
    lines, thumb_url = [], None

    for i, m in enumerate(sorted_m):
        count   = int(m.get("echoCount", 0))
        rank    = medals[i] if i < 3 else f"`#{i+1}`"
        tier    = get_tier(count)
        uid_lnk = next(
            (u for u, lnk in data.get("links", {}).items()
             if lnk.get("shadow_id") == m.get("shadowId") and lnk.get("approved")),
            None
        )
        gm      = guild.get_member(int(uid_lnk)) if uid_lnk else None
        mention = gm.mention if gm else f"**{m.get('codename', m.get('shadowId', '?'))}**"

        if i == (_lb_tick % min(3, len(sorted_m))) and gm and gm.display_avatar:
            thumb_url = gm.display_avatar.url

        in_session  = uid_lnk and uid_lnk in data.get("active_sessions", {})
        is_live_vc  = gm and gm.voice and gm.voice.channel is not None
        pulse       = _pulse(i, is_live_vc or in_session)
        bar         = _echo_bar(count, max_count)
        session_dot = " 📖" if in_session else ""
        vc_dot      = " 🎙️" if is_live_vc else ""

        sg_count = 0
        badges   = m.get("badges", {})
        if isinstance(badges, str):
            try: badges = json.loads(badges)
            except: badges = {}
        if isinstance(badges, dict):
            sg_count = badges.get("shadow_grind", 0)
        badge_str = f" 🏅×{sg_count}" if sg_count else ""

        line = (
            f"{rank} {mention}{vc_dot}{session_dot}\n"
            f"╰ `{bar}` **{count:,}✦** · *{tier['name']}*{badge_str} · {pulse}"
        )
        if len("\n".join(lines)) + len(line) + 1 > EMBED_DESC_LIMIT:
            break
        lines.append(line)

    echo_embed = make_embed(
        "☽ ECHO LEADERBOARD",
        "\n".join(lines) if lines else "*No operatives recorded yet.*",
        color=0xF0A500
    )
    echo_embed.set_footer(text=f"☽ SHADOWSEEKERS ORDER · all-time · {now_ts}")
    if thumb_url:
        echo_embed.set_thumbnail(url=thumb_url)
    await _upsert_lb_message(lb_ch, f"echo_{guild.id}", echo_embed)

    # ── 2. VC LEADERBOARD (all-time) ──────────────────────────────
    vc_time  = data.get("vc_time", {})
    links    = data.get("links", {})
    all_uids = set(vc_time.keys()) | set(_vc_join_times.keys())
    entries  = []

    for uid in all_uids:
        saved_secs = int(vc_time.get(uid, 0))
        live_extra = int(time_module.time() - _vc_join_times[uid]) if uid in _vc_join_times else 0
        total_secs = saved_secs + live_extra
        if total_secs <= 0:
            continue
        gm      = guild.get_member(int(uid))
        is_live = uid in _vc_join_times
        link    = links.get(uid, {})
        if link.get("approved"):
            shadow_id  = link["shadow_id"]
            member_obj = next((m for m in data["members"] if m.get("shadowId") == shadow_id), None)
            codename   = member_obj.get("codename", shadow_id) if member_obj else shadow_id
        else:
            codename = gm.display_name if gm else uid
        entries.append({"uid": uid, "codename": codename, "seconds": total_secs, "gm": gm, "live": is_live})

    entries.sort(key=lambda x: x["seconds"], reverse=True)
    max_vc_secs = entries[0]["seconds"] if entries else 1
    vc_lines, vc_thumb = [], None

    for i, e in enumerate(entries[:10]):
        rank     = medals[i] if i < 3 else f"`#{i+1}`"
        h        = e["seconds"] // 3600
        m_left   = (e["seconds"] % 3600) // 60
        mention  = e["gm"].mention if e["gm"] else f"**{e['codename']}**"
        pulse    = _pulse(i, e["live"])
        live_tag = " 🔴 **LIVE**" if e["live"] else ""
        bar      = _echo_bar(e["seconds"], max_vc_secs)
        if i == (_lb_tick % min(3, len(entries))) and e["gm"] and e["gm"].display_avatar:
            vc_thumb = e["gm"].display_avatar.url
        line = (
            f"{rank} {mention}{live_tag}\n"
            f"╰ `{bar}` **{h}h {m_left}m** · {pulse}"
        )
        if len("\n".join(vc_lines)) + len(line) + 1 > EMBED_DESC_LIMIT:
            break
        vc_lines.append(line)

    total_hrs = sum(e["seconds"] for e in entries) // 3600
    vc_embed  = make_embed(
        "🎙️ VC LEADERBOARD — TIME IN THE VOID",
        "\n".join(vc_lines) if vc_lines else "*No VC time recorded yet.*",
        color=0x10B981
    )
    vc_embed.set_footer(text=f"☽ SHADOWSEEKERS ORDER · {total_hrs}h total · all-time · {now_ts}")
    if vc_thumb:
        vc_embed.set_thumbnail(url=vc_thumb)
    await _upsert_lb_message(lb_ch, f"vc_{guild.id}", vc_embed)

    # ── 3. WEEKLY LEADERBOARD — echo gains + study hours this week ─
    week_dates = set()
    for i in range(7):
        d = now_dt - timedelta(days=i)
        week_dates.add(d.strftime("%m/%d"))

    weekly_entries = []
    for uid, link in data.get("links", {}).items():
        if not link.get("approved"):
            continue
        shadow_id  = link["shadow_id"]
        member_obj = next((m for m in data["members"] if m.get("shadowId") == shadow_id), None)
        if not member_obj:
            continue
        codename = member_obj.get("codename", shadow_id)
        history  = data.get("session_history", {}).get(uid, [])
        week_sessions = [s for s in history if s.get("date") in week_dates]
        week_echoes   = sum(s.get("awarded", 0) for s in week_sessions)
        week_secs     = sum(s.get("duration_seconds", 0) for s in week_sessions)
        week_sessions_count = len(week_sessions)
        if week_echoes == 0 and week_secs == 0:
            continue
        gm = guild.get_member(int(uid))
        weekly_entries.append({
            "uid": uid, "codename": codename, "gm": gm,
            "echoes": week_echoes, "seconds": week_secs,
            "sessions": week_sessions_count,
        })

    weekly_entries.sort(key=lambda x: (x["echoes"], x["seconds"]), reverse=True)
    max_w_echoes = weekly_entries[0]["echoes"] if weekly_entries else 1
    w_lines, w_thumb = [], None

    for i, e in enumerate(weekly_entries[:10]):
        rank    = medals[i] if i < 3 else f"`#{i+1}`"
        mention = e["gm"].mention if e["gm"] else f"**{e['codename']}**"
        h       = e["seconds"] // 3600
        m_left  = (e["seconds"] % 3600) // 60
        bar     = _echo_bar(e["echoes"], max_w_echoes)
        in_sess = e["uid"] in data.get("active_sessions", {})
        pulse   = _pulse(i, in_sess)
        if i == (_lb_tick % min(3, len(weekly_entries))) and e["gm"] and e["gm"].display_avatar:
            w_thumb = e["gm"].display_avatar.url
        line = (
            f"{rank} {mention}\n"
            f"╰ `{bar}` **+{e['echoes']}✦** · {h}h {m_left}m · {e['sessions']} sessions · {pulse}"
        )
        if len("\n".join(w_lines)) + len(line) + 1 > EMBED_DESC_LIMIT:
            break
        w_lines.append(line)

    # Figure out days left in the week (resets Monday)
    days_to_monday = (7 - now_dt.weekday()) % 7 or 7
    week_start = (now_dt - timedelta(days=now_dt.weekday())).strftime("%d %b")
    weekly_embed = make_embed(
        "📅 WEEKLY GRIND — THIS WEEK'S OPERATIVES",
        "\n".join(w_lines) if w_lines else "*No sessions logged this week yet.*",
        color=0xA855F7
    )
    weekly_embed.set_footer(
        text=f"☽ SHADOWSEEKERS ORDER · week of {week_start} · resets in {days_to_monday}d · {now_ts}"
    )
    if w_thumb:
        weekly_embed.set_thumbnail(url=w_thumb)
    await _upsert_lb_message(lb_ch, f"weekly_{guild.id}", weekly_embed)

    # ── 4. DAILY LEADERBOARD — today's study hours + echoes earned ─
    today_str_val = now_dt.strftime("%m/%d")
    daily_entries = []

    for uid, link in data.get("links", {}).items():
        if not link.get("approved"):
            continue
        shadow_id  = link["shadow_id"]
        member_obj = next((m for m in data["members"] if m.get("shadowId") == shadow_id), None)
        if not member_obj:
            continue
        codename = member_obj.get("codename", shadow_id)
        history  = data.get("session_history", {}).get(uid, [])
        today_sessions = [s for s in history if s.get("date") == today_str_val]
        today_echoes   = sum(s.get("awarded", 0) for s in today_sessions)
        today_secs     = sum(s.get("duration_seconds", 0) for s in today_sessions)

        # Add live session time for currently active session
        active_sess = data.get("active_sessions", {}).get(uid)
        is_live     = False
        if active_sess and isinstance(active_sess, dict):
            live_secs   = int(time_module.time() - active_sess.get("start_time", 0))
            today_secs += live_secs
            is_live     = True

        if today_echoes == 0 and today_secs == 0:
            continue

        gm = guild.get_member(int(uid))
        daily_entries.append({
            "uid": uid, "codename": codename, "gm": gm,
            "echoes": today_echoes, "seconds": today_secs, "live": is_live,
        })

    daily_entries.sort(key=lambda x: (x["seconds"], x["echoes"]), reverse=True)
    max_d_secs = daily_entries[0]["seconds"] if daily_entries else 1
    d_lines, d_thumb = [], None

    for i, e in enumerate(daily_entries[:10]):
        rank     = medals[i] if i < 3 else f"`#{i+1}`"
        mention  = e["gm"].mention if e["gm"] else f"**{e['codename']}**"
        h        = e["seconds"] // 3600
        m_left   = (e["seconds"] % 3600) // 60
        bar      = _echo_bar(e["seconds"], max_d_secs)
        live_tag = " 🔴" if e["live"] else ""
        pulse    = _pulse(i, e["live"])
        if i == (_lb_tick % min(3, len(daily_entries))) and e["gm"] and e["gm"].display_avatar:
            d_thumb = e["gm"].display_avatar.url
        line = (
            f"{rank} {mention}{live_tag}\n"
            f"╰ `{bar}` **{h}h {m_left}m** · +{e['echoes']}✦ today · {pulse}"
        )
        if len("\n".join(d_lines)) + len(line) + 1 > EMBED_DESC_LIMIT:
            break
        d_lines.append(line)

    daily_embed = make_embed(
        f"🌑 TODAY'S GRIND — {now_dt.strftime('%d %b %Y')}",
        "\n".join(d_lines) if d_lines else "*No sessions logged today yet. Start one with `/study`.*",
        color=0x6B6B9A
    )
    daily_embed.set_footer(text=f"☽ SHADOWSEEKERS ORDER · today · resets at midnight · {now_ts}")
    if d_thumb:
        daily_embed.set_thumbnail(url=d_thumb)
    await _upsert_lb_message(lb_ch, f"daily_{guild.id}", daily_embed)


@tasks.loop(seconds=45)
async def lb_tick_task():
    """Background pulse — refreshes all leaderboards every 45s."""
    for g in bot.guilds:
        try:
            await refresh_realtime_leaderboards(g)
        except Exception as e:
            print(f"[LB TICK] Error for guild {g.id}: {e}")


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    """Detect VC joins/leaves — track time, ping user, show timer if session running."""
    if member.bot:
        return

    uid  = str(member.id)
    data = await load_data()
    now  = time_module.time()

    joined = before.channel is None and after.channel is not None
    left   = before.channel is not None and after.channel is None

    if joined:
        _vc_join_times[uid] = now

        shadow_id = get_shadow_id(uid, data)
        # FIX: Always ping in focus-log/general, whether linked or not
        # Just use display name if not linked
        if shadow_id:
            m_obj    = get_member(shadow_id, data)
            codename = m_obj.get("codename", shadow_id) if m_obj else shadow_id
        else:
            codename = member.display_name

        # Update session VC status if session is active
        if uid in data["active_sessions"] and shadow_id:
            data["active_sessions"][uid]["in_vc"]      = True
            data["active_sessions"][uid]["vc_channel"] = after.channel.name
            await save_data(data)

            # Force-edit the live timer message to show VC bonus
            live_msg = _session_messages.get(uid)
            if live_msg:
                try:
                    sess_snap = data["active_sessions"][uid]
                    el        = int(now - sess_snap["start_time"])
                    bar       = make_progress_bar(el % 3600, 3600)
                    quick_embed = make_embed(
                        "☽ FOCUS SESSION IN PROGRESS",
                        f"**{sess_snap['task']}**\n\n"
                        f"`[{bar}]` **{format_duration(el)} elapsed** · 🎙️ In VC\n\n"
                        f"VC bonus now active — keep grinding.\n"
                        f"Use `/endsession` to submit proof and claim echoes.",
                        color=0xA855F7
                    )
                    quick_embed.set_author(name=f"Operative: {codename}")
                    await live_msg.edit(embed=quick_embed)
                except Exception as e:
                    print(f"[VC JOIN] Could not edit live timer: {e}")

            sess        = data["active_sessions"][uid]
            elapsed     = int(now - sess["start_time"])
            pomo_end    = sess.get("pomodoro_end")
            timer_total = sess.get("timer_total")
            is_pomo     = sess["session_type"] == "pomodoro"

            if pomo_end and timer_total:
                remaining = max(0, int(pomo_end - now))
                bar       = make_progress_bar(timer_total - remaining, timer_total)
                if is_pomo:
                    status_title = "🍅 POMODORO IN PROGRESS"
                else:
                    mins = timer_total // 60
                    status_title = f"⏱️ TIMED SESSION — {mins}min"
                dm_desc = (
                    f"**{sess['task']}**\n\n"
                    f"`[{bar}]` **{format_duration(remaining)} left**\n"
                    f"Elapsed: {format_duration(elapsed)}\n\n"
                    f"VC bonus now active — keep grinding."
                )
            else:
                bar = make_progress_bar(elapsed % 3600, 3600)
                status_title = "☽ FOCUS SESSION IN PROGRESS"
                dm_desc = (
                    f"**{sess['task']}**\n\n"
                    f"`[{bar}]` **{format_duration(elapsed)} elapsed**\n\n"
                    f"VC bonus now active — keep grinding."
                )

            dm_embed = make_embed(status_title, dm_desc, color=0xA855F7)
            dm_embed.set_author(name=f"Operative: {codename}")
            dm_embed.set_footer(text=f"Joined {after.channel.name} · VC bonus active")

            # Ping in session channel or focus-log
            sess_ch = member.guild.get_channel(int(sess["channel_id"])) if sess.get("channel_id") else None
            if not sess_ch:
                sess_ch = discord.utils.get(member.guild.text_channels, name=FOCUS_LOG_CHANNEL)
            if sess_ch:
                await sess_ch.send(content=member.mention, embed=dm_embed)
            return

        # ── Send ping directly into the VC channel itself ──
        vc_chan = after.channel
        print(f"[VC JOIN] uid={uid} codename={codename} linked={shadow_id is not None} vc={vc_chan}")

        try:
            if shadow_id:
                prompt_embed = make_embed(
                    "☽ OPERATIVE ENTERED THE VOID",
                    f"**{codename}** locked in.\n\nStart a session to earn echoes while you're here.\nUse `/study <task>` or `/pomodoro <task>` to lock in.",
                    color=0x6B6B9A
                )
                prompt_embed.set_author(
                    name=f"Operative: {codename}",
                    icon_url=member.display_avatar.url if member.display_avatar else None
                )
                prompt_embed.set_footer(text="SHADOWSEEKERS ORDER · VC detected")
                await vc_chan.send(content=member.mention, embed=prompt_embed)
            else:
                prompt_embed = make_embed(
                    "🎙️ MEMBER JOINED VC",
                    f"**{codename}** joined.\n\nLink your Shadow ID with `/link <shadow_id> <n>` to earn echoes.",
                    color=0x4B4B6B
                )
                prompt_embed.set_author(
                    name=codename,
                    icon_url=member.display_avatar.url if member.display_avatar else None
                )
                prompt_embed.set_footer(text="SHADOWSEEKERS ORDER · VC detected")
                await vc_chan.send(embed=prompt_embed)
        except Exception as e:
            print(f"[VC JOIN] Could not send to VC channel: {e}")

        asyncio.create_task(refresh_realtime_leaderboards(member.guild))

    elif left:
        join_time = _vc_join_times.pop(uid, None)
        if join_time:
            vc_seconds = int(now - join_time)
            if "vc_time" not in data:
                data["vc_time"] = {}
            data["vc_time"][uid] = data["vc_time"].get(uid, 0) + vc_seconds

        if uid in data["active_sessions"]:
            data["active_sessions"][uid]["in_vc"]      = False
            data["active_sessions"][uid]["vc_channel"] = ""

        await save_data(data)
        asyncio.create_task(refresh_realtime_leaderboards(member.guild))

# ══════════════════════════════════════════════════════════════════
#  SLASH COMMANDS
# ══════════════════════════════════════════════════════════════════

# ── /todo ─────────────────────────────────────────────────────────
todo_group = app_commands.Group(name="todo", description="Manage your objectives")

@todo_group.command(name="date", description="Switch your active session date — todos go to that day")
@app_commands.describe(date="Date in MM/DD format e.g. 04/15. Leave blank to see your current active date.")
async def todo_date(interaction: discord.Interaction, date: str = None):
    data = await load_data()
    uid  = str(interaction.user.id)

    if not get_shadow_id(uid, data):
        await interaction.response.send_message(
            embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first — use `/link <shadow_id> <n>`.", color=0xE63946)
        )
        return

    if date is None:
        active = get_active_date(uid, data)
        todos  = get_todos_for_date(uid, active, data)
        is_today = active == today_str()
        label    = " *(today)*" if is_today else ""
        await interaction.response.send_message(
            embed=make_embed(
                "◈ ACTIVE SESSION DATE",
                f"You're currently working on **{active}**{label}.\n`{len(todos)}` objective(s) on this date.",
                color=0xA855F7
            )
        )
        return

    if not re.match(r'^\d{2}/\d{2}$', date.strip()):
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID FORMAT", "Use `MM/DD` format — e.g. `04/15`.", color=0xE63946)
        )
        return

    try:
        datetime.strptime(date.strip() + f"/{datetime.now().year}", "%m/%d/%Y")
    except ValueError:
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID DATE", f"`{date}` isn't a real date. Try again.", color=0xE63946)
        )
        return

    date_key = date.strip()
    if not isinstance(data["todos"].get(uid), dict):
        data["todos"][uid] = {"active_date": today_str(), "dates": {}}
    data["todos"][uid]["active_date"] = date_key
    await save_data(data)

    todos    = get_todos_for_date(uid, date_key, data)
    is_today = date_key == today_str()
    label    = " *(today)*" if is_today else ""

    await interaction.response.send_message(
        embed=make_embed(
            "◈ SESSION DATE SWITCHED",
            f"**{interaction.user.display_name}** is now working on **{date_key}**{label}.\n"
            f"`{len(todos)}` objective(s) already on this date.\n\n"
            f"All `/todo add`, `/todo done`, `/todo list` now apply to **{date_key}**.",
            color=0x10B981
        )
    )

@todo_group.command(name="add", description="Log a new objective to your dossier")
@app_commands.describe(task="The objective to be carried out")
async def todo_add(interaction: discord.Interaction, task: str):
    data = await load_data()
    uid  = str(interaction.user.id)

    if not get_shadow_id(uid, data):
        await interaction.response.send_message(
            embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first — use `/link <shadow_id> <n>`.", color=0xE63946),
        )
        return

    active    = get_active_date(uid, data)
    todos     = get_todos_for_date(uid, active, data)
    todos.append({"task": task, "done": False, "priority": None})
    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    is_today  = active == today_str()
    date_note = "" if is_today else f" *(for {active})*"

    await interaction.response.send_message(
        embed=make_embed(
            "◉ OBJECTIVE ADDED",
            f"**{interaction.user.display_name}** logged objective **#{len(todos)}**{date_note}\n\n*{task}*",
            color=0x10B981
        )
    )

@todo_group.command(name="done", description="Mark objectives as fulfilled — single or comma-separated e.g. 1,3,5")
@app_commands.describe(numbers="Objective number(s), comma-separated e.g. 1,3,5")
async def todo_done(interaction: discord.Interaction, numbers: str):
    await interaction.response.defer()
    data   = await load_data()
    uid    = str(interaction.user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)

    try:
        indices = sorted(set(int(n.strip()) for n in numbers.split(",") if n.strip()))
    except ValueError:
        await interaction.followup.send(
            embed=make_embed("▲ INVALID INPUT", "Provide number(s) separated by commas e.g. `1,3,5`.", color=0xE63946)
        )
        return

    invalid = [n for n in indices if n < 1 or n > len(todos)]
    if not todos or invalid:
        await interaction.followup.send(
            embed=make_embed("▲ OBJECTIVE NOT FOUND", f"Objective(s) {', '.join(f'#{n}' for n in invalid)} don't exist. Check `/todo list`.", color=0xE63946),
        )
        return

    completed = []
    for n in indices:
        todos[n - 1]["done"] = True
        completed.append(todos[n - 1].get("task") or todos[n - 1].get("text", f"Objective #{n}"))

    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    total = len(todos)
    done_weight = 0.0
    for t in todos:
        ops = t.get("ops", [])
        if ops:
            done_ops = sum(1 for op in ops if op.get("done"))
            done_weight += done_ops / len(ops)
        else:
            if t["done"]:
                done_weight += 1
    done  = sum(1 for t in todos if t["done"])
    pct   = round((done_weight / total) * 100) if total else 0
    base  = data.get("base_echo_rate", 10)
    proj  = round(base * done_weight / total) if total else 0

    is_today  = active == today_str()
    date_note = "" if is_today else f" *(session: {active})*"
    task_lines = "\n".join(f"*{task}*" for task in completed)
    title = "☽ OBJECTIVE FULFILLED" if len(completed) == 1 else f"☽ {len(completed)} OBJECTIVES FULFILLED"

    await interaction.followup.send(
        embed=make_embed(
            title,
            f"**{interaction.user.display_name}** completed:{date_note}\n{task_lines}\n\n"
            f"`{done}/{total} objectives` · {pct}% complete\n"
            f"Projected echoes: **{proj}**",
            color=0x10B981
        )
    )

# ── FIX: /todo list — was missing the shadow_id check causing silent failures ──
@todo_group.command(name="list", description="View your operative dossier")
async def todo_list(interaction: discord.Interaction):
    data   = await load_data()
    uid    = str(interaction.user.id)

    # FIX: Added missing shadow_id check
    if not get_shadow_id(uid, data):
        await interaction.response.send_message(
            embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first — use `/link <shadow_id> <n>`.", color=0xE63946)
        )
        return

    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)

    if not todos:
        is_today  = active == today_str()
        date_note = "today" if is_today else active
        await interaction.response.send_message(
            embed=make_embed("◈ DOSSIER EMPTY", f"No objectives for **{date_note}**. Add one with `/todo add`.", color=0x7B2FBE)
        )
        return

    lines = []
    done_weight = 0.0
    for i, t in enumerate(todos, 1):
        # Handle both 'task' (manual todos) and 'text' (AI missions) keys
        if not isinstance(t, dict):
            continue
        task_text = t.get("task") or t.get("text")
        if not task_text:
            continue

        priority  = t.get("priority")
        suffix    = f" {PRIORITY_EMOJI[priority]}" if priority else ""
        ai_badge  = " 🤖" if t.get("source") == "ai" else ""
        ops       = t.get("ops", [])

        if ops and all(op.get("done") for op in ops):
            t["done"] = True

        if t.get("done"):
            lines.append(f"{DONE_EMOJI} ~~☽ {i}. {task_text}~~{suffix}{ai_badge}")
            done_weight += 1
        else:
            lines.append(f"{UNDONE_EMOJI} {i}. {task_text}{suffix}{ai_badge}")
            if ops:
                done_ops = sum(1 for op in ops if op.get("done"))
                done_weight += done_ops / len(ops)

        for op in ops:
            if op.get("done"):
                lines.append(f"    └ {DONE_EMOJI} ~~{op['task']}~~")
            else:
                lines.append(f"    └ {UNDONE_EMOJI} {op['task']}")

    total = len(todos)
    base  = data.get("base_echo_rate", 10)
    proj  = round(base * done_weight / total) if total else 0
    done  = sum(1 for t in todos if t["done"])

    is_today   = active == today_str()
    title_date = "TODAY'S" if is_today else active

    embed = make_embed(f"◈ {interaction.user.display_name}'s {title_date} OBJECTIVES", "\n".join(lines), color=0xA855F7)
    embed.add_field(name="Progress", value=f"{done}/{total} done · **{proj} echoes** on track", inline=False)
    await interaction.response.send_message(embed=embed)

@todo_group.command(name="clear", description="Purge your active date's dossier")
async def todo_clear(interaction: discord.Interaction):
    await interaction.response.defer()
    data   = await load_data()
    uid    = str(interaction.user.id)
    active = get_active_date(uid, data)
    set_todos_for_date(uid, active, [], data)
    await save_data(data)

    is_today  = active == today_str()
    date_note = "today's" if is_today else f"{active}'s"

    await interaction.followup.send(
        embed=make_embed("◈ OBJECTIVES CLEARED", f"**{interaction.user.display_name}** {date_note} dossier cleared. Fresh start.", color=0x6B6B9A)
    )

@todo_group.command(name="remove", description="Remove an objective from your dossier entirely")
@app_commands.describe(number="Objective number to remove (from /todo list)")
async def todo_remove(interaction: discord.Interaction, number: int):
    data   = await load_data()
    uid    = str(interaction.user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)

    if not todos or number < 1 or number > len(todos):
        await interaction.response.send_message(
            embed=make_embed("▲ OBJECTIVE NOT FOUND", f"Objective #{number} doesn't exist. Check `/todo list`.", color=0xE63946)
        )
        return

    removed = todos.pop(number - 1)
    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    await interaction.response.send_message(
        embed=make_embed(
            "◈ OBJECTIVE REMOVED",
            f"**{interaction.user.display_name}** struck objective **#{number}** from the dossier:\n\n~~{removed['task']}~~\n\n"
            f"`{len(todos)}` objective(s) remaining.",
            color=0x6B6B9A
        )
    )

@todo_group.command(name="multiadd", description="Log multiple objectives at once (comma separated)")
@app_commands.describe(tasks="Objectives separated by commas e.g. Infiltrate base, Secure the relic, Vanish")
async def todo_multiadd(interaction: discord.Interaction, tasks: str):
    data = await load_data()
    uid  = str(interaction.user.id)

    if not get_shadow_id(uid, data):
        await interaction.response.send_message(
            embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first — use `/link <shadow_id> <n>`.", color=0xE63946),
        )
        return

    task_list = [t.strip() for t in tasks.split(",") if t.strip()]
    if not task_list:
        await interaction.response.send_message(
            embed=make_embed("▲ NOTHING TO ADD", "No objectives found. Separate them with commas.", color=0xE63946),
        )
        return

    active      = get_active_date(uid, data)
    todos       = get_todos_for_date(uid, active, data)
    start_count = len(todos)
    for task in task_list:
        todos.append({"task": task, "done": False, "priority": None})
    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    is_today  = active == today_str()
    date_note = "" if is_today else f" *(for {active})*"
    lines     = [f"**#{start_count + i + 1}** · *{t}*" for i, t in enumerate(task_list)]

    await interaction.response.send_message(
        embed=make_embed(
            f"◉ {len(task_list)} OBJECTIVES ADDED",
            f"**{interaction.user.display_name}** added to the list{date_note}:\n\n" + "\n".join(lines),
            color=0x10B981
        )
    )

@todo_group.command(name="priority", description="Set priority on existing objectives (P1=critical, P2=important, P3=normal)")
@app_commands.describe(
    level="Priority level: p1, p2, p3, or none to clear",
    numbers="Task numbers to set, comma separated e.g. 1,3,5"
)
async def todo_priority(interaction: discord.Interaction, level: str, numbers: str):
    data   = await load_data()
    uid    = str(interaction.user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)

    if not todos:
        await interaction.response.send_message(
            embed=make_embed("▲ DOSSIER EMPTY", "No objectives to prioritize. Add some with `/todo add`.", color=0xE63946)
        )
        return

    lvl = level.lower().strip()
    if lvl not in ("p1", "p2", "p3", "none"):
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID PRIORITY", "Use `p1`, `p2`, `p3`, or `none` to clear.", color=0xE63946)
        )
        return

    priority_val = None if lvl == "none" else lvl

    try:
        indices = [int(n.strip()) for n in numbers.split(",") if n.strip()]
    except ValueError:
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID NUMBERS", "Provide task numbers separated by commas e.g. `1,3,5`.", color=0xE63946)
        )
        return

    invalid = [n for n in indices if n < 1 or n > len(todos)]
    if invalid:
        await interaction.response.send_message(
            embed=make_embed("▲ OUT OF RANGE", f"Task(s) {', '.join(str(n) for n in invalid)} don't exist. Check `/todo list`.", color=0xE63946)
        )
        return

    for n in indices:
        todos[n - 1]["priority"] = priority_val
    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    priority_labels = {"p1": "P1 — Critical", "p2": "P2 — Important", "p3": "P3 — Normal", "none": "None (cleared)"}
    color_map       = {"p1": 0xE63946, "p2": 0xF0A500, "p3": 0xF5C542, "none": 0x6B6B9A}
    task_names      = ", ".join(f"**#{n}**" for n in indices)

    await interaction.response.send_message(
        embed=make_embed(
            "◉ PRIORITY SET",
            f"{task_names} → **{priority_labels[lvl]}**",
            color=color_map[lvl]
        )
    )

tree.add_command(todo_group)

# ── /op ───────────────────────────────────────────────────────────
op_group = app_commands.Group(name="op", description="Manage ops (sub-tasks) under objectives")

@op_group.command(name="add", description="Add an op under an objective")
@app_commands.describe(objective="Objective number", op="The op to add")
async def op_add(interaction: discord.Interaction, objective: int, op: str):
    data   = await load_data()
    uid    = str(interaction.user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)

    if not todos or objective < 1 or objective > len(todos):
        await interaction.response.send_message(
            embed=make_embed("▲ OBJECTIVE NOT FOUND", f"Objective #{objective} doesn't exist. Check `/todo list`.", color=0xE63946)
        )
        return

    t = todos[objective - 1]
    if "ops" not in t:
        t["ops"] = []
    t["ops"].append({"task": op, "done": False})
    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    op_num = len(t["ops"])
    await interaction.response.send_message(
        embed=make_embed(
            "◉ OP ADDED",
            f"Op **#{op_num}** added under Objective **#{objective}** · *{todos[objective-1]['task']}*\n\n`└ ○ {op}`",
            color=0x10B981
        )
    )

@op_group.command(name="done", description="Mark ops as complete — single or comma-separated e.g. 1,2")
@app_commands.describe(objective="Objective number", ops="Op number(s), comma-separated e.g. 1,2")
async def op_done(interaction: discord.Interaction, objective: int, ops: str):
    data   = await load_data()
    uid    = str(interaction.user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)

    if not todos or objective < 1 or objective > len(todos):
        await interaction.response.send_message(
            embed=make_embed("▲ OBJECTIVE NOT FOUND", f"Objective #{objective} doesn't exist. Check `/todo list`.", color=0xE63946)
        )
        return

    t       = todos[objective - 1]
    op_list = t.get("ops", [])

    try:
        indices = sorted(set(int(n.strip()) for n in ops.split(",") if n.strip()))
    except ValueError:
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID INPUT", "Provide op number(s) separated by commas e.g. `1,2`.", color=0xE63946)
        )
        return

    invalid = [n for n in indices if n < 1 or n > len(op_list)]
    if not op_list or invalid:
        await interaction.response.send_message(
            embed=make_embed("▲ OP NOT FOUND", f"Op(s) {', '.join(f'#{n}' for n in invalid)} don't exist under Objective #{objective}.", color=0xE63946)
        )
        return

    for n in indices:
        op_list[n - 1]["done"] = True

    if all(o["done"] for o in op_list):
        t["done"] = True

    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    done_ops  = sum(1 for o in op_list if o["done"])
    obj_label = t["task"]
    auto_note = "\n\n✅ All ops complete — **objective auto-fulfilled.**" if t["done"] else ""

    total_weight = len(todos)
    done_weight  = 0.0
    for task in todos:
        task_ops = task.get("ops", [])
        if task_ops:
            done_weight += sum(1 for o in task_ops if o["done"]) / len(task_ops)
        elif task["done"]:
            done_weight += 1
    proj = round(data.get("base_echo_rate", 10) * done_weight / total_weight) if total_weight else 0

    title = "☽ OP COMPLETE" if len(indices) == 1 else f"☽ {len(indices)} OPS COMPLETE"
    await interaction.response.send_message(
        embed=make_embed(
            title,
            f"Op(s) **{', '.join(f'#{n}' for n in indices)}** under *{obj_label}* fulfilled.\n`{done_ops}/{len(op_list)} ops done`{auto_note}\n\nProjected echoes: **{proj}**",
            color=0x10B981
        )
    )

@op_group.command(name="multiadd", description="Add multiple ops under an objective (comma-separated)")
@app_commands.describe(objective="Objective number", ops="Ops separated by commas e.g. Disable cameras, Find the vault")
async def op_multiadd(interaction: discord.Interaction, objective: int, ops: str):
    data   = await load_data()
    uid    = str(interaction.user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)

    if not todos or objective < 1 or objective > len(todos):
        await interaction.response.send_message(
            embed=make_embed("▲ OBJECTIVE NOT FOUND", f"Objective #{objective} doesn't exist. Check `/todo list`.", color=0xE63946)
        )
        return

    op_list = [o.strip() for o in ops.split(",") if o.strip()]
    if not op_list:
        await interaction.response.send_message(
            embed=make_embed("▲ NOTHING TO ADD", "No ops found. Separate them with commas.", color=0xE63946)
        )
        return

    t = todos[objective - 1]
    if "ops" not in t:
        t["ops"] = []
    for o in op_list:
        t["ops"].append({"task": o, "done": False})
    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    lines = "\n".join(f"    └ ○ *{o}*" for o in op_list)
    await interaction.response.send_message(
        embed=make_embed(
            f"◉ {len(op_list)} OPS ADDED",
            f"Under Objective **#{objective}** · *{t['task']}*\n\n{lines}",
            color=0x10B981
        )
    )

@op_group.command(name="remove", description="Remove an op from an objective")
@app_commands.describe(objective="Objective number", op="Op number to remove")
async def op_remove(interaction: discord.Interaction, objective: int, op: int):
    data   = await load_data()
    uid    = str(interaction.user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)

    if not todos or objective < 1 or objective > len(todos):
        await interaction.response.send_message(
            embed=make_embed("▲ OBJECTIVE NOT FOUND", f"Objective #{objective} doesn't exist. Check `/todo list`.", color=0xE63946)
        )
        return

    t   = todos[objective - 1]
    ops = t.get("ops", [])

    if not ops or op < 1 or op > len(ops):
        await interaction.response.send_message(
            embed=make_embed("▲ OP NOT FOUND", f"Op #{op} doesn't exist under Objective #{objective}.", color=0xE63946)
        )
        return

    removed = ops.pop(op - 1)
    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    await interaction.response.send_message(
        embed=make_embed(
            "◈ OP REMOVED",
            f"~~{removed['task']}~~ removed from Objective **#{objective}** · *{t['task']}*\n`{len(ops)}` op(s) remaining.",
            color=0x6B6B9A
        )
    )

@op_group.command(name="move", description="Convert objectives into ops under a target objective")
@app_commands.describe(
    sources="Objective numbers to convert, comma-separated e.g. 2,3",
    target="Target objective number they become ops under"
)
async def op_move(interaction: discord.Interaction, sources: str, target: int):
    data   = await load_data()
    uid    = str(interaction.user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)

    if not todos or target < 1 or target > len(todos):
        await interaction.response.send_message(
            embed=make_embed("▲ OBJECTIVE NOT FOUND", f"Target objective #{target} doesn't exist.", color=0xE63946)
        )
        return

    try:
        src_nums = sorted(set(int(n.strip()) for n in sources.split(",") if n.strip()), reverse=True)
    except ValueError:
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID INPUT", "Provide comma-separated numbers e.g. `2,3`.", color=0xE63946)
        )
        return

    invalid = [n for n in src_nums if n < 1 or n > len(todos) or n == target]
    if invalid:
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID NUMBERS", f"Numbers {', '.join(str(n) for n in invalid)} are invalid or same as target.", color=0xE63946)
        )
        return

    target_task = todos[target - 1]
    if "ops" not in target_task:
        target_task["ops"] = []

    moved = []
    for n in src_nums:
        removed = todos.pop(n - 1)
        target_task["ops"].append({"task": removed["task"], "done": removed["done"]})
        moved.append(removed["task"])
        if n < target:
            target -= 1

    set_todos_for_date(uid, active, todos, data)
    await save_data(data)

    moved_lines = "\n".join(f"    └ ○ {m}" for m in reversed(moved))
    await interaction.response.send_message(
        embed=make_embed(
            "◈ OBJECTIVES CONVERTED TO OPS",
            f"Now under **{target_task['task']}**:\n{moved_lines}",
            color=0xA855F7
        )
    )

tree.add_command(op_group)

# ══════════════════════════════════════════════════════════════════
#  /exam — EXAM COUNTDOWN SYSTEM (NEW)
# ══════════════════════════════════════════════════════════════════

GROQ_API_KEY_MAIN = os.getenv("GROQ_API_KEY")
GROQ_API_URL_MAIN = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL_MAIN   = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

async def _fetch_exam_date_via_groq(exam_name: str) -> str | None:
    """
    Use Groq with web_search tool to find the real exam date.
    Returns a date string like 'MM/DD/YYYY' or None if not found.
    """
    if not GROQ_API_KEY_MAIN:
        return None

    tz   = pytz.timezone(TIMEZONE)
    year = datetime.now(tz).year

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY_MAIN}",
        "Content-Type": "application/json",
    }

    # Step 1: Ask Groq to search for the exam date
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a precise assistant. The user will give you an exam name. "
                    "Search the web and return ONLY the exam date in MM/DD/YYYY format. "
                    "If you cannot find the exact date, respond with exactly: UNKNOWN. "
                    "No explanations, no extra text — just the date or UNKNOWN."
                )
            },
            {
                "role": "user",
                "content": f"What is the official exam date for '{exam_name}' in {year}? Search and return only MM/DD/YYYY or UNKNOWN."
            }
        ],
        "tools": [{"type": "web_search"}],
        "tool_choice": "auto",
        "temperature": 0.1,
        "max_tokens": 100,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                GROQ_API_URL_MAIN, headers=headers, json=payload,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status == 200:
                    data_r = await resp.json()
                    # Handle tool-use response — extract text content
                    for choice in data_r.get("choices", []):
                        msg = choice.get("message", {})
                        result = (msg.get("content") or "").strip()
                        if result and result != "UNKNOWN":
                            # Try to extract MM/DD/YYYY from the result
                            match = re.search(r"\d{2}/\d{2}/\d{4}", result)
                            if match:
                                candidate = match.group(0)
                                try:
                                    datetime.strptime(candidate, "%m/%d/%Y")
                                    return candidate
                                except ValueError:
                                    pass
                        elif result == "UNKNOWN":
                            return None
                else:
                    print(f"[EXAM DATE FETCH] Groq HTTP {resp.status}")
    except Exception as e:
        print(f"[EXAM DATE FETCH] Groq error: {e}")

    return None

def _days_until(date_str: str) -> int:
    """Returns days until the given MM/DD/YYYY date. Negative = past."""
    tz       = pytz.timezone(TIMEZONE)
    now_date = datetime.now(tz).date()
    exam_dt  = datetime.strptime(date_str, "%m/%d/%Y").date()
    return (exam_dt - now_date).days

def _format_exam_countdown(days: int) -> str:
    if days < 0:
        return f"**{abs(days)} days ago** *(past)*"
    elif days == 0:
        return "**TODAY** 🚨"
    elif days == 1:
        return "**TOMORROW** ⚠️"
    elif days <= 7:
        return f"**{days} days** 🔥"
    elif days <= 30:
        return f"**{days} days** ⏳"
    else:
        return f"**{days} days**"

exam_group = app_commands.Group(name="exam", description="Track your upcoming exams with countdowns")

@exam_group.command(name="add", description="Add an exam to your profile — auto-fetches date if possible")
@app_commands.describe(
    name="Exam name e.g. 'JEE Advanced 2025', 'Physics Mid-Term', 'UPSC Prelims'",
    date="Date in MM/DD/YYYY format e.g. 05/25/2025 (leave blank to auto-fetch)"
)
async def exam_add(interaction: discord.Interaction, name: str, date: str = None):
    await interaction.response.defer()

    uid  = str(interaction.user.id)
    data = await load_data()

    # Validate manually provided date
    if date:
        date = date.strip()
        if not re.match(r'^\d{2}/\d{2}/\d{4}$', date):
            await interaction.followup.send(
                embed=make_embed("▲ INVALID DATE FORMAT", "Use `MM/DD/YYYY` — e.g. `05/25/2025`.", color=0xE63946)
            )
            return
        try:
            datetime.strptime(date, "%m/%d/%Y")
        except ValueError:
            await interaction.followup.send(
                embed=make_embed("▲ INVALID DATE", f"`{date}` is not a real date.", color=0xE63946)
            )
            return
        exam_date  = date
        date_source = "manual"
    else:
        # Try to auto-fetch via Groq
        exam_date   = await _fetch_exam_date_via_groq(name)
        date_source = "auto-fetched" if exam_date else None

    if "exams" not in data:
        data["exams"] = {}
    if uid not in data["exams"]:
        data["exams"][uid] = []

    if exam_date:
        days = _days_until(exam_date)
        exam_entry = {
            "name":   name,
            "date":   exam_date,
            "source": date_source,
        }
        data["exams"][uid].append(exam_entry)
        await save_data(data)

        countdown = _format_exam_countdown(days)
        source_note = " *(date auto-fetched)*" if date_source == "auto-fetched" else ""

        embed = make_embed(
            "📅 EXAM ADDED",
            f"**{name}**{source_note}\n\n"
            f"📆 Date: **{exam_date}**\n"
            f"⏳ Countdown: {countdown}\n\n"
            f"Use `/exam list` to see all your exams.",
            color=0xA855F7
        )
        await interaction.followup.send(embed=embed)

    else:
        # Couldn't auto-fetch — ask for manual date
        embed = make_embed(
            "📅 DATE NOT FOUND",
            f"Couldn't find the date for **{name}** automatically.\n\n"
            f"Please add it manually:\n"
            f"`/exam add name:{name} date:MM/DD/YYYY`\n\n"
            f"*(For common exams like JEE, NEET, UPSC, SAT, GRE — use the official name)*",
            color=0xF0A500
        )
        await interaction.followup.send(embed=embed)

@exam_group.command(name="list", description="View all your upcoming exams with countdowns")
async def exam_list(interaction: discord.Interaction):
    data = await load_data()
    uid  = str(interaction.user.id)

    exams = data.get("exams", {}).get(uid, [])

    if not exams:
        await interaction.response.send_message(
            embed=make_embed(
                "📅 NO EXAMS",
                "You haven't added any exams yet.\nUse `/exam add <name>` to track an exam.",
                color=0x7B2FBE
            )
        )
        return

    # Sort by date, soonest first
    def sort_key(e):
        try:
            return datetime.strptime(e["date"], "%m/%d/%Y")
        except:
            return datetime.max

    sorted_exams = sorted(exams, key=sort_key)

    lines = []
    for i, exam in enumerate(sorted_exams, 1):
        days      = _days_until(exam["date"])
        countdown = _format_exam_countdown(days)
        source    = " *(auto)*" if exam.get("source") == "auto-fetched" else ""
        lines.append(
            f"**{i}.** {exam['name']}{source}\n"
            f"    📆 {exam['date']} · {countdown}"
        )

    embed = make_embed(
        f"📅 {interaction.user.display_name}'s EXAM COUNTDOWN",
        "\n\n".join(lines),
        color=0xA855F7
    )
    embed.set_footer(text="☽ SHADOWSEEKERS ORDER · Grind before the deadline")
    await interaction.response.send_message(embed=embed)

@exam_group.command(name="remove", description="Remove an exam from your list")
@app_commands.describe(number="Exam number from /exam list")
async def exam_remove(interaction: discord.Interaction, number: int):
    data  = await load_data()
    uid   = str(interaction.user.id)
    exams = data.get("exams", {}).get(uid, [])

    if not exams or number < 1 or number > len(exams):
        await interaction.response.send_message(
            embed=make_embed("▲ EXAM NOT FOUND", f"Exam #{number} doesn't exist. Check `/exam list`.", color=0xE63946)
        )
        return

    # Sort same way as list so numbers match
    def sort_key(e):
        try:
            return datetime.strptime(e["date"], "%m/%d/%Y")
        except:
            return datetime.max

    sorted_exams = sorted(exams, key=sort_key)
    removed      = sorted_exams.pop(number - 1)

    # Rebuild in original insert order, minus the removed one
    new_exams = [e for e in exams if not (e["name"] == removed["name"] and e["date"] == removed["date"])]
    # Handle edge case of duplicate name+date by removing only first match
    if len(new_exams) == len(exams):
        exams.remove(removed)
        new_exams = exams

    data["exams"][uid] = new_exams
    await save_data(data)

    await interaction.response.send_message(
        embed=make_embed(
            "◈ EXAM REMOVED",
            f"~~{removed['name']}~~ removed from your countdown list.\n"
            f"`{len(new_exams)}` exam(s) remaining.",
            color=0x6B6B9A
        )
    )

tree.add_command(exam_group)

# ── /exams — server-wide exam countdown ──────────────────────────
@tree.command(name="exams", description="View all upcoming exams across the server")
async def exams_server(interaction: discord.Interaction):
    await interaction.response.defer()
    data = await load_data()

    all_exams = []  # (exam_dict, discord_member)
    for uid, user_exams in data.get("exams", {}).items():
        member_obj = interaction.guild.get_member(int(uid)) if interaction.guild else None
        name       = member_obj.display_name if member_obj else f"Operative {uid[:4]}"
        for exam in user_exams:
            all_exams.append((exam, name))

    if not all_exams:
        await interaction.followup.send(
            embed=make_embed(
                "📅 NO EXAMS",
                "No exams added yet. Members can add their exams with `/exam add`.",
                color=0x7B2FBE
            )
        )
        return

    def sort_key(pair):
        try:
            return datetime.strptime(pair[0]["date"], "%m/%d/%Y")
        except:
            return datetime.max

    sorted_all = sorted(all_exams, key=sort_key)

    lines = []
    for exam, member_name in sorted_all:
        days      = _days_until(exam["date"])
        countdown = _format_exam_countdown(days)
        lines.append(
            f"**{exam['name']}** · *{member_name}*\n"
            f"    📆 {exam['date']} · {countdown}"
        )

    # Chunk if too long
    desc = "\n\n".join(lines)
    if len(desc) > 3900:
        desc = "\n\n".join(lines[:15]) + f"\n\n*...and {len(lines) - 15} more*"

    embed = make_embed(
        "📅 SERVER EXAM COUNTDOWN",
        desc,
        color=0xF0A500
    )
    embed.set_footer(text=f"☽ SHADOWSEEKERS ORDER · {len(sorted_all)} total exams tracked")
    await interaction.followup.send(embed=embed)

# ── /echoes ───────────────────────────────────────────────────────
@tree.command(name="echoes", description="Reveal your echo resonance and operative rank")
async def echoes(interaction: discord.Interaction):
    await interaction.response.defer()

    data      = await load_data()
    uid       = str(interaction.user.id)
    shadow_id = get_shadow_id(uid, data)

    if not shadow_id:
        await interaction.followup.send(
            embed=make_embed("▲ NOT LINKED", "No Shadow ID linked. Use `/link <shadow_id> <n>` to get started.", color=0xE63946),
        )
        return

    member = get_member(shadow_id, data)
    if not member:
        data   = await load_data()  # refresh from MongoDB
        member = get_member(shadow_id, data)

    if not member:
        await interaction.followup.send(
            embed=make_embed("▲ OPERATIVE NOT FOUND", f"Shadow ID `{shadow_id}` has no record in the void.", color=0xE63946),
        )
        return

    count  = int(member.get("echoCount", 0))
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)
    done   = sum(1 for t in todos if t["done"])
    total  = len(todos)
    proj   = round(data.get("base_echo_rate", 10) * done / total) if total else 0

    is_today      = active == today_str()
    session_label = "Today's Objectives" if is_today else f"Objectives ({active})"

    today      = today_str()
    daily_key  = f"{uid}_{today}"
    daily_sess = data.get("daily_session_echoes", {}).get(daily_key, 0)
    sg_count   = member.get("badges", {})
    if isinstance(sg_count, str):
        try: sg_count = json.loads(sg_count)
        except: sg_count = {}
    sg_count = sg_count.get("shadow_grind", 0) if isinstance(sg_count, dict) else 0

    # Upcoming exams
    user_exams = data.get("exams", {}).get(uid, [])
    upcoming   = sorted(
        [e for e in user_exams if _days_until(e["date"]) >= 0],
        key=lambda e: _days_until(e["date"])
    )[:2]

    tier  = get_tier(count)
    embed = discord.Embed(title=f"☭ {member['codename']}", color=tier["color"])
    embed.add_field(name="Shadow ID",      value=f"`{shadow_id}`",        inline=True)
    embed.add_field(name="Echo Resonance", value=f"**{count:,} echoes**", inline=True)
    embed.add_field(name="Rank",           value=f"**{tier['name']}**",   inline=True)
    embed.add_field(name=session_label,    value=f"{done}/{total} fulfilled · +{proj} echoes on track", inline=False)
    embed.add_field(name="Study Echoes Today", value=f"{daily_sess}/{DAILY_SESSION_CAP}", inline=True)
    if sg_count:
        embed.add_field(name="Shadow Grind", value=f"🏅 × {sg_count}", inline=True)
    if upcoming:
        exam_lines = "\n".join(
            f"📅 **{e['name']}** — {_format_exam_countdown(_days_until(e['date']))}"
            for e in upcoming
        )
        embed.add_field(name="Upcoming Exams", value=exam_lines, inline=False)
    embed.set_footer(text="☭ SHADOWSEEKERS ORDER · DEEP IN THE DARK, I DON'T NEED THE LIGHT")
    await interaction.followup.send(embed=embed)

# ── /leaderboard ──────────────────────────────────────────────────
@tree.command(name="leaderboard", description="The most powerful operatives in the Order")
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()

    data = await load_data()

    sorted_m = sorted(data["members"], key=lambda m: int(m.get("echoCount", 0)), reverse=True)[:10]

    if not sorted_m:
        await interaction.followup.send(
            embed=make_embed("▲ NO DATA", "No operatives found. Try `/sync` first.", color=0xE63946),
        )
        return

    lines  = []
    medals = ["🥇","🥈","🥉"]
    for i, m in enumerate(sorted_m):
        count    = int(m.get("echoCount", 0))
        rank     = medals[i] if i < 3 else f"`#{i+1}`"
        tier     = get_tier(count)
        sg_count = m.get("badges", {})
        if isinstance(sg_count, str):
            try: sg_count = json.loads(sg_count)
            except: sg_count = {}
        sg_count = sg_count.get("shadow_grind", 0) if isinstance(sg_count, dict) else 0
        badge    = f" · 🏅×{sg_count}" if sg_count else ""
        discord_uid  = m.get("discordId")
        guild_member = interaction.guild.get_member(int(discord_uid)) if discord_uid and interaction.guild else None
        mention      = guild_member.mention if guild_member else f"**{m['codename']}**"
        lines.append(f"{rank} {mention} · `{m['shadowId']}` · **{count:,} echoes** · *{tier['name']}*{badge}")

    embed = make_embed("☽ ECHO LEADERBOARD — TOP OPERATIVES", "\n".join(lines), color=0xF0A500)
    embed.set_footer(text=f"☽ SHADOWSEEKERS ORDER · {len(data['members'])} total operatives")

    lb_ch = discord.utils.get(interaction.guild.text_channels, name=LEADERBOARD_CHANNEL) if interaction.guild else None
    if lb_ch and lb_ch.id != interaction.channel_id:
        await lb_ch.send(embed=embed)
        await interaction.followup.send(embed=make_embed("◈ POSTED", f"Leaderboard posted in {lb_ch.mention}", color=0x7B2FBE))
    else:
        await interaction.followup.send(embed=embed)


@tree.command(name="vcleaderboard", description="Top operatives by total voice channel time")
async def vcleaderboard(interaction: discord.Interaction):
    await interaction.response.defer()

    data     = await load_data()
    vc_time  = data.get("vc_time", {})

    if not vc_time:
        await interaction.followup.send(
            embed=make_embed("▲ NO VC DATA", "No voice channel time recorded yet. Join a VC to start earning.", color=0xE63946)
        )
        return

    entries = []
    for uid, seconds in vc_time.items():
        link = data["links"].get(uid, {})
        if not link.get("approved"):
            continue
        shadow_id    = link["shadow_id"]
        member       = next((m for m in data["members"] if m["shadowId"] == shadow_id), None)
        codename     = member.get("codename", shadow_id) if member else shadow_id
        guild_member = interaction.guild.get_member(int(uid)) if interaction.guild else None
        entries.append({"uid": uid, "codename": codename, "shadow_id": shadow_id,
                        "seconds": int(seconds), "guild_member": guild_member})

    entries.sort(key=lambda x: x["seconds"], reverse=True)
    top = entries[:10]

    if not top:
        await interaction.followup.send(embed=make_embed("▲ NO DATA", "No linked operatives with VC time yet.", color=0xE63946))
        return

    lines  = []
    medals = ["🥇","🥈","🥉"]
    for i, e in enumerate(top):
        rank    = medals[i] if i < 3 else f"`#{i+1}`"
        h       = e["seconds"] // 3600
        m_left  = (e["seconds"] % 3600) // 60
        mention = e["guild_member"].mention if e["guild_member"] else f"**{e['codename']}**"
        live    = " 🔴 LIVE" if (e["guild_member"] and e["guild_member"].voice and e["guild_member"].voice.channel) else ""
        lines.append(f"{rank} {mention} · **{h}h {m_left}m**{live}")

    total_hrs = sum(e["seconds"] for e in entries) // 3600
    embed = make_embed("🎙️ VC LEADERBOARD — MOST TIME IN THE VOID", "\n".join(lines), color=0x10B981)
    embed.set_footer(text=f"☽ SHADOWSEEKERS ORDER · {total_hrs}h total VC time across all operatives")

    vc_lb_ch = discord.utils.get(interaction.guild.text_channels, name=VC_LEADERBOARD_CHANNEL) if interaction.guild else None
    if vc_lb_ch and vc_lb_ch.id != interaction.channel_id:
        await vc_lb_ch.send(embed=embed)
        await interaction.followup.send(embed=make_embed("◈ POSTED", f"VC Leaderboard posted in {vc_lb_ch.mention}", color=0x7B2FBE))
    else:
        await interaction.followup.send(embed=embed)



# ── /link ─────────────────────────────────────────────────────────
@tree.command(name="link", description="Bind your Discord identity to your Shadow ID")
@app_commands.describe(
    shadow_id="Your Shadow ID (e.g. SS0069)",
    name="Your operative name"
)
async def link(interaction: discord.Interaction, shadow_id: str, name: str):
    # Defer immediately — admin channel notify happens before response which can exceed 3s
    await interaction.response.defer()

    data     = await load_data()
    uid      = str(interaction.user.id)
    sid      = shadow_id.upper().strip()
    codename = name.strip()

    if get_shadow_id(uid, data):
        await interaction.followup.send(
            embed=make_embed("▲ ALREADY LINKED", f"Already linked to `{data['links'][uid]['shadow_id']}`.", color=0xE63946),
        )
        return

    if not re.match(r'^SS\d{4}$', sid):
        await interaction.followup.send(
            embed=make_embed("▲ INVALID SHADOW ID", "The format must be `SS####` — e.g. `SS0069`. Check your credentials.", color=0xE63946),
        )
        return

    if not codename:
        await interaction.followup.send(
            embed=make_embed("▲ NAME REQUIRED", "You must provide your operative name to link.", color=0xE63946),
        )
        return

    for existing_link in data["links"].values():
        if existing_link["shadow_id"] == sid and existing_link.get("approved"):
            await interaction.followup.send(
                embed=make_embed("▲ ID ALREADY TAKEN", f"`{sid}` is already linked to someone else. Contact an admin if this is wrong.", color=0xE63946),
            )
            return

    data["pending_links"][uid] = {"shadow_id": sid, "codename": codename}
    await save_data(data)

    ch = discord.utils.get(interaction.guild.text_channels, name=APPROVE_CH)
    if ch:
        admin_role   = discord.utils.get(interaction.guild.roles, name=ADMIN_ROLE)
        role_mention = admin_role.mention if admin_role else f"@{ADMIN_ROLE}"
        embed = make_embed(
            "◈ LINK REQUEST",
            f"{interaction.user.mention} wants to link `{sid}` as **{codename}**\n\n"
            f"Use `/approve @{interaction.user.display_name}` to authorize.",
            color=0xF0A500
        )
        await ch.send(content=f"{role_mention} — new link request awaiting authorization.", embed=embed)

    await interaction.followup.send(
        embed=make_embed(
            "◈ REQUEST SENT",
            f"Your request to link `{sid}` as **{codename}** has been sent into the void.\nAwait authorization from the Order.",
            color=0xA855F7
        ),
    )

# ── /approve ──────────────────────────────────────────────────────
@tree.command(name="approve", description="[HIGH CLEARANCE] Authorize an operative's identity bind")
@app_commands.describe(user="The operative to authorize")
async def approve(interaction: discord.Interaction, user: discord.Member):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "This command is restricted to those with high clearance.", color=0xE63946))
        return

    data    = await load_data()
    uid     = str(user.id)
    pending = data["pending_links"].get(uid)

    if not pending:
        await interaction.response.send_message(
            embed=make_embed("▲ NO REQUEST FOUND", f"**{user.display_name}** has no pending link request.", color=0xE63946),
        )
        return

    if isinstance(pending, dict):
        sid      = pending["shadow_id"]
        codename = pending.get("codename", user.display_name)
    else:
        sid      = pending
        codename = user.display_name

    data["links"][uid] = {"shadow_id": sid, "approved": True, "codename": codename}
    del data["pending_links"][uid]

    new_member = {
        "shadowId":  sid,
        "codename":  codename,
        "discordId": uid,
        "echoCount": 0,
        "badges":    {},
    }
    gas_ok = await create_member_on_gas(new_member)

    if not any(m["shadowId"] == sid for m in data["members"]):
        data["members"].append(new_member)

    await save_data(data)

    status_note = "" if gas_ok else "\n⚠️ Shadow Records sync failed — member added locally, retry `/sync`."

    try:
        await user.send(embed=make_embed(
            "☽ LINK APPROVED",
            f"You're now linked to `{sid}` as **{codename}**.\nStep into the shadows — use `/todo` and `/echoes`.",
            color=0x10B981
        ))
    except:
        pass

    await interaction.response.send_message(
        embed=make_embed(
            "◉ APPROVED",
            f"**{user.display_name}** is now linked to `{sid}` as **{codename}**."
            f"\nShadow Records account created.{status_note}",
            color=0x10B981
        ),
    )

# ── /give ─────────────────────────────────────────────────────────
@tree.command(name="give", description="[HIGH CLEARANCE] Channel echoes to an operative")
@app_commands.describe(user="The operative to channel echoes to", amount="Echo amount (can be negative)")
async def give(interaction: discord.Interaction, user: discord.Member, amount: int):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "This command is restricted to those with high clearance.", color=0xE63946))
        return

    data      = await load_data()
    uid       = str(user.id)
    shadow_id = get_shadow_id(uid, data)

    if not shadow_id:
        await interaction.response.send_message(
            embed=make_embed("▲ OPERATIVE UNBOUND", f"**{user.display_name}** has no bound Shadow ID.", color=0xE63946),
        )
        return

    for i, m in enumerate(data["members"]):
        if m["shadowId"] == shadow_id:
            old = int(m.get("echoCount", 0))
            new = max(0, old + amount)
            data["members"][i]["echoCount"] = new
            await save_data(data)
            sign = "+" if amount >= 0 else ""
            await interaction.response.send_message(
                embed=make_embed(
                    "◉ ECHOES CHANNELED",
                    f"**{m['codename']}** (`{shadow_id}`)\n`{old:,}` → **{new:,}** ({sign}{amount:,})\nEcho count updated.",
                    color=0x10B981
                )
            )
            asyncio.create_task(push_to_gas(data))
            return

    await interaction.response.send_message(embed=make_embed("▲ OPERATIVE NOT FOUND", "No record found. Check the Shadow ID.", color=0xE63946))

# ── /setbase ──────────────────────────────────────────────────────
@tree.command(name="setbase", description="[HIGH CLEARANCE] Recalibrate the daily echo resonance threshold")
@app_commands.describe(amount="Base echoes per cycle for full dossier completion")
async def setbase(interaction: discord.Interaction, amount: int):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "This command is restricted to those with high clearance.", color=0xE63946))
        return
    data = await load_data()
    data["base_echo_rate"] = max(1, amount)
    await save_data(data)
    await interaction.response.send_message(
        embed=make_embed("◉ RESONANCE RECALIBRATED", f"The daily echo threshold has been set to **{amount:,}**.", color=0x10B981)
    )

# ── /forceday ─────────────────────────────────────────────────────
@tree.command(name="forceday", description="[HIGH CLEARANCE] Force the midnight echo reckoning")
async def forceday(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "This command is restricted to those with high clearance.", color=0xE63946))
        return
    await interaction.response.send_message(
        embed=make_embed("◉ DAILY RESET STARTED", "Calculating echoes for all members...", color=0xA855F7)
    )
    results     = await run_end_of_day(interaction.guild)
    total_given = sum(r["earned"] for r in results)
    await interaction.followup.send(
        embed=make_embed(
            "☽ DAILY RESET DONE",
            f"**{len(results)}** operatives assessed · **{total_given:,}** echoes awarded · Archive synced.",
            color=0x10B981
        )
    )

# ── /sync ─────────────────────────────────────────────────────────
@tree.command(name="sync", description="[HIGH CLEARANCE] Pull the latest operative data from the shadow archive")
async def sync_cmd(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "This command is restricted to those with high clearance.", color=0xE63946))
        return
    await interaction.response.send_message(embed=make_embed("◉ SYNCING", "Loading operative data from MongoDB...", color=0xA855F7))
    data = await load_data()
    if data.get("members"):
        await interaction.followup.send(
            embed=make_embed("◉ SYNC COMPLETE", f"**{len(data['members'])}** operatives loaded from MongoDB.", color=0x10B981),
        )
        asyncio.create_task(_push_members_to_gas_bg(data))
    else:
        await interaction.followup.send(
            embed=make_embed("▲ NO DATA", "No operative records found in MongoDB.", color=0xE63946),
        )

# ── /syncids ──────────────────────────────────────────────────────
async def bulk_syncids_on_gas(members: list) -> dict:
    try:
        payload = json.dumps({"action": "syncids", "members": members})
        async with aiohttp.ClientSession() as session:
            async with session.post(
                GAS_URL,
                data=payload,
                headers={"Content-Type": "text/plain"}
            ) as resp:
                text = await resp.text()
                result = json.loads(text)
                return result if isinstance(result, dict) else {}
    except Exception as e:
        print(f"[GAS SYNCIDS ERROR] {e}")
    return {}

@tree.command(name="syncids", description="[HIGH CLEARANCE] Create website records for all approved Discord-linked IDs")
async def syncids(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ CLEARANCE DENIED", "This command is restricted to those with high clearance.", color=0xE63946)
        )
        return

    await interaction.response.send_message(
        embed=make_embed("◉ SYNCING IDs", "Scanning approved links and pushing missing records to Shadow Records...", color=0xA855F7)
    )

    data = await load_data()
    mongo_echo_cache = {m["shadowId"]: int(m.get("echoCount", 0)) for m in data["members"]}
    # MongoDB is primary — no GAS fetch needed

    to_sync     = []
    id_to_label = {}

    for discord_id, link in data["links"].items():
        if not link.get("approved"):
            continue
        sid      = link["shadow_id"]
        codename = link.get("codename", f"Operative {sid}")
        to_sync.append({
            "shadowId":  sid,
            "codename":  codename,
            "discordId": discord_id,
            "echoCount": mongo_echo_cache.get(sid, 0),
        })
        id_to_label[sid] = f"`{sid}` **{codename}**"

    if not to_sync:
        await interaction.followup.send(
            embed=make_embed("◈ NOTHING TO SYNC", "No approved links found.", color=0x6B6B9A)
        )
        return

    gas_result = await bulk_syncids_on_gas(to_sync)

    gas_created = gas_result.get("created", [])
    for m in to_sync:
        if m["shadowId"] in gas_created:
            if not any(x["shadowId"] == m["shadowId"] for x in data["members"]):
                data["members"].append(m)
    await save_data(data)
    await push_to_gas(data)

    created = [id_to_label.get(sid, f"`{sid}`") for sid in gas_result.get("created", [])]
    skipped = gas_result.get("skipped", [])
    failed  = [id_to_label.get(sid, f"`{sid}`") for sid in gas_result.get("failed",  [])]

    lines = []
    if created:
        lines.append(f"**✅ Created ({len(created)}):**\n" + "\n".join(created))
    if skipped:
        lines.append(f"**⏭ Already existed ({len(skipped)}):** " + ", ".join(f"`{s}`" for s in skipped))
    if failed:
        lines.append(f"**⚠️ Failed ({len(failed)}):**\n" + "\n".join(failed))
    if not lines:
        lines.append("No approved links found to process.")

    total_echoes = sum(m["echoCount"] for m in to_sync)

    await interaction.followup.send(
        embed=make_embed(
            "☽ ID SYNC COMPLETE",
            "\n\n".join(lines) + f"\n\n*Echo counts pushed for all {len(data['members'])} operatives · {total_echoes:,} total echoes on record.*",
            color=0x10B981 if not failed else 0xF0A500
        )
    )

# ── /welcome ──────────────────────────────────────────────────────
async def _generate_welcome_text(welcomer_name: str, new_member_name: str) -> str:
    if not GROQ_API_KEY_MAIN:
        return (
            f"*The void stirs. A new shadow joins the Order.*\n\n"
            f"Welcome, **{new_member_name}** — the darkness has been waiting.\n"
            f"You were brought here by **{welcomer_name}**, a trusted operative.\n\n"
            f"The ShadowSeekers do not seek the light. We build in the dark, grind in silence, "
            f"and emerge as something the world wasn't ready for.\n\n"
            f"*Step in. The Order watches.*"
        )

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY_MAIN}",
        "Content-Type": "application/json",
    }
    prompt = (
        f"Write a welcome message for a new member named '{new_member_name}' "
        f"who was welcomed by operative '{welcomer_name}' into the ShadowSeekers Order — "
        f"a secret society of elite, high-performance individuals who study, grind, and build in silence. "
        f"The tone is dark, atmospheric, cinematic — like a covert society initiation. "
        f"Reference the darkness, the void, echoes, shadows, and the grind. "
        f"Mention both names naturally. Keep it 4–6 sentences. "
        f"Do NOT use markdown headers. No hashtags. Just pure atmospheric prose."
    )
    payload = {
        "model": GROQ_MODEL_MAIN,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are the voice of the ShadowSeekers Order — a secret society of elite operatives "
                    "who grind in silence and build in the dark. Your welcome messages are cinematic, "
                    "atmospheric, and feel like an initiation ritual. Dark, poetic, powerful. "
                    "Never generic. Always specific to the names given."
                )
            },
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.9,
        "max_tokens": 300,
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(GROQ_API_URL_MAIN, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data_r = await resp.json()
                    return data_r["choices"][0]["message"]["content"].strip()
                else:
                    print(f"[WELCOME AI] Groq error {resp.status}")
    except Exception as e:
        print(f"[WELCOME AI] Error: {e}")

    return (
        f"*The void stirs. A new shadow joins the Order.*\n\n"
        f"**{new_member_name}**, you were brought here by **{welcomer_name}** — and the Order does not forget a debt.\n\n"
        f"From this moment, you grind in the dark. You build in silence. You earn your echoes.\n\n"
        f"*Welcome to the ShadowSeekers. The darkness has been waiting.*"
    )


@tree.command(name="welcome", description="Welcome a new operative into the ShadowSeekers Order")
@app_commands.describe(member="The operative to welcome into the Order")
async def welcome(interaction: discord.Interaction, member: discord.Member):
    if member.bot:
        await interaction.response.send_message(
            embed=make_embed("▲ INVALID TARGET", "You can't welcome a bot into the Order.", color=0xE63946),
            ephemeral=True
        )
        return

    if member.id == interaction.user.id:
        await interaction.response.send_message(
            embed=make_embed("▲ NICE TRY", "You cannot welcome yourself, operative.", color=0xE63946),
            ephemeral=True
        )
        return

    await interaction.response.defer()

    welcomer_name    = interaction.user.display_name
    new_member_name  = member.display_name
    welcome_text     = await _generate_welcome_text(welcomer_name, new_member_name)

    embed = discord.Embed(
        description=f"🦇 *{welcome_text}*",
        color=0x7B2FBE
    )
    embed.set_author(
        name="☽ THE ORDER WELCOMES A NEW SHADOW",
        icon_url=member.display_avatar.url if member.display_avatar else None
    )
    embed.add_field(
        name="Welcomed by",
        value=interaction.user.mention,
        inline=True
    )
    embed.add_field(
        name="New Operative",
        value=member.mention,
        inline=True
    )
    embed.set_thumbnail(url=member.display_avatar.url if member.display_avatar else None)
    embed.set_footer(text="☽ SHADOWSEEKERS ORDER · DEEP IN THE DARK, I DON'T NEED THE LIGHT")

    await interaction.followup.send(
        content=f"{interaction.user.mention} welcomes {member.mention} into the void 🦇",
        embed=embed
    )


# ══════════════════════════════════════════════════════════════════
#  /ask — AI OPERATIVE ADVISOR (context-aware)
# ══════════════════════════════════════════════════════════════════

async def _build_user_context(uid: str, data: dict) -> str:
    """Build a rich context string about the operative for the AI."""
    today  = today_str()
    lines  = []

    # Identity
    shadow_id = get_shadow_id(uid, data)
    if shadow_id:
        member = get_member(shadow_id, data)
        codename   = member.get("codename", shadow_id) if member else shadow_id
        echo_count = int(member.get("echoCount", 0)) if member else 0
        tier = get_tier(echo_count)
        lines.append(f"OPERATIVE: {codename} (Shadow ID: {shadow_id})")
        lines.append(f"RANK: {tier['name']} · {echo_count} echoes")
    else:
        lines.append("OPERATIVE: Unlinked (no Shadow ID)")

    # Today's todos
    active_date = get_active_date(uid, data)
    todos = get_todos_for_date(uid, active_date, data)
    if todos:
        done   = [t for t in todos if isinstance(t, dict) and t.get("done")]
        undone = [t for t in todos if isinstance(t, dict) and not t.get("done") and "task" in t]
        lines.append(f"\nTODAY'S OBJECTIVES ({active_date}):")
        for t in undone:
            ops = t.get("ops", [])
            ops_str = f" [{sum(1 for op in ops if op.get('done'))}/{len(ops)} ops done]" if ops else ""
            lines.append(f"  PENDING: {t['task']}{ops_str}")
        for t in done:
            lines.append(f"  DONE: {t['task']}")
    else:
        lines.append("\nTODAY'S OBJECTIVES: None added yet.")

    # Active session
    sess = data.get("active_sessions", {}).get(uid)
    if sess and isinstance(sess, dict):
        elapsed = int(time_module.time() - sess.get("start_time", 0))
        lines.append(f"\nACTIVE SESSION: '{sess.get('task','?')}' · {format_duration(elapsed)} elapsed · type={sess.get('session_type','study')}")
    else:
        lines.append("\nACTIVE SESSION: None")

    # Session history (last 7)
    history = data.get("session_history", {}).get(uid, [])
    if history:
        recent = sorted(history, key=lambda x: x.get("date", ""), reverse=True)[:7]
        lines.append("\nRECENT SESSION HISTORY (last 7):")
        for h in recent:
            lines.append(f"  {h.get('date','?')} · {h.get('session_type','study')} · {format_duration(h.get('duration_seconds',0))} · '{h.get('task','?')}'")
    else:
        lines.append("\nRECENT SESSION HISTORY: None.")

    # Exams
    exams = data.get("exams", {}).get(uid, [])
    if exams:
        lines.append("\nUPCOMING EXAMS:")
        for e in sorted(exams, key=lambda x: x.get("date", "")):
            days = _days_until(e["date"])
            lines.append(f"  {e['name']} · {e['date']} · {_format_exam_countdown(days)}")
    else:
        lines.append("\nUPCOMING EXAMS: None.")

    return "\n".join(lines)


@tree.command(name="ask", description="Ask the Shadow AI anything — it knows your todos, sessions, echoes, and exams")
@app_commands.describe(question="e.g. 'How am I doing today?', 'What should I focus on?', 'Am I on track?'")
async def ask_ai(interaction: discord.Interaction, question: str):
    await interaction.response.defer()

    data = await load_data()
    uid  = str(interaction.user.id)
    context = await _build_user_context(uid, data)

    if not GROQ_API_KEY_MAIN:
        await interaction.followup.send(
            embed=make_embed("▲ AI OFFLINE", "No GROQ_API_KEY configured.", color=0xE63946)
        )
        return

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY_MAIN}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL_MAIN,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are the Shadow AI — an elite performance advisor inside the ShadowSeekers Order, "
                    "a secret society of high-performance individuals who grind in silence and build in the dark. "
                    "Your tone is sharp, direct, and tactical — like a seasoned commander briefing an operative. "
                    "You have full access to the operative's real-time data below. "
                    "Always reference their actual tasks, numbers, and streaks — never be generic. "
                    "Give specific, actionable advice. Keep responses under 220 words. "
                    "Use ☽ and ◈ for structure. No markdown headers.\n\n"
                    f"=== OPERATIVE DATA ===\n{context}\n=== END DATA ==="
                )
            },
            {"role": "user", "content": question}
        ],
        "temperature": 0.75,
        "max_tokens": 400,
    }

    try:
        async with aiohttp.ClientSession() as http_session:
            async with http_session.post(
                GROQ_API_URL_MAIN, headers=headers, json=payload,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status == 200:
                    data_r = await resp.json()
                    answer = data_r["choices"][0]["message"]["content"].strip()
                    embed  = make_embed("◈ SHADOW AI", answer, color=0x7B2FBE)
                    embed.set_author(
                        name=f"Query: {question[:60]}{'...' if len(question) > 60 else ''}",
                        icon_url=interaction.user.display_avatar.url if interaction.user.display_avatar else None
                    )
                    await interaction.followup.send(embed=embed)
                else:
                    err = await resp.text()
                    print(f"[ASK AI] Groq {resp.status}: {err[:200]}")
                    await interaction.followup.send(
                        embed=make_embed("▲ AI ERROR", f"Shadow AI failed (HTTP {resp.status}).", color=0xE63946)
                    )
    except Exception as e:
        print(f"[ASK AI] Exception: {e}")
        await interaction.followup.send(
            embed=make_embed("▲ AI ERROR", "Shadow AI is unreachable right now.", color=0xE63946)
        )


# ══════════════════════════════════════════════════════════════════
#  /plan — Operative Plan Commands
# ══════════════════════════════════════════════════════════════════

plan_group = app_commands.Group(name="plan", description="Manage your operative plan")
tree.add_command(plan_group)


@plan_group.command(name="new", description="Start building a new operative plan with SHADOW")
async def plan_new(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    uid = str(interaction.user.id)
    data = await load_data()
    shadow_id = get_shadow_id(uid, data)
    if not shadow_id:
        await interaction.followup.send(embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first — use `/link`.", color=0xE63946))
        return

    from shadow_ai import start_plan_new

    # We need a fake message-like object pointing to the right channel
    class _FakeMsg:
        author = interaction.user
        channel = interaction.channel

    await interaction.followup.send(embed=make_embed("◈ PLAN PROTOCOL", "Initiating plan-building sequence. Respond in this channel.", color=0x7B2FBE))
    await start_plan_new(_FakeMsg(), load_data, get_db)


@plan_group.command(name="view", description="View your current operative plan")
async def plan_view(interaction: discord.Interaction):
    await interaction.response.defer()
    uid = str(interaction.user.id)
    data = await load_data()
    shadow_id = get_shadow_id(uid, data)
    if not shadow_id:
        await interaction.followup.send(embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first.", color=0xE63946))
        return

    from shadow_ai import get_plan
    plan = await get_plan(uid, get_db)
    if not plan:
        await interaction.followup.send(embed=make_embed("▲ NO PLAN", "No operative plan on file. Use `/plan new` to create one.", color=0xE63946))
        return

    embed = make_embed("☽ OPERATIVE PLAN", plan.get("plan_text", "No details."), color=0x7B2FBE)
    embed.add_field(name="Goal", value=plan.get("goal", "—"), inline=True)
    embed.add_field(name="Timeline", value=plan.get("timeline", "—"), inline=True)
    embed.add_field(name="Hours/Day", value=str(plan.get("hours_per_day", "—")), inline=True)
    subjects = plan.get("subjects", [])
    if subjects:
        embed.add_field(name="Subjects", value=" · ".join(subjects), inline=False)
    created = plan.get("created_at", "")
    if created:
        embed.set_footer(text=f"Plan locked: {created[:10]}")
    await interaction.followup.send(embed=embed)


@plan_group.command(name="revise", description="Revise your existing operative plan with SHADOW")
async def plan_revise(interaction: discord.Interaction):
    await interaction.response.defer()
    uid = str(interaction.user.id)
    data = await load_data()
    shadow_id = get_shadow_id(uid, data)
    if not shadow_id:
        await interaction.followup.send(embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first.", color=0xE63946))
        return

    from shadow_ai import start_plan_revise

    class _FakeMsg:
        author = interaction.user
        channel = interaction.channel

    await interaction.followup.send(embed=make_embed("◈ PLAN REVISION", "Loading your plan. Respond in this channel.", color=0x7B2FBE))
    await start_plan_revise(_FakeMsg(), load_data, get_db)


@plan_group.command(name="delete", description="Delete your operative plan permanently")
async def plan_delete(interaction: discord.Interaction):
    await interaction.response.defer()
    uid = str(interaction.user.id)
    data = await load_data()
    shadow_id = get_shadow_id(uid, data)
    if not shadow_id:
        await interaction.followup.send(embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first.", color=0xE63946))
        return

    from shadow_ai import gas_delete_plan, mongo_delete_plan_cache, get_plan
    plan = await get_plan(uid, get_db)
    if not plan:
        await interaction.followup.send(embed=make_embed("▲ NO PLAN", "No plan on file to delete.", color=0xE63946))
        return

    await gas_delete_plan(uid)
    await mongo_delete_plan_cache(uid, get_db)
    await interaction.followup.send(embed=make_embed("◈ PLAN DELETED", "Operative plan has been wiped from the records.", color=0x10B981))


# ══════════════════════════════════════════════════════════════════
#  /newchat — Reset AI conversation history
# ══════════════════════════════════════════════════════════════════

@tree.command(name="newchat", description="Clear your SHADOW conversation history and start fresh")
async def newchat(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    uid = str(interaction.user.id)
    from shadow_ai import clear_user_chat
    await clear_user_chat(uid)
    await interaction.followup.send(
        embed=make_embed(
            "◈ MEMORY WIPED",
            "Conversation history cleared. SHADOW still knows your rank, echoes, and objectives — just not what you were talking about.",
            color=0x10B981,
        ),
        ephemeral=True,
    )


# ══════════════════════════════════════════════════════════════════
#  /token — Shadow Token management
# ══════════════════════════════════════════════════════════════════

TOKEN_TIERS = [
    {"tokens": 50,  "echoes": 100},
    {"tokens": 150, "echoes": 250},
    {"tokens": 500, "echoes": 700},
]


class TokenTierView(discord.ui.View):
    def __init__(self, uid: str, member_data: dict, current_tokens: int, echo_count: int):
        super().__init__(timeout=60)
        self.uid = uid
        self.member_data = member_data
        self.current_tokens = current_tokens
        self.echo_count = echo_count

        for tier in TOKEN_TIERS:
            can_afford = echo_count >= tier["echoes"]
            btn = discord.ui.Button(
                label=f"+{tier['tokens']} tokens — {tier['echoes']} echoes",
                style=discord.ButtonStyle.green if can_afford else discord.ButtonStyle.gray,
                disabled=not can_afford,
                custom_id=f"token_{tier['tokens']}_{tier['echoes']}",
            )
            btn.callback = self._make_callback(tier)
            self.add_item(btn)

    def _make_callback(self, tier: dict):
        async def callback(interaction: discord.Interaction):
            if str(interaction.user.id) != self.uid:
                await interaction.response.send_message("This isn't your token menu.", ephemeral=True)
                return

            await interaction.response.defer()

            from shadow_ai import d1_get_tokens, d1_set_tokens
            # Re-fetch live echo count
            data = await load_data()
            uid = self.uid
            shadow_id = get_shadow_id(uid, data)
            member = get_member(shadow_id, data) if shadow_id else None
            if not member:
                await interaction.followup.send("Member data not found.", ephemeral=True)
                return

            echo_count = int(member.get("echoCount", 0))
            if echo_count < tier["echoes"]:
                await interaction.followup.send(
                    embed=make_embed("▲ INSUFFICIENT ECHOES", f"You need {tier['echoes']} echoes. You have {echo_count}.", color=0xE63946),
                    ephemeral=True,
                )
                return

            # Deduct echoes
            member["echoCount"] = str(echo_count - tier["echoes"])
            await save_data(data)
            await push_to_gas(data)

            # Add tokens
            current = await d1_get_tokens(uid) or 0
            new_total = current + tier["tokens"]
            await d1_set_tokens(uid, new_total)

            embed = make_embed(
                "◈ TOKENS ACQUIRED",
                f"**+{tier['tokens']} shadow tokens** added to your reserves.\n\n"
                f"◈ Token balance: **{new_total}**\n"
                f"☽ Echo balance: **{echo_count - tier['echoes']}**",
                color=0x10B981,
            )
            await interaction.followup.send(embed=embed)
            for child in self.children:
                child.disabled = True
            self.stop()

        return callback


@tree.command(name="token", description="View your shadow token balance or purchase more")
async def token_cmd(interaction: discord.Interaction):
    await interaction.response.defer()
    uid = str(interaction.user.id)
    data = await load_data()
    shadow_id = get_shadow_id(uid, data)
    if not shadow_id:
        await interaction.followup.send(embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first.", color=0xE63946))
        return

    member = get_member(shadow_id, data)
    if not member:
        await interaction.followup.send(embed=make_embed("▲ NOT FOUND", "Member data not found.", color=0xE63946))
        return

    echo_count = int(member.get("echoCount", 0))

    from shadow_ai import get_tokens
    current_tokens = await get_tokens(uid)

    embed = make_embed(
        "☽ SHADOW TOKEN RESERVES",
        f"**Token balance:** {current_tokens} tokens\n"
        f"**Echo balance:** {echo_count} echoes\n\n"
        f"Each @ mention with SHADOW costs **1 token**.\n"
        f"Select a tier below to restock:",
        color=0x7B2FBE,
    )
    embed.add_field(name="Tier I",   value="50 tokens — 100 echoes",  inline=True)
    embed.add_field(name="Tier II",  value="150 tokens — 250 echoes", inline=True)
    embed.add_field(name="Tier III", value="500 tokens — 700 echoes", inline=True)

    view = TokenTierView(uid, member, current_tokens, echo_count)
    await interaction.followup.send(embed=embed, view=view)


# ── BOT EVENTS ────────────────────────────────────────────────────

@bot.event
async def on_member_join(member: discord.Member):
    """Ghost Guide — welcome embed in #general + AI intro DM to new recruit."""
    await ghost_send_welcome(member, get_db, bot)


@bot.event
async def on_message(message: discord.Message):
    """Handle @Shadowbot mentions, Ghost DM replies, and /train + /setwelcome custom channel sessions."""
    if message.author.bot:
        return

    uid = str(message.author.id)

    # DM handler — no ping needed in DMs
    if isinstance(message.channel, discord.DMChannel):
        if ghost_is_active(uid):
            await ghost_handle_dm(message, get_db)
            return
        # Any DM message goes to Shadow AI — no ping required
        await handle_mention(message, bot, load_data, save_data, get_db)
        return

    # Channel-only session handlers (admin tools)
    if not isinstance(message.channel, discord.DMChannel):
        # /train: intercept channel messages from admins in a training session
        if train_is_active(uid):
            await train_handle_message(message, get_db)
            return
        # /setwelcome custom: intercept messages from admins designing a custom welcome
        if welcome_custom_is_active(uid):
            await setwelcome_custom_handle_message(message, get_db)
            return
        # /setwelcome dm: intercept messages from admins designing the DM intro style
        if dm_design_is_active(uid):
            await setwelcome_dm_handle_message(message, get_db)
            return

    if bot.user in message.mentions:
        await handle_mention(message, bot, load_data, save_data, get_db)
    else:
        # Route follow-up messages for active multi-step action flows (link, session, admin todo)
        from shadow_ai import _pending_actions, dispatch_natural_language_action, passive_observe
        if str(message.author.id) in _pending_actions:
            content = message.content.strip()
            await dispatch_natural_language_action(message, content, load_data, save_data, get_db)
            return
        # Passive observer — reads all messages, reacts silently, rarely speaks
        await passive_observe(message, load_data, save_data)
    await bot.process_commands(message)


# ══════════════════════════════════════════════════════════════════
# /TRAIN — Admin-only Ghost knowledge builder
# ══════════════════════════════════════════════════════════════════

def _is_admin(interaction: discord.Interaction) -> bool:
    role = discord.utils.get(interaction.guild.roles, name=ADMIN_ROLE)
    return role in interaction.user.roles if role else interaction.user.guild_permissions.administrator


train_group = app_commands.Group(name="train", description="Admin: build Ghost's knowledge base")
tree.add_command(train_group)


@train_group.command(name="start", description="Start an AI-guided training session to teach Ghost about your server")
async def train_start_cmd(interaction: discord.Interaction):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946),
            ephemeral=True,
        )
        return
    await train_start(interaction, get_db)


@train_group.command(name="stop", description="Cancel your active training session")
async def train_stop_cmd(interaction: discord.Interaction):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946),
            ephemeral=True,
        )
        return
    await train_stop(interaction)


@train_group.command(name="list", description="View all knowledge docs saved to Ghost's knowledge base")
async def train_list_cmd(interaction: discord.Interaction):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946),
            ephemeral=True,
        )
        return
    await train_list(interaction, get_db)


@train_group.command(name="delete", description="Delete a knowledge doc from Ghost's knowledge base")
@app_commands.describe(doc_id="The doc ID to delete (get IDs from /train list)")
async def train_delete_cmd(interaction: discord.Interaction, doc_id: str):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946),
            ephemeral=True,
        )
        return
    await train_delete(interaction, doc_id, get_db)


# ══════════════════════════════════════════════════════════════════
# /SETWELCOME — Admin customise the AI-generated #general welcome
# ══════════════════════════════════════════════════════════════════

welcome_group = app_commands.Group(name="setwelcome", description="Admin: customise the AI welcome message")
tree.add_command(welcome_group)


@welcome_group.command(name="format", description="Set welcome style: 1-4 presets or 'custom' to design via AI chat")
@app_commands.describe(number="Format number 1–4, or 'custom' to design your own via AI chat")
async def setwelcome_format_cmd(interaction: discord.Interaction, number: str):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    if number.lower() == "custom":
        await setwelcome_custom_start(interaction, get_db)
    else:
        await setwelcome_format(interaction, number, get_db)


@welcome_group.command(name="custom", description="Design a fully custom welcome style by chatting with AI")
async def setwelcome_custom_cmd(interaction: discord.Interaction):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await setwelcome_custom_start(interaction, get_db)


@welcome_group.command(name="dm", description="Design Ghost's DM intro message for new members via AI chat")
async def setwelcome_dm_cmd(interaction: discord.Interaction):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await setwelcome_dm_start(interaction, get_db)


@welcome_group.command(name="tone", description="Add extra tone/vibe instructions for the AI on top of the format")
@app_commands.describe(instructions="e.g. 'Always mention that we value consistency over motivation'")
async def setwelcome_tone_cmd(interaction: discord.Interaction, instructions: str):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await setwelcome_tone(interaction, instructions, get_db)


@welcome_group.command(name="title", description="Override the auto-generated embed title (use {name} for member name)")
@app_commands.describe(title="e.g. '☽ {name} HAS ENTERED THE ORDER'")
async def setwelcome_title_cmd(interaction: discord.Interaction, title: str):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await setwelcome_title_override(interaction, title, get_db)


@welcome_group.command(name="color", description="Set the welcome embed accent color")
@app_commands.describe(hex_color="Hex color, e.g. 7B2FBE or #A855F7")
async def setwelcome_color_cmd(interaction: discord.Interaction, hex_color: str):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await setwelcome_color(interaction, hex_color, get_db)


@welcome_group.command(name="banner", description="Set a banner image at the bottom of the welcome embed")
@app_commands.describe(url="Direct image URL")
async def setwelcome_banner_cmd(interaction: discord.Interaction, url: str):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await setwelcome_banner(interaction, url, get_db)


@welcome_group.command(name="preview", description="Generate a live AI preview of the welcome using current settings")
async def setwelcome_preview_cmd(interaction: discord.Interaction):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await setwelcome_preview(interaction, get_db)


@welcome_group.command(name="formats", description="See all 4 available welcome format presets")
async def setwelcome_formats_cmd(interaction: discord.Interaction):
    if not _is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ ACCESS DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await setwelcome_formats(interaction)


# ══════════════════════════════════════════════════════════════════
# /admin — Full admin control group [HIGH CLEARANCE]
# ══════════════════════════════════════════════════════════════════
admin_group = app_commands.Group(name="admin", description="[HIGH CLEARANCE] Full admin control over operatives and bot")
tree.add_command(admin_group)


@admin_group.command(name="unlink", description="Remove an operative's Shadow ID link entirely")
@app_commands.describe(user="The operative to unlink")
async def admin_unlink(interaction: discord.Interaction, user: discord.Member):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    data = await load_data()
    uid  = str(user.id)
    sid  = get_shadow_id(uid, data)
    if not sid:
        await interaction.response.send_message(
            embed=make_embed("▲ NOT LINKED", f"**{user.display_name}** has no active link.", color=0xE63946), ephemeral=True)
        return
    data["links"].pop(uid, None)
    data["pending_links"].pop(uid, None)
    await save_data(data)
    await interaction.response.send_message(
        embed=make_embed("◈ UNLINKED",
            f"**{user.display_name}** (`{sid}`) has been unlinked.\n"
            f"They can `/link` again with a new Shadow ID.", color=0xF0A500))


@admin_group.command(name="forcelink", description="Directly link an operative without a pending request")
@app_commands.describe(user="The operative to link", shadow_id="Shadow ID e.g. SS0069", codename="Operative codename")
async def admin_forcelink(interaction: discord.Interaction, user: discord.Member, shadow_id: str, codename: str):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await interaction.response.defer()
    data = await load_data()
    uid  = str(user.id)
    sid  = shadow_id.upper().strip()

    if not re.match(r'^SS\d{4}$', sid):
        await interaction.followup.send(embed=make_embed("▲ INVALID ID", "Format must be `SS####` — e.g. `SS0069`.", color=0xE63946))
        return
    if get_shadow_id(uid, data):
        await interaction.followup.send(embed=make_embed("▲ ALREADY LINKED", f"**{user.display_name}** is already linked. Use `/admin unlink` first.", color=0xE63946))
        return
    for existing_link in data["links"].values():
        if existing_link.get("shadow_id") == sid and existing_link.get("approved"):
            await interaction.followup.send(embed=make_embed("▲ ID TAKEN", f"`{sid}` is already bound to another operative.", color=0xE63946))
            return

    data["links"][uid] = {"shadow_id": sid, "approved": True, "codename": codename.strip()}
    data["pending_links"].pop(uid, None)
    new_member = {"shadowId": sid, "codename": codename.strip(), "discordId": uid, "echoCount": 0, "badges": {}}
    gas_ok = await create_member_on_gas(new_member)
    if not any(m["shadowId"] == sid for m in data["members"]):
        data["members"].append(new_member)
    await save_data(data)
    try:
        await user.send(embed=make_embed(
            "☽ LINK CONFIRMED",
            f"You've been linked to `{sid}` as **{codename}** by an admin.\n"
            f"Use `/todo` and `/echoes` to get started.", color=0x10B981))
    except Exception:
        pass
    sync_note = "" if gas_ok else "\n⚠️ Shadow Records sync failed — retry `/sync`."
    await interaction.followup.send(
        embed=make_embed("◉ FORCE LINKED", f"**{user.display_name}** → `{sid}` as **{codename}**.{sync_note}", color=0x10B981))


@admin_group.command(name="setexam", description="Add an exam entry for any operative")
@app_commands.describe(user="The operative", name="Exam name e.g. JEE Advanced", date="Date MM/DD/YYYY")
async def admin_setexam(interaction: discord.Interaction, user: discord.Member, name: str, date: str):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    date = date.strip()
    if not re.match(r'^\d{2}/\d{2}/\d{4}$', date):
        await interaction.response.send_message(embed=make_embed("▲ INVALID DATE", "Use `MM/DD/YYYY` — e.g. `05/25/2025`.", color=0xE63946))
        return
    try:
        datetime.strptime(date, "%m/%d/%Y")
    except ValueError:
        await interaction.response.send_message(embed=make_embed("▲ INVALID DATE", f"`{date}` is not a real date.", color=0xE63946))
        return
    data = await load_data()
    uid  = str(user.id)
    if "exams" not in data:
        data["exams"] = {}
    if uid not in data["exams"]:
        data["exams"][uid] = []
    data["exams"][uid].append({"name": name.strip(), "date": date, "source": "admin"})
    await save_data(data)
    days = _days_until(date)
    await interaction.response.send_message(
        embed=make_embed("📅 EXAM SET",
            f"Added **{name}** on `{date}` for **{user.display_name}**.\n"
            f"⏳ {_format_exam_countdown(days)}", color=0x10B981))


@admin_group.command(name="removeexam", description="Remove an exam from any operative's list by number")
@app_commands.describe(user="The operative", number="Exam number from their /exam list")
async def admin_removeexam(interaction: discord.Interaction, user: discord.Member, number: int):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    data  = await load_data()
    uid   = str(user.id)
    exams = data.get("exams", {}).get(uid, [])
    if not exams or number < 1 or number > len(exams):
        await interaction.response.send_message(
            embed=make_embed("▲ NOT FOUND", f"Exam #{number} doesn't exist for **{user.display_name}**.", color=0xE63946))
        return
    def _sort_key(e):
        try: return datetime.strptime(e["date"], "%m/%d/%Y")
        except: return datetime.max
    sorted_exams = sorted(exams, key=_sort_key)
    removed = sorted_exams[number - 1]
    data["exams"][uid] = [e for e in exams if not (e["name"] == removed["name"] and e["date"] == removed["date"])]
    await save_data(data)
    await interaction.response.send_message(
        embed=make_embed("◈ EXAM REMOVED", f"~~{removed['name']}~~ removed from **{user.display_name}**'s list.", color=0x6B6B9A))


@admin_group.command(name="settodo", description="Add a todo objective directly to any operative's dossier")
@app_commands.describe(user="The operative", task="The objective text")
async def admin_settodo(interaction: discord.Interaction, user: discord.Member, task: str):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    data   = await load_data()
    uid    = str(user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)
    todos.append({"task": task.strip(), "done": False, "priority": None, "ops": [], "source": "admin"})
    set_todos_for_date(uid, active, todos, data)
    await save_data(data)
    await interaction.response.send_message(
        embed=make_embed("◈ OBJECTIVE SET",
            f"Added to **{user.display_name}**'s dossier:\n*{task}*\n\n"
            f"`{len(todos)}` objective(s) total.", color=0x10B981))


@admin_group.command(name="donetodo", description="Mark a todo as done for any operative")
@app_commands.describe(user="The operative", number="Objective number from their dossier")
async def admin_donetodo(interaction: discord.Interaction, user: discord.Member, number: int):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    data   = await load_data()
    uid    = str(user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)
    if not todos or number < 1 or number > len(todos):
        await interaction.response.send_message(
            embed=make_embed("▲ NOT FOUND", f"Objective #{number} doesn't exist for **{user.display_name}**.", color=0xE63946))
        return
    todos[number - 1]["done"] = True
    set_todos_for_date(uid, active, todos, data)
    await save_data(data)
    task = todos[number - 1].get("task") or todos[number - 1].get("text", f"Objective #{number}")
    await interaction.response.send_message(
        embed=make_embed("☽ OBJECTIVE MARKED DONE",
            f"**{user.display_name}**: ~~{task}~~\n"
            f"`{sum(1 for t in todos if t.get('done'))}/{len(todos)}` objectives complete.", color=0x10B981))


@admin_group.command(name="cleartodos", description="Wipe all todos for today from any operative's dossier")
@app_commands.describe(user="The operative")
async def admin_cleartodos(interaction: discord.Interaction, user: discord.Member):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    data   = await load_data()
    uid    = str(user.id)
    active = get_active_date(uid, data)
    set_todos_for_date(uid, active, [], data)
    await save_data(data)
    await interaction.response.send_message(
        embed=make_embed("◈ DOSSIER CLEARED",
            f"**{user.display_name}**'s dossier for `{active}` has been wiped.", color=0x6B6B9A))


@admin_group.command(name="viewtodos", description="View any operative's current dossier")
@app_commands.describe(user="The operative")
async def admin_viewtodos(interaction: discord.Interaction, user: discord.Member):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    data   = await load_data()
    uid    = str(user.id)
    active = get_active_date(uid, data)
    todos  = get_todos_for_date(uid, active, data)
    if not todos:
        await interaction.response.send_message(
            embed=make_embed(f"◈ {user.display_name}'s DOSSIER", "No objectives for today.", color=0x7B2FBE), ephemeral=True)
        return
    lines = []
    for i, t in enumerate(todos, 1):
        if not isinstance(t, dict): continue
        text   = t.get("task") or t.get("text", "?")
        status = "✅" if t.get("done") else "◻️"
        badge  = " 🤖" if t.get("source") in ("ai", "admin") else ""
        lines.append(f"{status} {i}. {text}{badge}")
    done = sum(1 for t in todos if isinstance(t, dict) and t.get("done"))
    await interaction.response.send_message(
        embed=make_embed(f"◈ {user.display_name}'s DOSSIER ({active})",
            "\n".join(lines) + f"\n\n`{done}/{len(todos)}` done", color=0xA855F7), ephemeral=True)


@admin_group.command(name="viewsessions", description="View any operative's session history and weekly analytics")
@app_commands.describe(user="The operative to inspect")
async def admin_viewsessions(interaction: discord.Interaction, user: discord.Member):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    data = await load_data()
    uid  = str(user.id)
    shadow_id = get_shadow_id(uid, data)

    if not shadow_id:
        await interaction.followup.send(embed=make_embed("▲ NOT LINKED", f"**{user.display_name}** has no linked Shadow ID.", color=0xE63946), ephemeral=True)
        return

    member = get_member(shadow_id, data)
    codename   = member.get("codename", shadow_id) if member else shadow_id
    echo_count = int(member.get("echoCount", 0)) if member else 0

    history = data.get("session_history", {}).get(uid, [])

    tz = pytz.timezone(TIMEZONE)
    now_dt = datetime.now(tz)
    week_dates = set()
    for i in range(7):
        d = now_dt - timedelta(days=i)
        week_dates.add(d.strftime("%m/%d"))

    week_sessions  = [s for s in history if s.get("date") in week_dates]
    total_secs     = sum(s.get("duration_seconds", 0) for s in week_sessions)
    total_echoes   = sum(s.get("awarded", 0) for s in week_sessions)
    total_sessions = len(week_sessions)
    vc_sessions    = sum(1 for s in week_sessions if s.get("in_vc"))
    hours_str      = f"{total_secs // 3600}h {(total_secs % 3600) // 60}m"

    # Active session check
    active = data["active_sessions"].get(uid)
    active_note = ""
    if active:
        elapsed = int(time_module.time() - active["start_time"])
        active_note = f"\n\n🔴 **Live now:** {active['task']} · {format_duration(elapsed)} elapsed"

    # Last 10 sessions
    recent = sorted(history, key=lambda x: x.get("date", ""), reverse=True)[:10]
    session_lines = []
    for s in recent:
        dur = format_duration(s.get("duration_seconds", 0))
        echoes = s.get("awarded", 0)
        stype  = "🍅" if s.get("session_type") == "pomodoro" else "🦇"
        vc     = " 🎙️" if s.get("in_vc") else ""
        session_lines.append(f"`{s.get('date','?')}` {stype}{vc} **{s.get('task','?')}** · {dur} · +{echoes}e")

    embed = make_embed(
        f"📊 {codename} · SESSION HISTORY",
        f"**This week:** {total_sessions} sessions · {hours_str} · {total_echoes} echoes"
        f"{'  ·  🎙️ ' + str(vc_sessions) + ' in VC' if vc_sessions else ''}"
        f"{active_note}",
        color=0xA855F7
    )
    if session_lines:
        embed.add_field(name="Recent Sessions (last 10)", value="\n".join(session_lines), inline=False)
    else:
        embed.add_field(name="Recent Sessions", value="No session history recorded yet.", inline=False)

    embed.add_field(name="Shadow ID", value=f"`{shadow_id}`", inline=True)
    embed.add_field(name="Echo Count", value=f"{echo_count:,}", inline=True)
    embed.add_field(name="Total Logged", value=f"{len(history)} sessions all-time", inline=True)

    avatar_url = user.display_avatar.url if user.display_avatar else None
    if avatar_url:
        embed.set_thumbnail(url=avatar_url)
    embed.set_author(name=f"Viewing: {user.display_name}", icon_url=avatar_url)
    embed.set_footer(text="☽ SHADOWSEEKERS ORDER · ADMIN INTEL")

    await interaction.followup.send(embed=embed, ephemeral=True)


@admin_group.command(name="listlinks", description="Show all linked operatives and their Shadow IDs")
async def admin_listlinks(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    data  = await load_data()
    links = [(uid, l) for uid, l in data["links"].items() if l.get("approved")]
    if not links:
        await interaction.response.send_message(
            embed=make_embed("◈ NO LINKS", "No approved links found.", color=0x7B2FBE), ephemeral=True)
        return
    lines = []
    for uid, l in sorted(links, key=lambda x: x[1].get("shadow_id", "")):
        member = interaction.guild.get_member(int(uid))
        dname  = member.display_name if member else f"Unknown ({uid})"
        lines.append(f"`{l['shadow_id']}` **{l.get('codename','?')}** — {dname}")
    # Chunk if too long for one embed
    chunks, chunk = [], []
    for line in lines:
        chunk.append(line)
        if len("\n".join(chunk)) > 3800:
            chunks.append("\n".join(chunk[:-1]))
            chunk = [line]
    chunks.append("\n".join(chunk))
    for i, text in enumerate(chunks):
        title = f"◈ LINKED OPERATIVES ({len(links)} total)" if i == 0 else "◈ cont."
        if i == 0:
            await interaction.response.send_message(embed=make_embed(title, text, color=0xA855F7), ephemeral=True)
        else:
            await interaction.followup.send(embed=make_embed(title, text, color=0xA855F7), ephemeral=True)


@admin_group.command(name="announce", description="Send a message to all members with a specific role (DM or channel)")
@app_commands.describe(
    role="The role to target",
    message="The message to send",
    channel="Post in channel instead of DMs (leave blank to DM everyone)"
)
async def admin_announce(interaction: discord.Interaction, role: discord.Role, message: str, channel: discord.TextChannel = None):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)

    targets = [m for m in interaction.guild.members if role in m.roles and not m.bot]
    if not targets:
        await interaction.followup.send(
            embed=make_embed("▲ NO MEMBERS", f"No members found with role **{role.name}**.", color=0xE63946), ephemeral=True)
        return

    embed = discord.Embed(description=message, color=0xA855F7)
    embed.set_author(name=f"☽ {interaction.user.display_name} · ShadowSeekers Order")
    embed.set_footer(text="☽ SHADOWSEEKERS ORDER · DEEP IN THE DARK, I DON'T NEED THE LIGHT")

    if channel:
        await channel.send(content=role.mention, embed=embed)
        await interaction.followup.send(
            embed=make_embed("◉ ANNOUNCED",
                f"Posted in {channel.mention} pinging **{role.name}** ({len(targets)} members).", color=0x10B981), ephemeral=True)
    else:
        sent, failed = 0, 0
        for m in targets:
            try:
                await m.send(embed=embed)
                sent += 1
            except Exception:
                failed += 1
        note = f"\n⚠️ {failed} member(s) have DMs closed." if failed else ""
        await interaction.followup.send(
            embed=make_embed("◉ DMs SENT",
                f"Delivered to **{sent}/{len(targets)}** members with role **{role.name}**.{note}", color=0x10B981), ephemeral=True)


@admin_group.command(name="dm", description="Send a direct message to a specific operative from the bot")
@app_commands.describe(user="The operative to DM", message="The message to send")
async def admin_dm(interaction: discord.Interaction, user: discord.Member, message: str):
    if not is_admin(interaction):
        await interaction.response.send_message(embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    embed = discord.Embed(description=message, color=0xA855F7)
    embed.set_author(name=f"☽ {interaction.user.display_name} · ShadowSeekers Order")
    embed.set_footer(text="☽ SHADOWSEEKERS ORDER · DEEP IN THE DARK, I DON'T NEED THE LIGHT")
    try:
        await user.send(embed=embed)
        await interaction.response.send_message(
            embed=make_embed("◉ DM SENT", f"Message delivered to **{user.display_name}**.", color=0x10B981), ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message(
            embed=make_embed("▲ DM FAILED", f"**{user.display_name}** has DMs closed.", color=0xE63946), ephemeral=True)



@admin_group.command(name="addvctime", description="Manually add VC time to an operative (use to recover lost time)")
@app_commands.describe(
    user="The operative to credit",
    hours="Hours to add",
    minutes="Minutes to add"
)
async def admin_addvctime(interaction: discord.Interaction, user: discord.Member, hours: int = 0, minutes: int = 0):
    if not is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("\u25b2 CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return

    total_seconds = hours * 3600 + minutes * 60
    if total_seconds <= 0:
        await interaction.response.send_message(
            embed=make_embed("\u25b2 INVALID TIME", "Provide at least 1 minute or 1 hour to add.", color=0xE63946), ephemeral=True)
        return

    data = await load_data()
    uid  = str(user.id)

    if "vc_time" not in data:
        data["vc_time"] = {}

    old_secs = int(data["vc_time"].get(uid, 0))
    data["vc_time"][uid] = old_secs + total_seconds
    await save_data(data)

    old_h  = old_secs // 3600
    old_m  = (old_secs % 3600) // 60
    new_h  = data["vc_time"][uid] // 3600
    new_m  = (data["vc_time"][uid] % 3600) // 60

    await interaction.response.send_message(
        embed=make_embed(
            "\u25ce VC TIME CREDITED",
            f"**{user.display_name}**\n"
            f"Added: **{hours}h {minutes}m**\n"
            f"`{old_h}h {old_m}m` \u2192 **{new_h}h {new_m}m** total VC time",
            color=0x10B981
        )
    )
    if interaction.guild:
        asyncio.create_task(refresh_realtime_leaderboards(interaction.guild))


@bot.event
async def on_ready():
    print(f"[SHADOW BOT] Logged in as {bot.user} ({bot.user.id})")
    if MONGO_URI:
        print("[SHADOW BOT] MongoDB connected — data is persistent ✓")
    else:
        print("[SHADOW BOT] WARNING: MONGO_URI not set — using local file")

    setup_ai_missions(bot, tree)
    setup_shadow_ai(bot)

    # ── Shadow Brain — autonomous thinking loop ───────────────────
    try:
        from shadow_brain import start_shadow_brain
        await start_shadow_brain(bot, tree)
    except Exception as e:
        print(f"[SHADOW BOT] Shadow Brain init failed: {e}")

    # Set bot presence so it shows Online, not Completed
    await bot.change_presence(
        status=discord.Status.online,
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="the shadows ☽"
        )
    )

    try:
        # Clear guild-specific commands on all guilds first (removes duplicates from previous deploy)
        for guild in bot.guilds:
            tree.clear_commands(guild=guild)
            await tree.sync(guild=guild)
            print(f"[SHADOW BOT] Cleared guild commands: {guild.name}")
        # Then do the global sync
        synced = await tree.sync()
        print(f"[SHADOW BOT] Synced {len(synced)} slash commands globally")
    except Exception as e:
        print(f"[SHADOW BOT] Sync error: {e}")

    data = await load_data()
    loaded = await load_data()
    print(f"[SHADOW BOT] Loaded {len(loaded['members'])} members from MongoDB")

    # Ensure MongoDB TTL index for plan_cache (15 min expiry)
    await ensure_plan_ttl_index(get_db)

    # Seed shadow tokens for existing linked members who don't have any yet
    try:
        for uid, link in loaded.get("links", {}).items():
            if link.get("approved"):
                existing = await d1_get_tokens(uid)
                if existing is None:
                    await d1_set_tokens(uid, LINKED_BONUS)
                    print(f"[TOKENS] Seeded {LINKED_BONUS} tokens for existing member uid={uid}")
    except Exception as e:
        print(f"[TOKENS] Seeding error: {e}")

    # Load persisted leaderboard message IDs so we edit in-place after restarts
    await _load_lb_message_ids()

    daily_echo_task.start()
    session_ticker.start()
    ai_mission_task.start()
    phantom_alert_task.start()
    lb_tick_task.start()
    print(f"[SHADOW BOT] Daily task scheduled at {EOD_HOUR}:{EOD_MINUTE:02d} {TIMEZONE}")
    print(f"[SHADOW BOT] Session ticker started")
    print("[SHADOW BOT] AI Mission Generator started")
    print("[SHADOW BOT] Phantom Alert task started")
    print("[SHADOW BOT] Leaderboard pulse task started (45s tick)")

    # ── Seed _vc_join_times for anyone already in VC when the bot starts ──
    # This prevents losing VC time for members who were in VC before a restart.
    vc_seeded = 0
    for g in bot.guilds:
        for vc_channel in g.voice_channels:
            for member in vc_channel.members:
                if not member.bot:
                    uid = str(member.id)
                    if uid not in _vc_join_times:
                        _vc_join_times[uid] = time_module.time()
                        vc_seeded += 1
    if vc_seeded:
        print(f"[VC TRACKING] Seeded join times for {vc_seeded} member(s) already in VC at startup")

    # Seed realtime leaderboard messages on startup
    for g in bot.guilds:
        asyncio.create_task(refresh_realtime_leaderboards(g))
    print("[SHADOW BOT] Realtime leaderboards seeded")

if __name__ == "__main__":
    bot.run(TOKEN)


# ══════════════════════════════════════════════════════════════════
#  /shadowcard — Admin uploads a Shadow Card image
#  Card stored in MongoDB and displayed in shadowcard channel
#  Syncs shadowCardImage to Shadow Records via GAS
# ══════════════════════════════════════════════════════════════════

async def _save_shadowcard_db(shadow_id: str, image_url: str, uploader_id: str):
    db = get_db()
    if db is not None:
        await db["shadowcards"].update_one(
            {"_id": shadow_id},
            {"$set": {
                "shadow_id":   shadow_id,
                "image_url":   image_url,
                "uploaded_by": uploader_id,
                "uploaded_at": datetime.now(pytz.timezone(TIMEZONE)).isoformat(),
            }},
            upsert=True,
        )

async def _get_shadowcard_db(shadow_id: str):
    db = get_db()
    if db is not None:
        return await db["shadowcards"].find_one({"_id": shadow_id})
    return None

async def _push_shadowcard_to_gas(shadow_id: str, image_url: str):
    try:
        payload = json.dumps({"action": "updateShadowCard", "shadowId": shadow_id, "shadowCardImage": image_url})
        async with aiohttp.ClientSession() as sess:
            async with sess.post(GAS_URL, data=payload, headers={"Content-Type": "text/plain"},
                                 timeout=aiohttp.ClientTimeout(total=15)) as resp:
                print(f"[SHADOWCARD] GAS push status={resp.status} sid={shadow_id}")
    except Exception as e:
        print(f"[SHADOWCARD] GAS push error: {e}")


@tree.command(name="shadowcard", description="[HIGH CLEARANCE] Upload a Shadow Card image for an operative")
@app_commands.describe(
    user="The operative to assign this card to",
    image="Upload the Shadow Card image directly",
    image_url="Or paste a direct image URL instead of uploading",
)
async def shadowcard_cmd(
    interaction: discord.Interaction,
    user: discord.Member,
    image: discord.Attachment = None,
    image_url: str = None,
):
    if not is_admin(interaction):
        await interaction.response.send_message(
            embed=make_embed("▲ CLEARANCE DENIED", "High clearance required.", color=0xE63946), ephemeral=True)
        return
    if not image and not image_url:
        await interaction.response.send_message(
            embed=make_embed("▲ IMAGE REQUIRED", "Provide an image upload **or** a direct image URL.", color=0xE63946),
            ephemeral=True)
        return

    await interaction.response.defer()
    data      = await load_data()
    uid       = str(user.id)
    shadow_id = get_shadow_id(uid, data)
    if not shadow_id:
        await interaction.followup.send(
            embed=make_embed("▲ NOT LINKED", f"**{user.display_name}** has no linked Shadow ID.", color=0xE63946))
        return

    final_url = image.url if image else image_url.strip()

    # Save to MongoDB
    await _save_shadowcard_db(shadow_id, final_url, str(interaction.user.id))

    # Update member record for GAS/website
    for i, m in enumerate(data["members"]):
        if m["shadowId"] == shadow_id:
            data["members"][i]["shadowCardImage"] = final_url
            break
    await save_data(data)
    asyncio.create_task(_push_shadowcard_to_gas(shadow_id, final_url))

    member_obj = get_member(shadow_id, data)
    codename   = member_obj.get("codename", shadow_id) if member_obj else shadow_id

    card_embed = discord.Embed(
        title=f"🃏 SHADOW CARD — {codename}",
        description=f"**Shadow ID:** `{shadow_id}`\n**Operative:** {user.mention}\n\n*Now live on Shadow Records.*",
        color=0x7B2FBE,
    )
    card_embed.set_image(url=final_url)
    card_embed.set_footer(text=f"☽ SHADOWSEEKERS ORDER · Uploaded by {interaction.user.display_name}")

    sc_ch = discord.utils.get(interaction.guild.text_channels, name=SHADOWCARD_CHANNEL)
    if sc_ch and sc_ch.id != interaction.channel_id:
        await sc_ch.send(embed=card_embed)
        await interaction.followup.send(
            embed=make_embed("🃏 SHADOW CARD UPLOADED",
                f"`{shadow_id}` — **{codename}**\nPosted in {sc_ch.mention} and synced to Shadow Records.",
                color=0x10B981))
    else:
        await interaction.followup.send(embed=card_embed)


@tree.command(name="viewshadowcard", description="View the Shadow Card for an operative")
@app_commands.describe(user="The operative whose card you want to view (leave blank for your own)")
async def viewshadowcard_cmd(interaction: discord.Interaction, user: discord.Member = None):
    await interaction.response.defer()
    target    = user or interaction.user
    data      = await load_data()
    uid       = str(target.id)
    shadow_id = get_shadow_id(uid, data)
    if not shadow_id:
        await interaction.followup.send(
            embed=make_embed("▲ NOT LINKED", f"**{target.display_name}** has no linked Shadow ID.", color=0xE63946))
        return
    card = await _get_shadowcard_db(shadow_id)
    if not card:
        await interaction.followup.send(embed=make_embed(
            "◈ NO SHADOW CARD",
            f"No Shadow Card set yet for `{shadow_id}`.\nAdmins can upload one with `/shadowcard @{target.display_name}`.",
            color=0x6B6B9A))
        return
    member_obj = get_member(shadow_id, data)
    codename   = member_obj.get("codename", shadow_id) if member_obj else shadow_id
    embed = discord.Embed(title=f"🃏 SHADOW CARD — {codename}", description=f"**Shadow ID:** `{shadow_id}`", color=0x7B2FBE)
    embed.set_image(url=card["image_url"])
    embed.set_footer(text="☽ SHADOWSEEKERS ORDER · Shadow Records")
    await interaction.followup.send(embed=embed)

