"""
╔══════════════════════════════════════════════════════╗
║         SHADOW BOT · AI MISSION GENERATOR            ║
║   Groq-powered personalized daily mission engine     ║
║   Fixed: reads real todo/session data correctly      ║
╚══════════════════════════════════════════════════════╝
"""

import os
import sys
import json
import asyncio
import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks
from datetime import datetime, time, timedelta
import pytz

# ── CONFIG ────────────────────────────────────────────────────────
GROQ_API_KEY      = os.getenv("GROQ_API_KEY")
GROQ_API_URL      = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL        = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GENERAL_CHANNEL   = os.getenv("GENERAL_CHANNEL", "general")
# Set MISSION_CHANNEL to post AI missions in a dedicated channel (e.g. "ai-missions")
# Leave unset to fall back to GENERAL_CHANNEL
MISSION_CHANNEL   = os.getenv("MISSION_CHANNEL", "")
# Set MISSION_THREAD to post inside a specific thread name within MISSION_CHANNEL (or GENERAL_CHANNEL)
# The bot will find an existing thread with that name or create one if it doesn't exist
# e.g. MISSION_THREAD=daily-missions
MISSION_THREAD    = os.getenv("MISSION_THREAD", "")
MISSION_GEN_HOUR  = int(os.getenv("MISSION_GEN_HOUR", "6"))
MISSION_GEN_MIN   = int(os.getenv("MISSION_GEN_MIN", "0"))
TIMEZONE          = os.getenv("TIMEZONE", "Asia/Kolkata")

_pending_missions: dict[str, list[str]] = {}
# Opt-in state is managed by shadow_brain — import helpers at runtime to avoid circular deps


async def _resolve_mission_target(guild: discord.Guild) -> discord.abc.Messageable | None:
    """
    Returns the channel or thread where missions should be posted.
    Priority:
      1. If MISSION_THREAD is set — find or create a thread with that name
         inside MISSION_CHANNEL (or GENERAL_CHANNEL as fallback).
      2. If MISSION_CHANNEL is set — post directly in that channel.
      3. Fall back to GENERAL_CHANNEL.
    """
    ch_name = MISSION_CHANNEL if MISSION_CHANNEL else GENERAL_CHANNEL
    channel = discord.utils.get(guild.text_channels, name=ch_name)
    if not channel:
        channel = discord.utils.get(guild.text_channels, name=GENERAL_CHANNEL)
    if not channel:
        return None

    if not MISSION_THREAD:
        return channel

    # Look for an existing active thread with that name
    thread_name = MISSION_THREAD
    for thread in channel.threads:
        if thread.name.lower() == thread_name.lower():
            return thread

    # Also check archived threads
    try:
        async for thread in channel.archived_threads():
            if thread.name.lower() == thread_name.lower():
                await thread.unarchive()
                return thread
    except Exception:
        pass

    # Create a new thread if none found
    try:
        thread = await channel.create_thread(
            name=thread_name,
            type=discord.ChannelType.public_thread,
            reason="Shadow Bot — AI Mission thread",
        )
        return thread
    except Exception as e:
        print(f"[AI MISSIONS] Could not create thread '{thread_name}': {e}")
        return channel  # fall back to channel itself


# ── GROQ CALL ─────────────────────────────────────────────────────
async def call_groq(prompt: str, system: str = None):
    if not GROQ_API_KEY:
        print("[AI MISSIONS] ERROR: GROQ_API_KEY not set.")
        return None

    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    payload = {"model": GROQ_MODEL, "messages": messages, "temperature": 0.7, "max_tokens": 500}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(GROQ_API_URL, headers=headers, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    print(f"[AI MISSIONS] Groq error {resp.status}: {(await resp.text())[:300]}")
                    return None
                data = await resp.json()
                return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[AI MISSIONS] Groq request failed: {e}")
        return None


# ── DAYS UNTIL EXAM ───────────────────────────────────────────────
def _days_until(date_str: str) -> int:
    try:
        tz = pytz.timezone(TIMEZONE)
        now_date = datetime.now(tz).date()
        exam_dt = datetime.strptime(date_str, "%m/%d/%Y").date()
        return (exam_dt - now_date).days
    except Exception:
        return 999


# ── RICH CONTEXT BUILDER ──────────────────────────────────────────
def build_rich_context(uid: str, data: dict) -> dict:
    """
    Build complete, accurate context from real bot data.
    KEY FIX: todos are stored with key "task" not "text".
    Also pulls session_history, exams, and plan.
    """
    ctx = {
        "codename": "Operative",
        "tier": "Initiate",
        "echo_count": 0,
        "todo_history": [],
        "session_history": [],
        "exams": [],
        "plan": None,
        "completion_rate": 0.0,
        "avg_session_hrs": 0.0,
        "streak": 0,
    }

    link = data.get("links", {}).get(uid, {})
    if not link.get("approved"):
        return ctx

    shadow_id = link["shadow_id"]
    member = next((m for m in data.get("members", []) if m["shadowId"] == shadow_id), None)
    if not member:
        return ctx

    ctx["codename"] = member.get("codename", shadow_id)
    ctx["echo_count"] = int(member.get("echoCount", 0))

    for tier, min_e in [("Voidborn", 5000), ("Wraith", 3000), ("Phantom", 1500), ("Seeker", 500)]:
        if ctx["echo_count"] >= min_e:
            ctx["tier"] = tier
            break

    # ── Todo history — FIX: use "task" key, not "text" ────────────
    entry = data.get("todos", {}).get(uid)
    if isinstance(entry, dict):
        dates_map = entry.get("dates", {})
        tz = pytz.timezone(TIMEZONE)
        today = datetime.now(tz).date()

        def parse_date(d_str):
            try:
                parts = d_str.split("/")
                return datetime(today.year, int(parts[0]), int(parts[1])).date()
            except Exception:
                return None

        recent = sorted(
            [(d, parse_date(d)) for d in dates_map if parse_date(d)],
            key=lambda x: x[1], reverse=True
        )[:7]

        total, done_count = 0, 0
        active_days = set()

        for date_str, date_obj in recent:
            for t in dates_map.get(date_str, []):
                if not isinstance(t, dict):
                    continue
                # FIXED: check "task" first, then "text" as fallback
                task_text = t.get("task") or t.get("text", "")
                if not task_text:
                    continue
                done = t.get("done", False)
                ctx["todo_history"].append({"date": date_str, "task": task_text, "done": done})
                total += 1
                if done:
                    done_count += 1
                    active_days.add(date_str)

        ctx["completion_rate"] = round(done_count / total, 2) if total > 0 else 0.0

        # Streak
        streak = 0
        for i in range(7):
            d = today - timedelta(days=i)
            if d.strftime("%m/%d") in active_days or d.strftime("%-m/%-d") in active_days:
                streak += 1
            else:
                break
        ctx["streak"] = streak

    # ── Session history ───────────────────────────────────────────
    for s in data.get("session_history", {}).get(uid, [])[-20:]:
        dur_hrs = round(s.get("duration_seconds", 0) / 3600, 2)
        ctx["session_history"].append({
            "date": s.get("date", "?"),
            "task": s.get("task", ""),
            "duration_hrs": dur_hrs,
            "type": s.get("session_type", "study"),
        })

    if ctx["session_history"]:
        ctx["avg_session_hrs"] = round(
            sum(s["duration_hrs"] for s in ctx["session_history"]) / len(ctx["session_history"]), 2
        )

    # ── Exams ─────────────────────────────────────────────────────
    for e in sorted(data.get("exams", {}).get(uid, []),
                    key=lambda x: _days_until(x.get("date", "12/31/9999"))):
        days = _days_until(e.get("date", "12/31/9999"))
        if days >= 0:
            ctx["exams"].append({"name": e.get("name", "Exam"), "date": e.get("date", "?"), "days_left": days})

    # ── Plan (may be in data dict if cached) ──────────────────────
    ctx["plan"] = data.get("plans", {}).get(uid)

    return ctx


# ── PROMPT BUILDER ────────────────────────────────────────────────
def build_mission_prompt(ctx: dict) -> str:
    rate = int(ctx["completion_rate"] * 100)
    avg = ctx["avg_session_hrs"]

    todo_block = "  No recorded todos yet."
    if ctx["todo_history"]:
        lines = []
        for t in ctx["todo_history"][-12:]:
            mark = "✓" if t["done"] else "✗"
            lines.append(f"  [{t['date']}] {mark} {t['task']}")
        todo_block = "\n".join(lines)

    sess_block = "  No sessions yet."
    if ctx["session_history"]:
        lines = []
        for s in ctx["session_history"][-8:]:
            lines.append(f"  [{s['date']}] {s['type']} · {s['duration_hrs']}h · \"{s['task']}\"")
        sess_block = "\n".join(lines)

    exam_block = "  None."
    if ctx["exams"]:
        lines = []
        for e in ctx["exams"][:3]:
            urgency = "🔥 URGENT" if e["days_left"] <= 7 else ("⚠️ SOON" if e["days_left"] <= 30 else "📅")
            lines.append(f"  {urgency} {e['name']} — {e['days_left']} days ({e['date']})")
        exam_block = "\n".join(lines)

    plan_block = ""
    if ctx["plan"]:
        p = ctx["plan"]
        subs = ", ".join(p.get("subjects", [])) or "—"
        plan_block = f"""
OPERATIVE STUDY PLAN:
- Goal: {p.get("goal", "—")}
- Subjects: {subs}
- Daily target: {p.get("hours_per_day", "?")} hours
- Timeline: {p.get("timeline", "?")}
"""

    difficulty_note = "give 3 shorter, achievable missions" if rate < 50 else "give 4-5 challenging, specific missions"
    session_note = "shorter focused blocks (under 1h)" if avg < 1 else f"sessions around {avg}h"

    return f"""Generate personalized daily missions for this operative.

OPERATIVE: {ctx["codename"]} | RANK: {ctx["tier"]} | {ctx["echo_count"]:,} echoes
STATS: {rate}% completion rate (last 7 days) | avg session {avg}h | streak {ctx["streak"]} days

RECENT TODOS (what they actually study):
{todo_block}

RECENT STUDY SESSIONS:
{sess_block}

UPCOMING EXAMS:
{exam_block}
{plan_block}
RULES — follow these exactly:
1. READ the todo and session history above. Generate missions IN THOSE EXACT SUBJECTS. If they study calculus, give calculus missions. If they code React, give React missions. Never invent subjects.
2. EXAM URGENCY: exam within 7 days → 2+ missions must be direct exam prep. Within 30 days → 1+ mission.
3. SPECIFIC: every mission states a topic, chapter, problem count, or duration. Never vague.
4. DIFFICULTY: completion {rate}% → {difficulty_note}. Session length → target {session_note}.
5. OUTPUT: mission text only, one per line, no bullets, no numbers, no preamble.

If absolutely no history exists, generate 3 general missions: a focused Pomodoro, a planning task, and a review session.""".strip()


# ── PARSE MISSIONS ────────────────────────────────────────────────
def parse_missions(raw_text: str) -> list[str]:
    missions = []
    for line in raw_text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        for prefix in ["- ", "• ", "* "]:
            if line.startswith(prefix):
                line = line[len(prefix):]
        if len(line) > 2 and line[0].isdigit() and line[1] in (".", ")"):
            line = line[2:].strip()
        if line and len(line) > 5:
            missions.append(line)
    return missions[:5]


# ── GENERATE FOR ONE USER ─────────────────────────────────────────
async def generate_missions_for_user(uid: str, data: dict, get_db_fn=None):
    link = data.get("links", {}).get(uid, {})
    if not link or not link.get("approved"):
        return None

    ctx = build_rich_context(uid, data)

    # Load plan from Mongo/GAS if not already in ctx
    if ctx["plan"] is None and get_db_fn:
        try:
            from shadow_ai import get_plan
            plan = await get_plan(uid, get_db_fn)
            if plan:
                ctx["plan"] = plan
        except Exception as e:
            print(f"[AI MISSIONS] Plan load error: {e}")

    system = (
        "You are the Shadow Mission AI — the intelligence engine of the ShadowSeekers Order. "
        "You generate laser-focused daily missions based on each operative's real study data. "
        "You never invent subjects. Every mission is grounded in what they actually study. "
        "You are precise, elite, and atmospheric."
    )

    missions = parse_missions(await call_groq(build_mission_prompt(ctx), system=system) or "")
    if missions:
        _pending_missions[uid] = missions
    return missions or None


# ── EMBED BUILDER ─────────────────────────────────────────────────
def make_mission_embed(codename: str, tier_name: str, missions: list[str], ctx: dict = None) -> discord.Embed:
    colors = {"Initiate": 0x6B6B9A, "Seeker": 0x7B2FBE, "Phantom": 0xA855F7, "Wraith": 0xE63946, "Voidborn": 0xF0A500}
    color = colors.get(tier_name, 0xA855F7)
    lines = "\n".join(f"◈ {m}" for m in missions)

    extra = ""
    if ctx:
        if ctx.get("exams"):
            e = ctx["exams"][0]
            if e["days_left"] <= 7:
                extra += f"\n\n🔥 **{e['name']}** is in **{e['days_left']} days** — missions adjusted."
            elif e["days_left"] <= 30:
                extra += f"\n\n📅 **{e['name']}** in {e['days_left']} days — prep included."
        rate = int(ctx.get("completion_rate", 0) * 100)
        streak = ctx.get("streak", 0)
        extra += f"\n*{rate}% completion last 7 days"
        if streak > 1:
            extra += f" · {streak}-day streak 🔥"
        extra += "*"

    embed = discord.Embed(
        title="🧠 AI MISSION BRIEF — TODAY'S OPERATIONS",
        description=(
            f"**Operative:** {codename} · **Tier:** {tier_name}\n\n"
            f"{lines}{extra}\n\n"
            f"*Use `/acceptmissions` to deploy these to your dossier.*"
        ),
        color=color,
    )
    embed.set_footer(text="☽ SHADOWSEEKERS ORDER · AI MISSION ENGINE · Missions built from your real data")
    return embed


# ── SCHEDULED TASK ────────────────────────────────────────────────
_bot_ref  = None
_tree_ref = None


@tasks.loop(time=time(hour=MISSION_GEN_HOUR, minute=MISSION_GEN_MIN, tzinfo=pytz.timezone(TIMEZONE)))
async def ai_mission_task():
    if _bot_ref is None:
        return
    try:
        main_mod = sys.modules.get("__main__")
        if not main_mod or not hasattr(main_mod, "load_data"):
            return
        load_data = main_mod.load_data
        get_db = getattr(main_mod, "get_db", None)
    except Exception as e:
        print(f"[AI MISSIONS] Import error: {e}")
        return

    data = await load_data()
    for guild in _bot_ref.guilds:
        mission_target = await _resolve_mission_target(guild)
        if not mission_target:
            continue
        approved_uids = [uid for uid, link in data["links"].items() if link.get("approved")]
        print(f"[AI MISSIONS] Generating for {len(approved_uids)} operatives...")

        for uid in approved_uids:
            # Only broadcast to users who explicitly opted IN
            try:
                from shadow_brain import is_mission_opted_in
                if not is_mission_opted_in(uid):
                    continue
            except ImportError:
                pass  # if brain not loaded, skip all (safe default)
            try:
                await asyncio.sleep(1.5)
                ctx = build_rich_context(uid, data)
                missions = await generate_missions_for_user(uid, data, get_db)
                if not missions:
                    continue
                discord_member = guild.get_member(int(uid))
                mention = discord_member.mention if discord_member else f"`{ctx['codename']}`"
                embed = make_mission_embed(ctx["codename"], ctx["tier"], missions, ctx=ctx)
                await mission_target.send(content=f"{mention} — your missions for today have arrived.", embed=embed)
            except Exception as e:
                print(f"[AI MISSIONS] Error for uid={uid}: {e}")

        print(f"[AI MISSIONS] Broadcast complete for {guild.name}")


# ── COMMANDS ──────────────────────────────────────────────────────
def register_commands(tree: app_commands.CommandTree):

    @tree.command(name="acceptmissions", description="Deploy today's AI missions to your dossier ('all' or '1,3,5')")
    @app_commands.describe(numbers="Which missions to accept: 'all' or comma-separated e.g. '1,3,5'")
    async def acceptmissions(interaction: discord.Interaction, numbers: str = "all"):
        await interaction.response.defer()
        uid = str(interaction.user.id)
        missions = _pending_missions.get(uid)
        if not missions:
            await interaction.followup.send(embed=discord.Embed(
                title="◈ NO MISSIONS PENDING",
                description="Use `/generatemissions` to generate now, or wait for 6 AM broadcast.",
                color=0x6B6B9A,
            ).set_footer(text="☽ SHADOWSEEKERS ORDER · AI MISSION ENGINE"))
            return

        if numbers.strip().lower() == "all":
            selected = missions
        else:
            try:
                indices = [int(x.strip()) for x in numbers.split(",")]
                invalid = [n for n in indices if n < 1 or n > len(missions)]
                if invalid:
                    await interaction.followup.send(embed=discord.Embed(
                        title="▲ INVALID NUMBERS",
                        description=f"Mission(s) {', '.join(str(n) for n in invalid)} don't exist. You have {len(missions)}.",
                        color=0xE63946,
                    ))
                    return
                selected = [missions[i - 1] for i in indices]
            except ValueError:
                await interaction.followup.send(embed=discord.Embed(
                    title="▲ INVALID INPUT", description="Use `all` or comma-separated numbers.", color=0xE63946,
                ))
                return

        try:
            main_mod = sys.modules.get("__main__")
            load_data          = main_mod.load_data
            save_data          = main_mod.save_data
            set_todos_for_date = main_mod.set_todos_for_date
            get_todos_for_date = main_mod.get_todos_for_date
            get_shadow_id      = main_mod.get_shadow_id
            today_str          = main_mod.today_str
            make_embed         = main_mod.make_embed
        except Exception as e:
            await interaction.followup.send(embed=discord.Embed(
                title="▲ INTEGRATION ERROR", description=f"`{e}`", color=0xE63946,
            ))
            return

        data = await load_data()
        shadow_id = get_shadow_id(uid, data)
        if not shadow_id:
            await interaction.followup.send(
                embed=make_embed("▲ NOT LINKED", "Link your Shadow ID first.", color=0xE63946))
            return

        today = today_str()
        existing = get_todos_for_date(uid, today, data)
        new_todos = [
            {"task": m, "done": False, "ops": [], "priority": "p2", "source": "ai"}
            for m in selected
        ]
        set_todos_for_date(uid, today, existing + new_todos, data)
        await save_data(data)

        if len(selected) == len(missions):
            del _pending_missions[uid]

        lines = "\n".join(f"◈ {m}" for m in selected)
        await interaction.followup.send(embed=make_embed(
            "✅ MISSIONS DEPLOYED",
            f"**{len(selected)} mission{'s' if len(selected) != 1 else ''}** added 🤖:\n\n{lines}\n\nView: `/todo list`",
            color=0x10B981,
        ))

    @tree.command(name="generatemissions", description="Generate personalized AI missions from your real study data")
    async def generatemissions(interaction: discord.Interaction):
        uid = str(interaction.user.id)
        await interaction.response.defer(thinking=True)
        try:
            main_mod = sys.modules.get("__main__")
            load_data     = main_mod.load_data
            get_shadow_id = main_mod.get_shadow_id
            make_embed    = main_mod.make_embed
            get_db        = getattr(main_mod, "get_db", None)
        except Exception as e:
            await interaction.followup.send(embed=discord.Embed(
                title="▲ INTEGRATION ERROR", description=f"`{e}`", color=0xE63946))
            return

        data = await load_data()
        if not get_shadow_id(uid, data):
            await interaction.followup.send(embed=make_embed(
                "▲ NOT LINKED", "Link your Shadow ID first.", color=0xE63946))
            return

        ctx = build_rich_context(uid, data)

        if ctx["plan"] is None and get_db:
            try:
                from shadow_ai import get_plan
                plan = await get_plan(uid, get_db)
                if plan:
                    ctx["plan"] = plan
            except Exception:
                pass

        missions = await generate_missions_for_user(uid, data, get_db)

        if not missions:
            has_data = bool(ctx["todo_history"] or ctx["session_history"])
            desc = (
                "The AI needs your real study data to personalize missions.\n\n"
                "◈ Add todos with `/todo add` or @mention the bot\n"
                "◈ Run a session with `/study` or `/pomodoro`\n"
                "◈ Set your plan with `/plan new`\n\n"
                "A few days of data is all it needs."
                if not has_data else
                "The AI engine timed out. Try again shortly."
            )
            await interaction.followup.send(embed=make_embed("▲ NO DATA YET" if not has_data else "▲ FAILED", desc, color=0xF0A500))
            return

        await interaction.followup.send(embed=make_mission_embed(ctx["codename"], ctx["tier"], missions, ctx=ctx))

    @tree.command(name="mymissions", description="View your pending AI missions before accepting them")
    async def mymissions(interaction: discord.Interaction):
        uid = str(interaction.user.id)
        missions = _pending_missions.get(uid)
        if not missions:
            await interaction.response.send_message(embed=discord.Embed(
                title="◈ NO PENDING MISSIONS",
                description="Use `/generatemissions` to generate now.",
                color=0x6B6B9A,
            ).set_footer(text="☽ SHADOWSEEKERS ORDER · AI MISSION ENGINE"))
            return
        lines = "\n".join(f"`{i+1}.` {m}" for i, m in enumerate(missions))
        await interaction.response.send_message(embed=discord.Embed(
            title="🧠 YOUR PENDING MISSIONS",
            description=f"{lines}\n\n*Use `/acceptmissions` or `/acceptmissions 1,3` to pick.*",
            color=0xA855F7,
        ).set_footer(text="☽ SHADOWSEEKERS ORDER · AI MISSION ENGINE"))

    # /startmissions and /stopmissions are now handled by shadow_brain.py
    # which persists opt-in state to D1 (survives restarts)


# ── SETUP ─────────────────────────────────────────────────────────
def setup_ai_missions(bot, tree: app_commands.CommandTree):
    global _bot_ref, _tree_ref
    _bot_ref  = bot
    _tree_ref = tree
    register_commands(tree)
    print("[AI MISSIONS] AI Mission Generator registered ✓")
    ch_display = MISSION_CHANNEL if MISSION_CHANNEL else f"{GENERAL_CHANNEL} (fallback)"
    thread_display = f" → thread: {MISSION_THREAD}" if MISSION_THREAD else ""
    print(f"[AI MISSIONS] Daily broadcast at {MISSION_GEN_HOUR}:{MISSION_GEN_MIN:02d} {TIMEZONE} → #{ch_display}{thread_display}")
