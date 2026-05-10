"""
╔══════════════════════════════════════════════════════╗
║         SHADOW BOT · MAIN ENTRYPOINT                 ║
║   Runs Discord bot + Void FastAPI server together    ║
║   Start with: python main.py                         ║
╚══════════════════════════════════════════════════════╝
"""

import asyncio
import os
import threading

from dotenv import load_dotenv
load_dotenv()

import uvicorn

# ── Import the FastAPI app (void server) ──────────────────────────
from void_server import app as fastapi_app

# ── Import the Discord bot (without triggering bot.run) ──────────
import bot as shadow_bot

PORT = int(os.getenv("PORT", 8080))


def run_fastapi():
    """Run the Void FastAPI server in a background thread."""
    uvicorn.run(
        fastapi_app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
    )


if __name__ == "__main__":
    # Start FastAPI in a daemon thread (dies when main process dies)
    fastapi_thread = threading.Thread(target=run_fastapi, daemon=True)
    fastapi_thread.start()
    print(f"[MAIN] Void Server starting on port {PORT}")

    # Run the Discord bot in the main thread (blocks until bot stops)
    print("[MAIN] Starting Shadow Bot...")
    shadow_bot.bot.run(shadow_bot.TOKEN)
