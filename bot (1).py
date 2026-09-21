import asyncio
import json
import logging
import os
import time
from datetime import datetime, time as clock_time, timezone
from typing import Any
from zoneinfo import ZoneInfo

import discord
import aiohttp
from aiohttp import web
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("minecraft-status")

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
CHANNEL_ID = int(os.environ["DISCORD_CHANNEL_ID"])
TELEMETRY_SECRET = os.environ["TELEMETRY_SECRET"]
MY_MC_API_KEY = os.getenv("MY_MC_API_KEY", "")
SERVER_ID = os.getenv("SERVER_ID", "Minecraft")
MY_MC_BASE_URL = os.getenv("MY_MC_BASE_URL", "https://api.my-mc.link").rstrip("/")
PROVIDER_TIMEZONE = ZoneInfo(os.getenv("PROVIDER_TIMEZONE", "America/New_York"))
RECOVERY_AFTER_SECONDS = int(os.getenv("RECOVERY_AFTER_SECONDS", "600"))
RECOVERY_COOLDOWN_SECONDS = int(os.getenv("RECOVERY_COOLDOWN_SECONDS", "1800"))
DISCORD_EDIT_INTERVAL_SECONDS = int(os.getenv("DISCORD_EDIT_INTERVAL_SECONDS", "30"))
DISCORD_MAX_BACKOFF_SECONDS = int(os.getenv("DISCORD_MAX_BACKOFF_SECONDS", "300"))
START_HOUR = int(os.getenv("DAILY_START_HOUR", "10"))
START_MINUTE = int(os.getenv("DAILY_START_MINUTE", "29"))
LINK_REPAIR_DELAY_SECONDS = int(os.getenv("LINK_REPAIR_DELAY_SECONDS", "20"))
START_MARKER_FILE = os.getenv("START_MARKER_FILE", "automatic-start-date.txt")
STATUS_MESSAGE_ID_FILE = os.getenv("STATUS_MESSAGE_ID_FILE", "status-message-id.txt")

# The bot checks every 15 seconds. A heartbeat older than this is considered offline.
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL_SECONDS", "15"))
STALE_AFTER = int(os.getenv("STALE_AFTER_SECONDS", "45"))
HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("PORT", os.getenv("HTTP_PORT", "8080")))
OFFLINE_RAM_MAX_MB = float(os.getenv("OFFLINE_RAM_MAX_MB", "6144"))

# Optional: put the ID of an existing status message here. If omitted, the bot creates one.
status_message_id_text = os.getenv("STATUS_MESSAGE_ID", "").strip()
if status_message_id_text:
    STATUS_MESSAGE_ID = int(status_message_id_text)
else:
    try:
        with open(STATUS_MESSAGE_ID_FILE, "r", encoding="utf-8") as file:
            saved_status_id = file.read().strip()
        STATUS_MESSAGE_ID = int(saved_status_id) if saved_status_id else None
    except (FileNotFoundError, ValueError, OSError):
        STATUS_MESSAGE_ID = None


def progress_bar(value: float, maximum: float = 100.0, length: int = 10) -> str:
    """Return a compact Discord-friendly bar, clamped to a safe range."""
    if maximum <= 0:
        return "░" * length
    ratio = max(0.0, min(1.0, value / maximum))
    filled = round(ratio * length)
    return "█" * filled + "░" * (length - filled)


def cpu_display(cpu: float) -> str:
    return f"{progress_bar(cpu)}  **{cpu:.1f}%**"


def ram_display(used_mb: float, max_mb: float) -> str:
    percentage = (used_mb / max_mb * 100.0) if max_mb > 0 else 0.0
    return (
        f"{progress_bar(percentage)}  **{used_mb:.0f} / {max_mb:.0f} MB**\n"
        f"{percentage:.1f}% used"
    )

intents = discord.Intents.none()
intents.guilds = True

class StatusBot(discord.Client):
    def __init__(self) -> None:
        super().__init__(intents=intents)
        self.channel: discord.TextChannel | None = None
        self.status_message: discord.Message | None = None
        self.status_message_id: int | None = STATUS_MESSAGE_ID
        self.telemetry: dict[str, Any] = {}
        self.last_seen_monotonic: float | None = None
        self.last_seen_at: datetime | None = None
        self.telemetry_lock = asyncio.Lock()
        self.http_runner: web.AppRunner | None = None
        self.api_session: aiohttp.ClientSession | None = None
        self.recovery_lock = asyncio.Lock()
        self.last_recovery_monotonic: float | None = None
        self.last_recovery_result: str = "not attempted"
        self.start_attempted_date: str | None = self.load_start_marker()
        self.last_discord_edit_monotonic: float | None = None
        self.last_rendered_online: bool | None = None
        # Exactly one lock and one code path are allowed to edit the status message.
        self.discord_edit_lock = asyncio.Lock()
        self.discord_rate_limited_until = 0.0
        self.discord_backoff_until = 0.0
        self.discord_backoff_seconds = 1.0
        self.automatic_actions_task: asyncio.Task[None] | None = None

    async def setup_hook(self) -> None:
        self.api_session = aiohttp.ClientSession(
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "x-my-mc-auth": MY_MC_API_KEY,
            },
            timeout=aiohttp.ClientTimeout(total=20),
        )
        self.update_status_message.start()
        self.automatic_actions_task = asyncio.create_task(
            self.automatic_actions_loop(), name="provider-automatic-actions"
        )
        await self.start_http_server()

    async def on_ready(self) -> None:
        log.info("Logged in as %s", self.user)
        channel = self.get_channel(CHANNEL_ID)
        if channel is None:
            channel = await self.fetch_channel(CHANNEL_ID)
        if not isinstance(channel, discord.TextChannel):
            raise RuntimeError("DISCORD_CHANNEL_ID must be a text channel ID")
        self.channel = channel
        await self.ensure_status_message()
        await self.update_message()

    async def start_http_server(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self.health)
        app.router.add_post("/telemetry", self.receive_telemetry)
        self.http_runner = web.AppRunner(app)
        await self.http_runner.setup()
        site = web.TCPSite(self.http_runner, HTTP_HOST, HTTP_PORT)
        await site.start()
        log.info("Telemetry endpoint listening on %s:%s", HTTP_HOST, HTTP_PORT)

    async def health(self, request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "discord_ready": self.is_ready()})

    async def receive_telemetry(self, request: web.Request) -> web.Response:
        if request.headers.get("X-Server-Secret") != TELEMETRY_SECRET:
            return web.json_response({"error": "unauthorized"}, status=401)

        try:
            data = await request.json()
        except (json.JSONDecodeError, ValueError):
            return web.json_response({"error": "body must be JSON"}, status=400)

        required = ("cpu", "ram_used_mb", "ram_max_mb", "tps", "mspt", "players", "disk_free_gb", "uptime")
        missing = [key for key in required if key not in data]
        if missing:
            return web.json_response({"error": "missing fields", "fields": missing}, status=400)

        # Keep only expected values and coerce numeric fields. This prevents arbitrary
        # data from being inserted into the Discord message.
        try:
            clean = {
                "cpu": float(data["cpu"]),
                "ram_used_mb": float(data["ram_used_mb"]),
                "ram_max_mb": float(data["ram_max_mb"]),
                "tps": float(data["tps"]),
                "mspt": float(data["mspt"]),
                "players": int(data["players"]),
                "disk_free_gb": float(data["disk_free_gb"]),
                "uptime": str(data["uptime"])[:64],
            }
        except (TypeError, ValueError):
            return web.json_response({"error": "invalid telemetry types"}, status=400)

        async with self.telemetry_lock:
            self.telemetry = clean
            self.last_seen_monotonic = time.monotonic()
            self.last_seen_at = datetime.now(timezone.utc)

        return web.json_response({"ok": True})

    async def ensure_status_message(self) -> None:
        if self.channel is None:
            return

        if self.status_message_id is not None:
            try:
                self.status_message = await self.channel.fetch_message(self.status_message_id)
                return
            except discord.NotFound:
                log.warning("Saved status message was not found; creating one replacement message")

        self.status_message = await self.channel.send("Starting Minecraft status monitor...")
        self.status_message_id = self.status_message.id
        try:
            with open(STATUS_MESSAGE_ID_FILE, "w", encoding="utf-8") as file:
                file.write(str(self.status_message_id))
        except OSError:
            log.warning("Could not persist status message ID; set STATUS_MESSAGE_ID manually")
        log.info("Created status message ID: %s", self.status_message.id)

    @tasks.loop(seconds=UPDATE_INTERVAL)
    async def update_status_message(self) -> None:
        if not self.is_ready():
            return
        await self.update_message()

    @update_status_message.before_loop
    async def before_update_status_message(self) -> None:
        await self.wait_until_ready()

    async def automatic_actions_loop(self) -> None:
        """Provider automation runs independently of Discord API delays."""
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await self.run_automatic_actions()
            except Exception:
                # Provider/API failures must never stop this loop or the HTTP server.
                log.exception("Automatic provider action failed unexpectedly")
            await asyncio.sleep(UPDATE_INTERVAL)

    def load_start_marker(self) -> str | None:
        try:
            with open(START_MARKER_FILE, "r", encoding="utf-8") as file:
                value = file.read().strip()
                return value or None
        except FileNotFoundError:
            return None
        except OSError as error:
            log.warning("Could not read automatic start marker: %s", error)
            return None

    def save_start_marker(self, date_key: str) -> None:
        try:
            with open(START_MARKER_FILE, "w", encoding="utf-8") as file:
                file.write(date_key)
            self.start_attempted_date = date_key
        except OSError as error:
            log.warning("Could not save automatic start marker: %s", error)

    def in_protected_window(self, now: datetime) -> bool:
        """Provider shutdown window: 10:00 through 10:28:59 Eastern time."""
        current = now.timetz().replace(tzinfo=None)
        return clock_time(10, 0) <= current < clock_time(START_HOUR, START_MINUTE)

    async def provider_request(self, method: str, endpoint: str) -> tuple[bool, dict[str, Any] | None, str]:
        if not MY_MC_API_KEY:
            return False, None, "MY_MC_API_KEY is not configured"
        if self.api_session is None or self.api_session.closed:
            self.api_session = aiohttp.ClientSession(
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "x-my-mc-auth": MY_MC_API_KEY,
                },
                timeout=aiohttp.ClientTimeout(total=20),
            )
        try:
            async with self.api_session.request(
                method, f"{MY_MC_BASE_URL}/{endpoint.lstrip('/')}"
            ) as response:
                raw = await response.text()
                try:
                    data = await response.json(content_type=None)
                except Exception:
                    data = None
                if not 200 <= response.status < 300:
                    return False, data if isinstance(data, dict) else None, f"HTTP {response.status}: {raw[:200]}"
                if not isinstance(data, dict) or data.get("success") is not True:
                    message = data.get("message", "API rejected the request") if isinstance(data, dict) else raw
                    return False, data if isinstance(data, dict) else None, str(message)
                return True, data, str(data.get("message", "OK"))
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            return False, None, str(error)

    async def run_automatic_actions(self) -> None:
        """Run guarded provider actions without repeated calls every 15 seconds."""
        now = datetime.now(PROVIDER_TIMEZONE)
        if self.in_protected_window(now):
            return

        # The provider's daily shutdown is followed by one start attempt at 10:29.
        # The six-minute window tolerates a polling tick that lands just after 10:29.
        date_key = now.date().isoformat()
        if clock_time(START_HOUR, START_MINUTE) <= now.timetz().replace(tzinfo=None) < clock_time(START_HOUR, START_MINUTE + 6):
            if self.start_attempted_date != date_key:
                async with self.recovery_lock:
                    if self.start_attempted_date == date_key:
                        return
                    status_ok, status_data, error = await self.provider_request("GET", f"/status/{SERVER_ID}")
                    status_block = status_data.get("status", {}) if isinstance(status_data, dict) else {}
                    online = status_block.get("isOnline") if isinstance(status_block, dict) else None
                    if status_ok and online is True:
                        self.save_start_marker(date_key)
                        log.info("Automatic daily start skipped: provider says server is online")
                        # Even when the container is already online, refresh the
                        # network link because the provider does not start it reliably.
                        await asyncio.sleep(LINK_REPAIR_DELAY_SECONDS)
                        await self.provider_request("GET", "/my-link")
                        return
                    elif status_ok and online is False:
                        success, _, message = await self.provider_request("POST", "/start")
                        self.save_start_marker(date_key)
                        self.last_recovery_result = f"daily start: {message}"
                        log.info("Automatic daily start: success=%s message=%s", success, message)
                        # Starting the container does not automatically start the
                        # network Docker, so repair it after the container has time
                        # to boot instead of calling /my-link immediately.
                        await asyncio.sleep(LINK_REPAIR_DELAY_SECONDS)
                        link_success, _, link_message = await self.provider_request("GET", "/my-link")
                        log.info("Post-start /my-link: success=%s message=%s", link_success, link_message)
                        return
                    else:
                        log.warning("Could not verify server before daily start: %s", error)

        # Do not attempt recovery until the telemetry stream has been absent for 10 minutes.
        if self.last_seen_monotonic is None:
            return
        stale_for = time.monotonic() - self.last_seen_monotonic
        if stale_for < RECOVERY_AFTER_SECONDS:
            return
        if (
            self.last_recovery_monotonic is not None
            and time.monotonic() - self.last_recovery_monotonic < RECOVERY_COOLDOWN_SECONDS
        ):
            return

        async with self.recovery_lock:
            # Re-check after waiting for the lock so two ticks cannot race.
            if self.last_seen_monotonic is not None and time.monotonic() - self.last_seen_monotonic < RECOVERY_AFTER_SECONDS:
                return
            self.last_recovery_monotonic = time.monotonic()
            success, _, message = await self.provider_request("GET", "/my-link")
            self.last_recovery_result = f"network recovery: {message}"
            log.warning("Telemetry stale for %.0f seconds; /my-link success=%s: %s", stale_for, success, message)

    async def update_message(self) -> None:
        if self.status_message is None:
            return

        # A 429 is a server-directed cooldown. Do not retry inside the window.
        now_monotonic = time.monotonic()
        if now_monotonic < self.discord_rate_limited_until:
            return
        if now_monotonic < self.discord_backoff_until:
            return

        async with self.telemetry_lock:
            stats = dict(self.telemetry)
            last_seen = self.last_seen_monotonic
            last_seen_at = self.last_seen_at

        is_online = (
            last_seen is not None
            and time.monotonic() - last_seen <= STALE_AFTER
            and bool(stats)
        )

        # Telemetry may arrive every 15 seconds, but editing a Discord message
        # every 30 seconds is enough for a live display and is gentler on free
        # hosting and Discord rate limits. State transitions are always immediate.
        if (
            self.last_discord_edit_monotonic is not None
            and self.last_rendered_online == is_online
            and time.monotonic() - self.last_discord_edit_monotonic < DISCORD_EDIT_INTERVAL_SECONDS
        ):
            return

        if is_online:
            color = discord.Color.green()
            title = "Minecraft Server Status — Online"
            updated = (
                f"<t:{int(last_seen_at.timestamp())}:R>"
                if last_seen_at else "just now"
            )
            description = "Live telemetry received from the Fabric server."
            fields = [
                ("CPU", cpu_display(stats["cpu"]), True),
                ("RAM", ram_display(stats["ram_used_mb"], stats["ram_max_mb"]), True),
                ("TPS", f"{stats['tps']:.1f}", True),
                ("MSPT", f"{stats['mspt']:.1f} ms", True),
                ("Players", str(stats['players']), True),
                ("Disk free", f"{stats['disk_free_gb']:.1f} GB", True),
                ("Uptime", stats['uptime'], True),
                ("Last update", updated, False),
            ]
        else:
            color = discord.Color.red()
            title = "Minecraft Server Status — Offline"
            description = (
                "No live telemetry heartbeat is arriving. The Minecraft server may be off, "
                "or its network Docker may need to be restarted with `/my-mc-link`."
            )
            # Keep the last known disk value because disk capacity can still be useful
            # while the server is offline. All live server metrics are deliberately zero.
            disk_free = stats.get("disk_free_gb")
            disk_text = f"{disk_free:.1f} GB" if disk_free is not None else "unknown"
            fields = [
                ("CPU", cpu_display(0.0), True),
                ("RAM", ram_display(0.0, OFFLINE_RAM_MAX_MB), True),
                ("TPS", "0.0", True),
                ("MSPT", "0 ms", True),
                ("Players", "0", True),
                ("Disk free", disk_text, True),
                ("Uptime", "0m", True),
                ("Last update", "just now", False),
            ]

        embed = discord.Embed(title=title, description=description, color=color)
        for name, value, inline in fields:
            embed.add_field(name=name, value=value, inline=inline)
        embed.set_footer(text=f"Updated every {UPDATE_INTERVAL} seconds")

        # This is the only status-message edit path. The lock coalesces any
        # overlapping trigger and ensures that two edits never run together.
        if self.discord_edit_lock.locked():
            return
        async with self.discord_edit_lock:
            now_monotonic = time.monotonic()
            if now_monotonic < self.discord_rate_limited_until:
                return
            if now_monotonic < self.discord_backoff_until:
                return
            try:
                await self.status_message.edit(content=None, embed=embed)
                self.last_discord_edit_monotonic = time.monotonic()
                self.last_rendered_online = is_online
                self.discord_backoff_seconds = 1.0
                self.discord_backoff_until = 0.0
            except discord.NotFound:
                # The persistent message was deleted externally. Recreate it
                # only for that deletion, never as a rate-limit workaround.
                self.status_message = None
                try:
                    await self.ensure_status_message()
                except Exception:
                    log.exception("Status message was deleted and could not be recreated")
            except discord.HTTPException as error:
                if error.status == 429:
                    retry_after = getattr(error, "retry_after", None)
                    if retry_after is None:
                        retry_after = 60.0
                    retry_after = max(1.0, min(float(retry_after), DISCORD_MAX_BACKOFF_SECONDS))
                    self.discord_rate_limited_until = time.monotonic() + retry_after
                    log.warning(
                        "Discord rate limit (429); pausing status edits for %.1f seconds; newest state will be rendered afterward",
                        retry_after,
                    )
                else:
                    delay = min(self.discord_backoff_seconds, DISCORD_MAX_BACKOFF_SECONDS)
                    self.discord_backoff_until = time.monotonic() + delay
                    self.discord_backoff_seconds = min(delay * 2.0, DISCORD_MAX_BACKOFF_SECONDS)
                    log.warning(
                        "Discord HTTP error %s; status edit backed off for %.1f seconds",
                        error.status,
                        delay,
                    )
            except (discord.DiscordServerError, asyncio.TimeoutError) as error:
                delay = min(self.discord_backoff_seconds, DISCORD_MAX_BACKOFF_SECONDS)
                self.discord_backoff_until = time.monotonic() + delay
                self.discord_backoff_seconds = min(delay * 2.0, DISCORD_MAX_BACKOFF_SECONDS)
                log.warning("Transient Discord error; status edit backed off for %.1f seconds: %s", delay, error)
            except Exception:
                # Never let an edit failure kill the bot or telemetry endpoint.
                log.exception("Unexpected status-message edit failure")

    async def close(self) -> None:
        self.update_status_message.cancel()
        if self.automatic_actions_task is not None:
            self.automatic_actions_task.cancel()
        if self.http_runner is not None:
            await self.http_runner.cleanup()
        if self.api_session is not None and not self.api_session.closed:
            await self.api_session.close()
        await super().close()


bot = StatusBot()
bot.run(DISCORD_TOKEN)
