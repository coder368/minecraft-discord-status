import asyncio
import json
import logging
import os
import time
from datetime import datetime, time as clock_time, timezone
from enum import Enum
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import discord
from aiohttp import web
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
PROVIDER_TIMEZONE = ZoneInfo(os.getenv("PROVIDER_TIMEZONE", "Asia/Dhaka"))
RECOVERY_AFTER_SECONDS = int(os.getenv("RECOVERY_AFTER_SECONDS", "600"))
RECOVERY_COOLDOWN_SECONDS = int(os.getenv("RECOVERY_COOLDOWN_SECONDS", "1800"))
DISCORD_EDIT_INTERVAL_SECONDS = int(os.getenv("DISCORD_EDIT_INTERVAL_SECONDS", "30"))
DISCORD_MAX_BACKOFF_SECONDS = int(os.getenv("DISCORD_MAX_BACKOFF_SECONDS", "300"))
START_HOUR = int(os.getenv("DAILY_START_HOUR", "10"))
START_MINUTE = int(os.getenv("DAILY_START_MINUTE", "0"))
LINK_REPAIR_DELAY_SECONDS = int(os.getenv("LINK_REPAIR_DELAY_SECONDS", "20"))
START_MARKER_FILE = os.getenv("START_MARKER_FILE", "automatic-start-date.txt")
STATUS_MESSAGE_ID_FILE = os.getenv("STATUS_MESSAGE_ID_FILE", "status-message-id.txt")
PROVIDER_RETRY_ATTEMPTS = max(1, int(os.getenv("PROVIDER_RETRY_ATTEMPTS", "3")))
PROVIDER_RETRY_BASE_SECONDS = max(0.5, float(os.getenv("PROVIDER_RETRY_BASE_SECONDS", "2")))
PROVIDER_RETRY_MAX_SECONDS = max(
    PROVIDER_RETRY_BASE_SECONDS,
    float(os.getenv("PROVIDER_RETRY_MAX_SECONDS", "30")),
)

# The bot checks telemetry every 15 seconds. A heartbeat older than this is offline.
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL_SECONDS", "15"))
STALE_AFTER = int(os.getenv("STALE_AFTER_SECONDS", "45"))
HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("PORT", os.getenv("HTTP_PORT", "8080")))
OFFLINE_RAM_MAX_MB = float(os.getenv("OFFLINE_RAM_MAX_MB", "6144"))

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


class ServerState(str, Enum):
    OFFLINE = "OFFLINE"
    STARTING = "STARTING"
    ONLINE = "ONLINE"
    RESTARTING = "RESTARTING"


intents = discord.Intents.none()
intents.guilds = True


class StatusBot(discord.Client):
    def __init__(self) -> None:
        super().__init__(intents=intents)
        self.channel: discord.TextChannel | None = None
        self.status_message: discord.Message | None = None
        self.status_message_id: int | None = STATUS_MESSAGE_ID

        # Telemetry is the source of truth for the watchdog and state machine.
        self.telemetry: dict[str, Any] = {}
        self.last_seen_monotonic: float | None = None
        self.last_seen_at: datetime | None = None
        self.telemetry_lock = asyncio.Lock()
        self.server_state = ServerState.OFFLINE
        self.last_player_count: int | None = None

        # The HTTP server and provider client are independent from Discord edits.
        self.http_runner: web.AppRunner | None = None
        self.api_session: aiohttp.ClientSession | None = None
        self.recovery_lock = asyncio.Lock()
        self.last_recovery_monotonic: float | None = None
        self.last_recovery_result: str = "not attempted"
        self.last_provider_failure_log_monotonic: float | None = None
        self.provider_retry_until = 0.0

        self.start_attempted_date: str | None = self.load_start_marker()
        self.daily_start_retry_until = 0.0

        # One serialized, coalescing Discord display pipeline.
        self.status_wakeup = asyncio.Event()
        self.status_event_pending = True
        self.last_discord_edit_monotonic: float | None = None
        self.last_rendered_state: ServerState | None = None
        self.last_render_signature: tuple[Any, ...] | None = None
        self.discord_edit_lock = asyncio.Lock()
        self.discord_rate_limited_until = 0.0
        self.discord_backoff_until = 0.0
        self.discord_backoff_seconds = 1.0

        self.status_updater_task: asyncio.Task[None] | None = None
        self.automatic_actions_task: asyncio.Task[None] | None = None

    async def setup_hook(self) -> None:
        self.api_session = self._new_provider_session()
        self.status_updater_task = asyncio.create_task(
            self.status_update_loop(), name="discord-status-updater"
        )
        self.automatic_actions_task = asyncio.create_task(
            self.automatic_actions_loop(), name="minecraft-watchdog"
        )
        await self.start_http_server()

    def _new_provider_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "x-my-mc-auth": MY_MC_API_KEY,
            },
            timeout=aiohttp.ClientTimeout(total=20),
        )

    async def on_ready(self) -> None:
        log.info("Logged in as %s", self.user)
        channel = self.get_channel(CHANNEL_ID)
        if channel is None:
            channel = await self.fetch_channel(CHANNEL_ID)
        if not isinstance(channel, discord.TextChannel):
            raise RuntimeError("DISCORD_CHANNEL_ID must be a text channel ID")
        self.channel = channel
        await self.ensure_status_message()
        self.request_status_update()

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

        required = (
            "cpu",
            "ram_used_mb",
            "ram_max_mb",
            "tps",
            "mspt",
            "players",
            "disk_free_gb",
            "uptime",
        )
        missing = [key for key in required if key not in data]
        if missing:
            return web.json_response({"error": "missing fields", "fields": missing}, status=400)

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

        now = time.monotonic()
        async with self.telemetry_lock:
            previous_players = self.last_player_count
            self.telemetry = clean
            self.last_seen_monotonic = now
            self.last_seen_at = datetime.now(timezone.utc)
            self.last_player_count = clean["players"]

        # Telemetry drives state transitions; Discord only displays them.
        if self.server_state != ServerState.ONLINE:
            self.set_server_state(ServerState.ONLINE)
        if previous_players is not None and previous_players != clean["players"]:
            log.info("Player count changed: %s -> %s", previous_players, clean["players"])
            self.request_status_update()

        return web.json_response({"ok": True})

    def set_server_state(self, new_state: ServerState) -> None:
        if self.server_state == new_state:
            return
        old_state = self.server_state
        self.server_state = new_state
        log.info("Minecraft state changed: %s -> %s", old_state.value, new_state.value)
        self.request_status_update()

    def request_status_update(self) -> None:
        # Coalesce all state/player/periodic requests into one newest render.
        self.status_event_pending = True
        self.status_wakeup.set()

    async def ensure_status_message(self) -> None:
        if self.channel is None:
            return

        if self.status_message_id is not None:
            try:
                self.status_message = await self.channel.fetch_message(self.status_message_id)
                return
            except discord.NotFound:
                log.warning("Saved status message was not found; creating one replacement message")
            except discord.HTTPException as error:
                log.warning("Could not fetch saved status message (HTTP %s); will retry later", error.status)
                return

        try:
            self.status_message = await self.channel.send("Starting Minecraft status monitor...")
            self.status_message_id = self.status_message.id
            try:
                with open(STATUS_MESSAGE_ID_FILE, "w", encoding="utf-8") as file:
                    file.write(str(self.status_message_id))
            except OSError:
                log.warning("Could not persist status message ID; set STATUS_MESSAGE_ID manually")
            log.info("Created status message ID: %s", self.status_message.id)
        except discord.HTTPException as error:
            log.warning("Could not create status message (HTTP %s); status display will retry", error.status)

    async def status_update_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                now = time.monotonic()
                periodic_due = (
                    self.last_discord_edit_monotonic is None
                    or now - self.last_discord_edit_monotonic >= DISCORD_EDIT_INTERVAL_SECONDS
                )
                cooldown_due = max(self.discord_rate_limited_until, self.discord_backoff_until)
                if self.status_message is None:
                    wait_seconds = float(UPDATE_INTERVAL)
                else:
                    wait_seconds = 0.0 if self.status_event_pending or periodic_due else float(UPDATE_INTERVAL)
                if cooldown_due > now:
                    wait_seconds = max(wait_seconds, cooldown_due - now)

                self.status_wakeup.clear()
                try:
                    await asyncio.wait_for(self.status_wakeup.wait(), timeout=wait_seconds)
                except asyncio.TimeoutError:
                    pass
                await self.update_message()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Discord status updater loop failed; watchdog remains independent")
                await asyncio.sleep(1)

    async def automatic_actions_loop(self) -> None:
        """Watchdog/provider automation, completely independent of Discord edits."""
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await self.run_automatic_actions()
            except asyncio.CancelledError:
                raise
            except Exception:
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

    def _provider_error_text(self, text: str) -> str:
        # Keep diagnostics useful without ever echoing configured secrets.
        safe = str(text).replace(MY_MC_API_KEY, "[redacted]")
        safe = safe.replace(TELEMETRY_SECRET, "[redacted]")
        return safe[:200]

    async def provider_request(
        self,
        method: str,
        endpoint: str,
        *,
        attempts: int = PROVIDER_RETRY_ATTEMPTS,
    ) -> tuple[bool, dict[str, Any] | None, str]:
        if not MY_MC_API_KEY:
            return False, None, "MY_MC_API_KEY is not configured"
        if self.api_session is None or self.api_session.closed:
            self.api_session = self._new_provider_session()

        retryable_statuses = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
        last_error = "provider request failed"
        for attempt in range(1, max(1, attempts) + 1):
            if self.provider_retry_until > time.monotonic():
                await asyncio.sleep(self.provider_retry_until - time.monotonic())
            try:
                async with self.api_session.request(
                    method, f"{MY_MC_BASE_URL}/{endpoint.lstrip('/')}"
                ) as response:
                    raw = await response.text()
                    try:
                        data = await response.json(content_type=None)
                    except Exception:
                        data = None

                    if 200 <= response.status < 300 and isinstance(data, dict) and data.get("success") is True:
                        self.provider_retry_until = 0.0
                        return True, data, str(data.get("message", "OK"))[:200]

                    if isinstance(data, dict):
                        message = str(data.get("message", "API rejected the request"))
                    else:
                        message = raw[:200] or "empty provider response"
                    last_error = f"HTTP {response.status}: {self._provider_error_text(message)}"

                    if response.status not in retryable_statuses or attempt >= attempts:
                        log.warning("My-MC.Link %s %s failed: %s", method, endpoint, last_error)
                        return False, data if isinstance(data, dict) else None, last_error
                    retry_after = response.headers.get("Retry-After")
                    try:
                        delay = float(retry_after) if retry_after else min(
                            PROVIDER_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                            PROVIDER_RETRY_MAX_SECONDS,
                        )
                    except (TypeError, ValueError):
                        delay = min(
                            PROVIDER_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                            PROVIDER_RETRY_MAX_SECONDS,
                        )
            except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                last_error = self._provider_error_text(error)
                if attempt >= attempts:
                    log.warning("My-MC.Link %s %s failed after retries: %s", method, endpoint, last_error)
                    return False, None, last_error
                delay = min(
                    PROVIDER_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                    PROVIDER_RETRY_MAX_SECONDS,
                )

            delay = max(0.5, min(delay, PROVIDER_RETRY_MAX_SECONDS))
            self.provider_retry_until = time.monotonic() + delay
            log.warning(
                "My-MC.Link %s %s temporary failure; retry %d/%d in %.1fs (%s)",
                method,
                endpoint,
                attempt,
                attempts - 1,
                delay,
                last_error,
            )
            await asyncio.sleep(delay)

        return False, None, last_error

    async def run_automatic_actions(self) -> None:
        """Run guarded provider actions without repeated calls every 15 seconds."""
        now = datetime.now(PROVIDER_TIMEZONE)
        date_key = now.date().isoformat()
        start_time = clock_time(START_HOUR, START_MINUTE)
        current_time = now.timetz().replace(tzinfo=None)

        # Asia/Dhaka 10:00 by default. Once the scheduled time has passed, a
        # process that woke slightly late still performs one attempt that day.
        in_start_window = current_time >= start_time
        if in_start_window and self.start_attempted_date != date_key and time.monotonic() >= self.daily_start_retry_until:
            async with self.recovery_lock:
                if self.start_attempted_date == date_key:
                    return
                status_ok, status_data, error = await self.provider_request("GET", f"/status/{SERVER_ID}")
                status_block = status_data.get("status", {}) if isinstance(status_data, dict) else {}
                online = status_block.get("isOnline") if isinstance(status_block, dict) else None
                if status_ok and online is True:
                    self.save_start_marker(date_key)
                    log.info("Automatic daily start skipped: provider says server is online")
                    await asyncio.sleep(LINK_REPAIR_DELAY_SECONDS)
                    link_success, _, link_message = await self.provider_request("GET", "/my-link")
                    log.info("Daily online /my-link: success=%s message=%s", link_success, link_message)
                elif status_ok and online is False:
                    self.set_server_state(ServerState.STARTING)
                    success, _, message = await self.provider_request("POST", "/start")
                    self.save_start_marker(date_key)
                    self.last_recovery_result = f"daily start: {message}"
                    log.info("Automatic daily start: success=%s message=%s", success, message)
                    await asyncio.sleep(LINK_REPAIR_DELAY_SECONDS)
                    link_success, _, link_message = await self.provider_request("GET", "/my-link")
                    log.info("Post-start /my-link: success=%s message=%s", link_success, link_message)
                else:
                    self.daily_start_retry_until = time.monotonic() + 60.0
                    log.warning("Could not verify server before daily start: %s", error)

        async with self.telemetry_lock:
            last_seen = self.last_seen_monotonic
        if last_seen is None:
            return

        stale_for = time.monotonic() - last_seen
        if stale_for >= STALE_AFTER and self.server_state == ServerState.ONLINE:
            self.set_server_state(ServerState.OFFLINE)
        if stale_for < RECOVERY_AFTER_SECONDS:
            return
        if (
            self.last_recovery_monotonic is not None
            and time.monotonic() - self.last_recovery_monotonic < RECOVERY_COOLDOWN_SECONDS
        ):
            return

        async with self.recovery_lock:
            async with self.telemetry_lock:
                latest_seen = self.last_seen_monotonic
            if latest_seen is not None and time.monotonic() - latest_seen < RECOVERY_AFTER_SECONDS:
                return
            self.last_recovery_monotonic = time.monotonic()
            self.set_server_state(ServerState.RESTARTING)
            success, _, message = await self.provider_request("GET", "/my-link")
            self.last_recovery_result = f"network recovery: {message}"
            log.warning(
                "Telemetry stale for %.0f seconds; /my-link success=%s: %s",
                stale_for,
                success,
                message,
            )

    def _snapshot_state(self) -> tuple[dict[str, Any], float | None, datetime | None, ServerState]:
        return dict(self.telemetry), self.last_seen_monotonic, self.last_seen_at, self.server_state

    async def update_message(self) -> None:
        if self.status_message is None:
            return

        now = time.monotonic()
        if now < self.discord_rate_limited_until or now < self.discord_backoff_until:
            return

        async with self.telemetry_lock:
            stats, last_seen, last_seen_at, state = self._snapshot_state()

        is_online = (
            state == ServerState.ONLINE
            and last_seen is not None
            and now - last_seen <= STALE_AFTER
            and bool(stats)
        )
        state_event = self.status_event_pending or self.last_rendered_state != state
        periodic_due = (
            self.last_discord_edit_monotonic is None
            or now - self.last_discord_edit_monotonic >= DISCORD_EDIT_INTERVAL_SECONDS
        )
        if not state_event and not periodic_due:
            return

        if is_online:
            color = discord.Color.green()
            title = "Minecraft Server Status — Online"
            updated = f"<t:{int(last_seen_at.timestamp())}:R>" if last_seen_at else "just now"
            description = "Live telemetry received from the Fabric server."
            fields = [
                ("CPU", cpu_display(stats["cpu"]), True),
                ("RAM", ram_display(stats["ram_used_mb"], stats["ram_max_mb"]), True),
                ("TPS", f"{stats['tps']:.1f}", True),
                ("MSPT", f"{stats['mspt']:.1f} ms", True),
                ("Players", str(stats["players"]), True),
                ("Disk free", f"{stats['disk_free_gb']:.1f} GB", True),
                ("Uptime", stats["uptime"], True),
                ("Last update", updated, False),
            ]
        else:
            color = discord.Color.orange() if state in {ServerState.STARTING, ServerState.RESTARTING} else discord.Color.red()
            title = f"Minecraft Server Status — {state.value.title()}"
            description = (
                "Minecraft is starting; waiting for telemetry."
                if state == ServerState.STARTING
                else "Minecraft is restarting; waiting for telemetry."
                if state == ServerState.RESTARTING
                else "No live telemetry heartbeat is arriving. The Minecraft server may be offline."
            )
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
        embed.set_footer(text=f"Live telemetry; periodic refresh every {DISCORD_EDIT_INTERVAL_SECONDS}s")

        # Do not treat the relative Last-update timestamp as a meaningful
        # change. It changes on every packet, while the displayed metrics below
        # are the values users actually need refreshed.
        render_signature = (
            is_online,
            state.value,
            round(stats.get("cpu", 0.0), 1) if is_online else 0.0,
            round(stats.get("ram_used_mb", 0.0), 0) if is_online else 0.0,
            round(stats.get("ram_max_mb", OFFLINE_RAM_MAX_MB), 0) if is_online else OFFLINE_RAM_MAX_MB,
            round(stats.get("tps", 0.0), 1) if is_online else 0.0,
            round(stats.get("mspt", 0.0), 1) if is_online else 0.0,
            stats.get("players", 0) if is_online else 0,
            round(stats.get("disk_free_gb", 0.0), 1) if is_online else round(disk_free or 0.0, 1),
            stats.get("uptime", "0m") if is_online else "0m",
        )
        if not state_event and self.last_render_signature == render_signature:
            return

        if self.discord_edit_lock.locked():
            return
        async with self.discord_edit_lock:
            now = time.monotonic()
            if now < self.discord_rate_limited_until or now < self.discord_backoff_until:
                return
            try:
                await self.status_message.edit(content=None, embed=embed)
                self.last_discord_edit_monotonic = time.monotonic()
                self.last_rendered_state = state
                self.last_render_signature = render_signature
                self.status_event_pending = False
                self.discord_backoff_seconds = 1.0
                self.discord_backoff_until = 0.0
            except discord.NotFound:
                self.status_message = None
                self.status_event_pending = True
                try:
                    await self.ensure_status_message()
                except Exception:
                    log.exception("Status message was deleted and could not be recreated")
            except discord.HTTPException as error:
                if error.status == 429:
                    retry_after = getattr(error, "retry_after", None)
                    if retry_after is None:
                        response = getattr(error, "response", None)
                        headers = getattr(response, "headers", {}) if response else {}
                        retry_after = headers.get("Retry-After") if headers else None
                    try:
                        retry_after = float(retry_after) if retry_after is not None else 60.0
                    except (TypeError, ValueError):
                        retry_after = 60.0
                    retry_after = max(1.0, min(retry_after, DISCORD_MAX_BACKOFF_SECONDS))
                    self.discord_rate_limited_until = time.monotonic() + retry_after
                    log.warning(
                        "Discord rate limit (429); pausing edits for %.1fs and coalescing newest state",
                        retry_after,
                    )
                else:
                    delay = min(self.discord_backoff_seconds, DISCORD_MAX_BACKOFF_SECONDS)
                    self.discord_backoff_until = time.monotonic() + delay
                    self.discord_backoff_seconds = min(delay * 2.0, DISCORD_MAX_BACKOFF_SECONDS)
                    log.warning("Discord HTTP error %s; edit backed off for %.1fs", error.status, delay)
            except (discord.DiscordServerError, asyncio.TimeoutError) as error:
                delay = min(self.discord_backoff_seconds, DISCORD_MAX_BACKOFF_SECONDS)
                self.discord_backoff_until = time.monotonic() + delay
                self.discord_backoff_seconds = min(delay * 2.0, DISCORD_MAX_BACKOFF_SECONDS)
                log.warning("Transient Discord error; edit backed off for %.1fs: %s", delay, error)
            except Exception:
                log.exception("Unexpected status-message edit failure; watchdog continues")

    async def close(self) -> None:
        if self.status_updater_task is not None:
            self.status_updater_task.cancel()
        if self.automatic_actions_task is not None:
            self.automatic_actions_task.cancel()
        if self.http_runner is not None:
            await self.http_runner.cleanup()
        if self.api_session is not None and not self.api_session.closed:
            await self.api_session.close()
        await super().close()


bot = StatusBot()
bot.run(DISCORD_TOKEN)
