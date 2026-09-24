import discord
import asyncio
import aiohttp
import os
import json
from datetime import datetime
from discord.ext import tasks
from aiohttp import web

# --- Configuration ---
# Ensure these match your Render Environment Variables
TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
MESSAGE_ID = int(os.getenv("DISCORD_MESSAGE_ID", "0"))
# MY_MC_BASE_URL should be your Cloudflare Worker URL
MY_MC_BASE_URL = os.getenv("MY_MC_BASE_URL", "").rstrip("/")
SERVER_NAME = os.getenv("SERVER_NAME", "Minecraft")

# Global State to prevent Discord Rate Limits (429)
last_embed_state = None

intents = discord.Intents.default()
client = discord.Client(intents=intents)

# --- API Functions ---
async def fetch_server_status(session):
    """Fetches the status via the Cloudflare Worker."""
    url = f"{MY_MC_BASE_URL}/status/{SERVER_NAME}"
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status == 200:
                return await resp.json()
            elif resp.status in (404, 400):
                # FIX: 404 (No Cache) or 400 (Not Running) means the server is safely OFFLINE.
                return {"online": False, "players": {"online": 0, "max": 0}, "motd": "Server is offline"}
            else:
                print(f"[Warning] API returned {resp.status} for status check.")
                return {"online": False}
    except Exception as e:
        print(f"[Error] Failed to fetch status: {e}")
        return {"online": False}

async def trigger_server_start(session):
    """Triggers the start command if the server is offline."""
    url = f"{MY_MC_BASE_URL}/start/{SERVER_NAME}"
    print(f"Attempting to start server via {url}...")
    try:
        # Some APIs use POST for starting, some use GET. Adjust method if needed.
        async with session.post(url, timeout=15) as resp:
            if resp.status in (200, 204):
                print("[Success] Start command sent to My-MC.Link.")
                return True
            else:
                text = await resp.text()
                print(f"[Failed] Start command returned HTTP {resp.status}: {text}")
                return False
    except Exception as e:
        print(f"[Error] Failed to trigger start: {e}")
        return False


# --- Bot Background Tasks ---
@tasks.loop(seconds=60)
async def update_status_loop():
    global last_embed_state
    await client.wait_until_ready()

    if not CHANNEL_ID or not MESSAGE_ID:
        return

    channel = client.get_channel(CHANNEL_ID)
    if not channel:
        print("[Error] Could not find the Discord channel.")
        return

    try:
        message = await channel.fetch_message(MESSAGE_ID)
    except Exception as e:
        print(f"[Error] Could not fetch Discord message: {e}")
        return

    async with aiohttp.ClientSession() as session:
        # 1. Fetch current status
        status_data = await fetch_server_status(session)
        is_online = status_data.get("online", False)

        # 2. Auto-Start Logic (Trigger if Offline)
        if not is_online:
            print("Server detected as OFFLINE. Triggering auto-start request...")
            await trigger_server_start(session)

        # 3. Build the Discord Embed
        if is_online:
            players = status_data.get("players", {})
            online_players = players.get("online", 0)
            max_players = players.get("max", 0)
            motd = status_data.get("motd", "A Minecraft Server")
            
            embed = discord.Embed(title=f"{SERVER_NAME} Server Status", color=discord.Color.green())
            embed.add_field(name="Status", value="🟢 Online", inline=False)
            embed.add_field(name="Players", value=f"{online_players}/{max_players}", inline=False)
            embed.add_field(name="Message of the Day", value=motd, inline=False)
        else:
            embed = discord.Embed(title=f"{SERVER_NAME} Server Status", color=discord.Color.red())
            embed.add_field(name="Status", value="🔴 Offline / Starting...", inline=False)
            embed.add_field(name="Info", value="A start request has been sent to the host.", inline=False)

        embed.timestamp = discord.utils.utcnow()

        # 4. Anti-Rate Limit Check (Only edit if data changes)
        embed_dict = embed.to_dict()
        if 'timestamp' in embed_dict:
            del embed_dict['timestamp']  # Ignore timestamp for comparison
        
        current_embed_state = json.dumps(embed_dict, sort_keys=True)
        
        if current_embed_state != last_embed_state:
            try:
                await message.edit(embed=embed)
                last_embed_state = current_embed_state
                state_text = "ONLINE" if is_online else "OFFLINE"
                print(f"[Update] Discord message updated. Server is {state_text}.")
            except discord.errors.HTTPException as e:
                if e.status == 429:
                    print("[Rate Limit] Discord 429 Too Many Requests. Skipping edit.")
                else:
                    print(f"[Error] Failed to edit message: {e}")

@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    if not update_status_loop.is_running():
        update_status_loop.start()

# --- Render Web Server (Keeps the app alive) ---
async def health_check(request):
    return web.Response(text="OK", status=200)

async def telemetry_endpoint(request):
    return web.Response(text="Telemetry received", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/health', health_check)
    app.router.add_post('/telemetry', telemetry_endpoint)
    
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"Web server listening on port {port} for Render health checks.")

async def main():
    # Start web server and Discord bot concurrently
    await start_web_server()
    await client.start(TOKEN)

if __name__ == "__main__":
    asyncio.run(main())
    

#
