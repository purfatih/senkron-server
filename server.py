"""Senkron - aracı sunucu.

Lider hedef adını yollar, sunucu aynı odadaki üyelere iletir.
Render / Railway / Fly.io gibi servislerde çalışır (PORT ortam değişkenini kullanır).
"""
import hmac
import json
import os
import time

from aiohttp import web, WSMsgType

MAX_ROOM_SIZE = 50
MAX_MSG_BYTES = 32768      # hedef görüntüsü (küçük PNG, base64) de taşınır
MAX_IMG_CHARS = 24000
# Ortam değişkeni SENKRON_TOKEN tanımlıysa, bağlanan herkesin aynı şifreyi girmesi gerekir.
TOKEN = os.environ.get("SENKRON_TOKEN", "")

# oda adı -> {ws: {"role": ..., "name": ...}}
rooms = {}


async def health(request):
    return web.Response(text="Senkron sunucusu calisiyor")


async def send_members(room):
    members = rooms.get(room, {})
    payload = json.dumps({"type": "members", "n": len(members)})
    for ws in list(members):
        try:
            await ws.send_str(payload)
        except Exception:
            pass


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=MAX_MSG_BYTES)
    await ws.prepare(request)
    room = None
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except ValueError:
                continue
            if not isinstance(data, dict):
                continue

            if room is None:
                if data.get("type") != "join":
                    continue
                if TOKEN and not hmac.compare_digest(
                        str(data.get("token", "")).encode(), TOKEN.encode()):
                    await ws.send_str(json.dumps(
                        {"type": "error", "msg": "Sifre yanlis", "fatal": True}))
                    await ws.close()
                    break
                r = str(data.get("room", "")).strip()
                if len(r) < 4 or len(r) > 64:
                    await ws.send_str(json.dumps(
                        {"type": "error", "msg": "Oda kodu en az 4 karakter olmali"}))
                    continue
                members = rooms.setdefault(r, {})
                if len(members) >= MAX_ROOM_SIZE:
                    await ws.send_str(json.dumps(
                        {"type": "error", "msg": "Oda dolu"}))
                    continue
                room = r
                members[ws] = {
                    "role": "leader" if data.get("role") == "leader" else "member",
                    "name": str(data.get("name", ""))[:32],
                }
                await send_members(room)
                continue

            if data.get("type") in ("target", "name"):
                info = rooms[room].get(ws)
                if not info or info["role"] != "leader":
                    continue
                target = str(data.get("target", "")).strip()[:32]
                img = data.get("img")
                if not isinstance(img, str) or len(img) > MAX_IMG_CHARS:
                    img = None
                if not target and not img:
                    continue
                msg_out = {
                    "type": data["type"],
                    "target": target,
                    "from": info["name"],
                    "ts": time.time(),
                }
                if img:
                    msg_out["img"] = img
                payload = json.dumps(msg_out)
                for other in list(rooms[room]):
                    if other is not ws:
                        try:
                            await other.send_str(payload)
                        except Exception:
                            pass
    finally:
        if room is not None:
            rooms.get(room, {}).pop(ws, None)
            if not rooms.get(room):
                rooms.pop(room, None)
            else:
                await send_members(room)
    return ws


def make_app():
    app = web.Application()
    app.add_routes([web.get("/", health), web.get("/ws", ws_handler)])
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=int(os.environ.get("PORT", 8765)))
