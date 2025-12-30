from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from urllib.parse import urlparse, parse_qs
import uuid
import requests
import re
import socketio
import asyncio
from socketio import ASGIApp
import yt_dlp

app = FastAPI()

# ===== WebSocket setup with python-socketio =====
sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')

# ===== In-memory rooms =====
rooms = {}
# rooms[room_id] = {
#   "queue": [ {video_id, title} ],
#   "current": None
# }

# ===== WebSocket client tracking =====
room_clients = {}  # room_id -> set of sid

# ===== Utils =====
def extract_video_id(url: str):
    parsed = urlparse(url)
    if parsed.hostname in ["www.youtube.com", "youtube.com"]:
        return parse_qs(parsed.query).get("v", [None])[0]
    if parsed.hostname == "youtu.be":
        return parsed.path[1:]
    return None

def get_youtube_title(video_id: str):
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"
        oembed = f"https://www.youtube.com/oembed?url={url}&format=json"
        r = requests.get(oembed, timeout=5)
        if r.status_code == 200:
            return r.json().get("title", "YouTube Karaoke")
    except:
        pass
    return "YouTube Karaoke"

# ===== API Endpoints =====

@app.get("/tv.html")
def tv_page():
    return FileResponse("static/tv.html")

@app.get("/phone.html")
def tv_page():
    return FileResponse("static/phone.html")

@app.post("/room/create")
def create_room():
    room_id = uuid.uuid4().hex[:6].upper()
    rooms[room_id] = {"queue": [], "current": None}
    return {"room_id": room_id}

@app.get("/room/{room_id}")
def get_room(room_id: str):
    return rooms.get(room_id, {})

@app.get("/server_ip")
def server_ip(request: Request):
    return {
        "ip": request.client.host
    }

@app.get("/search")
def search_youtube(q: str, mode: str = "karaoke"):
    query = q + " karaoke" if mode == "karaoke" else q

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
    }

    results = []

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"ytsearch10:{query}", download=False)

        for entry in info.get("entries", []):
            results.append({
                "video_id": entry["id"],
                "title": entry["title"]
            })

    return results

@app.get("/room/{room_id}/queue")
def get_queue(room_id: str):
    room = rooms.get(room_id)
    if not room:
        return []
    return room["queue"][:5]

@app.post("/room/{room_id}/add")
async def add_song(room_id: str, request: Request):
    data = await request.json()
    video_id = extract_video_id(data.get("url", ""))

    if not video_id:
        return JSONResponse({"error": "Invalid YouTube URL"}, status_code=400)

    title = get_youtube_title(video_id)

    room = rooms[room_id]
    room["queue"].append({
        "video_id": video_id,
        "title": title
    })
    
    # If no song is currently playing and queue was empty before, auto-play first song
    if room["current"] is None and len(room["queue"]) == 1:
        room["current"] = room["queue"].pop(0)
        await broadcast_to_room(room_id, 'current_song', room["current"])
    
    # Broadcast queue update
    await broadcast_to_room(room_id, 'queue_update', room["queue"][:5])
    return {"status": "ok"}

@app.post("/room/{room_id}/next")
async def next_song(room_id: str):
    room = rooms[room_id]
    if room["current"] is None and room["queue"]:
        room["current"] = room["queue"].pop(0)
    
    return room["current"]

@app.get("/room/{room_id}/current")
def get_current(room_id: str):
    room = rooms.get(room_id)
    if not room:
        return None
    return room["current"]

@app.post("/room/{room_id}/finish")
async def finish_song(room_id: str):
    room = rooms.get(room_id)
    if not room:
        return {"error": "Room not found"}

    # Clear current
    room["current"] = None

    # Auto-play next song if queue not empty
    if room["queue"]:
        room["current"] = room["queue"].pop(0)

    # Broadcast updates
    await broadcast_to_room(room_id, "current_song", room["current"])
    await broadcast_to_room(room_id, "queue_update", room["queue"][:5])

    return {"status": "ok"}

@app.post("/room/{room_id}/remove")
async def remove_song(room_id: str, request: Request):
    data = await request.json()
    index = data.get("index")

    room = rooms.get(room_id)
    if not room:
        return JSONResponse({"error": "Room not found"}, status_code=404)

    queue = room["queue"]

    if index is None or index < 0 or index >= len(queue):
        return JSONResponse({"error": "Invalid index"}, status_code=400)

    # Xóa bài
    removed = queue.pop(index)

    # Broadcast cập nhật queue
    await broadcast_to_room(room_id, "queue_update", queue[:5])

    return {"status": "ok", "removed": removed}

@app.post("/room/{room_id}/skip")
async def skip_song(room_id: str):
    room = rooms.get(room_id)
    if not room:
        return {"error": "Room not found"}

    # Clear current
    room["current"] = None

    # Auto play next
    if room["queue"]:
        room["current"] = room["queue"].pop(0)

    # Broadcast state
    await broadcast_to_room(room_id, "current_song", room["current"])
    await broadcast_to_room(room_id, "queue_update", room["queue"][:5])

    return {"status": "skipped"}

@app.post("/room/{room_id}/reorder")
async def reorder_queue(room_id: str, request: Request):
    data = await request.json()
    new_order = data.get("order", [])

    room = rooms.get(room_id)
    if not room:
        return JSONResponse({"error": "Room not found"}, status_code=404)

    # Dùng **toàn bộ queue**, không chỉ [:5]
    full_queue = room["queue"]

    # Map video_id -> song
    song_map = {song["video_id"]: song for song in full_queue}

    # Tạo queue mới theo order gửi lên
    new_queue = [song_map[sid] for sid in new_order if sid in song_map]

    room["queue"] = new_queue

    # Broadcast **chỉ 5 bài đầu** vẫn được TV hiển thị đúng
    await broadcast_to_room(room_id, "queue_update", room["queue"][:5])
    return JSONResponse({"status": "ok", "queue": room["queue"][:5]})

# ===== Disable cache =====
@app.middleware("http")
async def no_cache(request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    return response

# ===== Static =====
app.mount("/", StaticFiles(directory="static", html=True), name="static")

# ===== WebSocket Events =====
@sio.event
async def connect(sid, environ):
    print(f"✓ Client connected: {sid}")

@sio.event
async def disconnect(sid):
    print(f"✗ Client disconnected: {sid}")
    # Remove from all rooms
    for room_id in list(room_clients.keys()):
        if sid in room_clients[room_id]:
            room_clients[room_id].remove(sid)
            if not room_clients[room_id]:
                del room_clients[room_id]

@sio.event
async def join_room(sid, data):
    room_id = data.get("room_id")
    if not room_id:
        return

    # ❌ Room không tồn tại
    if room_id not in rooms:
        await sio.emit("room_invalid", to=sid)
        print(f"✗ User {sid} tried to join invalid room {room_id}")
        return

    # ✅ Room hợp lệ
    if room_id not in room_clients:
        room_clients[room_id] = set()

    room_clients[room_id].add(sid)
    await sio.enter_room(sid, room_id)

    print(f"✓ User {sid} joined room {room_id}")

    room = rooms[room_id]

    # Gửi trạng thái ban đầu
    await sio.emit("room_joined", {"room_id": room_id}, to=sid)
    await sio.emit("current_song", room.get("current"), to=sid)
    await sio.emit("queue_update", room.get("queue", [])[:5], to=sid)

@sio.event
async def leave_room(sid, data):
    room_id = data.get("room_id")
    if room_id and room_id in room_clients:
        room_clients[room_id].discard(sid)
        if not room_clients[room_id]:
            del room_clients[room_id]
    await sio.leave_room(sid, room_id)

async def broadcast_to_room(room_id, event, data):
    """Broadcast event to all clients in a room"""
    if room_id in room_clients and room_clients[room_id]:
        await sio.emit(event, data, room=room_id)

app = ASGIApp(sio, app)