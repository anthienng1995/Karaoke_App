from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from urllib.parse import urlparse, parse_qs, quote
import uuid
import requests
import re
import socketio
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from socketio import ASGIApp
import yt_dlp
import httpx
import json

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

# ===== YouTube Suggestion System =====

class TTLCache:
    """Thread-safe in-memory TTL cache with automatic expiration.
    
    Redis-ready: Easy to swap with Redis by implementing same interface.
    """
    def __init__(self, ttl_seconds: int = 3600):
        self.cache: Dict[str, Tuple[float, any]] = {}
        self.ttl = ttl_seconds
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[any]:
        """Get value from cache if not expired."""
        async with self._lock:
            if key not in self.cache:
                return None
            
            expires_at, value = self.cache[key]
            if time.time() > expires_at:
                del self.cache[key]
                return None
            
            return value
    
    async def set(self, key: str, value: any):
        """Set value in cache with TTL."""
        async with self._lock:
            expires_at = time.time() + self.ttl
            self.cache[key] = (expires_at, value)
    
    async def clear_expired(self):
        """Remove expired entries (call periodically)."""
        async with self._lock:
            now = time.time()
            expired = [k for k, (exp, _) in self.cache.items() if now > exp]
            for k in expired:
                del self.cache[k]

class RateLimiter:
    """Simple per-IP rate limiter (sliding window).
    
    Redis-ready: Can be replaced with Redis-based rate limiter.
    """
    def __init__(self, max_requests: int = 60, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests: Dict[str, List[float]] = {}
        self._lock = asyncio.Lock()
    
    async def is_allowed(self, ip: str) -> bool:
        """Check if IP is within rate limit."""
        async with self._lock:
            now = time.time()
            cutoff = now - self.window
            
            if ip not in self.requests:
                self.requests[ip] = []
            
            # Remove old requests outside window
            self.requests[ip] = [t for t in self.requests[ip] if t > cutoff]
            
            if len(self.requests[ip]) >= self.max_requests:
                return False
            
            self.requests[ip].append(now)
            return True

# Initialize suggestion system components
suggestion_cache = TTLCache(ttl_seconds=3600)  # 1 hour cache
rate_limiter = RateLimiter(max_requests=60, window_seconds=60)  # 60 req/min per IP

# Reusable httpx client with connection pooling
http_client: Optional[httpx.AsyncClient] = None

async def get_http_client() -> httpx.AsyncClient:
    """Get or create singleton httpx client with UTF-8 encoding support."""
    global http_client
    if http_client is None:
        http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(2.0, connect=1.0),  # Aggressive timeout for speed
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            headers={
                "Accept-Charset": "utf-8",
            },
            follow_redirects=True,
        )
    return http_client

async def close_http_client():
    """Cleanup httpx client on shutdown."""
    global http_client
    if http_client:
        await http_client.aclose()
        http_client = None

def normalize_query(query: str) -> str:
    """Normalize search query: trim, collapse spaces (keep case for Vietnamese)."""
    return re.sub(r'\s+', ' ', query.strip())

async def fetch_youtube_suggestions(query: str) -> List[str]:
    """Fetch suggestions from YouTube's public suggestion API.
    
    Uses: https://suggestqueries.google.com/complete/search
    Returns list of suggestion strings (text-only).
    Supports Vietnamese and other Unicode characters.
    """
    client = await get_http_client()
    
    # YouTube suggestion API endpoint
    url = "https://suggestqueries.google.com/complete/search"
    params = {
        "client": "firefox",
        "ds": "yt",
        "q": query,
        "hl": "vi"  # Set language to Vietnamese for better results
    }
    
    try:
        # Explicitly set headers for UTF-8 encoding
        headers = {
            "Accept": "*/*",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        print(f"[DEBUG] Fetching suggestions for query: '{query}'")
        response = await client.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        print(f"[DEBUG] Response status: {response.status_code}")
        print(f"[DEBUG] Response headers: {dict(response.headers)}")
        
        # Read raw bytes first to handle encoding properly
        raw_bytes = response.content
        print(f"[DEBUG] Raw bytes length: {len(raw_bytes)}")
        
        # Try to decode with UTF-8, with fallback strategies
        text_content = None
        for encoding in ['utf-8', 'utf-8-sig', 'latin1']:
            try:
                text_content = raw_bytes.decode(encoding)
                print(f"[DEBUG] Successfully decoded with {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        # Last resort: decode with error handling
        if text_content is None:
            text_content = raw_bytes.decode('utf-8', errors='replace')
            print(f"[DEBUG] Decoded with UTF-8 error replacement")
        
        print(f"[DEBUG] Text content (first 500 chars): {text_content[:500]}")
        
        # Parse JSON from text
        # YouTube API returns format: ["query", [["suggestion1", ...], ...], ...]
        # But sometimes it might be wrapped in JSONP like window.google.ac.h(...)
        try:
            # Try to extract JSON from JSONP wrapper if present
            text_clean = text_content.strip()
            if text_clean.startswith('window.google.ac.h(') or text_clean.startswith('google.ac.h('):
                # Extract JSON part from JSONP
                start = text_clean.find('(') + 1
                end = text_clean.rfind(')')
                if start > 0 and end > start:
                    text_clean = text_clean[start:end].strip()
                    print(f"[DEBUG] Extracted JSON from JSONP wrapper")
            
            data = json.loads(text_clean)
            print(f"[DEBUG] Parsed JSON type: {type(data)}, length: {len(data) if isinstance(data, list) else 'N/A'}")
            
            if isinstance(data, dict):
                # Sometimes it's a dict with 'q' and 's' keys
                if 's' in data:
                    suggestions_list = data['s']
                    print(f"[DEBUG] Found suggestions in dict format, count: {len(suggestions_list) if isinstance(suggestions_list, list) else 'N/A'}")
                    if isinstance(suggestions_list, list):
                        result = []
                        for s in suggestions_list:
                            if isinstance(s, str):
                                result.append(s)
                            elif isinstance(s, list) and len(s) > 0:
                                result.append(str(s[0]))
                        print(f"[DEBUG] Returning {len(result)} suggestions from dict format")
                        return result
        except json.JSONDecodeError as json_err:
            # If JSON parsing fails, try to clean the response
            print(f"[DEBUG] JSON decode error, attempting to fix: {json_err}")
            print(f"[DEBUG] Problematic text (first 500 chars): {text_content[:500]}")
            # Try with error replacement
            try:
                text_clean = raw_bytes.decode('utf-8', errors='ignore').strip()
                if text_clean.startswith('window.google.ac.h(') or text_clean.startswith('google.ac.h('):
                    start = text_clean.find('(') + 1
                    end = text_clean.rfind(')')
                    if start > 0 and end > start:
                        text_clean = text_clean[start:end].strip()
                data = json.loads(text_clean)
            except:
                print(f"[DEBUG] Failed to parse even after cleanup")
                return []
        
        # Response format: ["query", [["suggestion1", ...], ...], ...]
        # We only need the suggestions array (index 1)
        if isinstance(data, list):
            print(f"[DEBUG] Data is list with {len(data)} elements")
            if len(data) > 1:
                suggestions = data[1]
                print(f"[DEBUG] Suggestions array type: {type(suggestions)}, length: {len(suggestions) if isinstance(suggestions, list) else 'N/A'}")
                
                # Extract just the text
                result = []
                for idx, s in enumerate(suggestions):
                    print(f"[DEBUG] Suggestion {idx}: {type(s)} = {s}")
                    if isinstance(s, str):
                        # Direct string suggestion
                        if s.strip():
                            result.append(s)
                            print(f"[DEBUG] Added suggestion (string): {s}")
                    elif isinstance(s, list) and len(s) > 0:
                        # List format: [suggestion_text, ...]
                        suggestion_text = s[0] if isinstance(s[0], str) else str(s[0])
                        if suggestion_text.strip():
                            result.append(suggestion_text)
                            print(f"[DEBUG] Added suggestion (list): {suggestion_text}")
                
                print(f"[DEBUG] Returning {len(result)} suggestions")
                return result
            else:
                print(f"[DEBUG] Data list has only {len(data)} elements, expected at least 2")
        else:
            print(f"[DEBUG] Data is not a list: {type(data)}, value: {data}")
        
        print(f"[DEBUG] Returning empty list")
        return []
    
    except (httpx.HTTPError, httpx.TimeoutException) as e:
        # Network/timeout errors
        print(f"[ERROR] YouTube suggestion API network error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return []
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        # Encoding/parsing errors
        print(f"[ERROR] YouTube suggestion API parse error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return []
    except Exception as e:
        # Catch any other unexpected errors
        print(f"[ERROR] Unexpected error in fetch_youtube_suggestions: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return []

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

@app.get("/suggest")
async def get_suggestions(q: str, request: Request):
    """Ultra-fast YouTube search suggestions endpoint.
    
    Args:
        q: Search query (minimum 2 characters)
        request: FastAPI request object (for IP-based rate limiting)
    
    Returns:
        JSON list of suggestion strings, e.g., ["suggestion 1", "suggestion 2", ...]
    
    Features:
        - <100ms response time (cached)
        - In-memory TTL cache (1 hour)
        - Per-IP rate limiting (60 req/min)
        - Graceful error handling
        - Fully async
        - Full Vietnamese Unicode support
    
    Example response:
        ["sơn tùng mtp", "sơn tùng mtp có chắc yêu là đây", "sơn tùng mtp latest"]
    """
    # Get client IP for rate limiting
    client_ip = request.client.host if request.client else "unknown"
    
    # Rate limiting check
    if not await rate_limiter.is_allowed(client_ip):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please slow down."
        )
    
    # Normalize and validate query (keep original case for Vietnamese)
    normalized = normalize_query(q)
    
    if len(normalized) < 2:
        return JSONResponse(
            content={"error": "Query must be at least 2 characters"},
            status_code=400
        )
    
    print(f"[DEBUG] /suggest endpoint called with query: '{q}' -> normalized: '{normalized}'")
    
    # Check cache first (use lowercase for cache key to avoid duplicates)
    cache_key = normalized.lower()
    cached = await suggestion_cache.get(cache_key)
    if cached is not None:
        # Ensure we return a proper list with UTF-8 headers
        result = cached if isinstance(cached, list) else []
        print(f"[DEBUG] Returning cached suggestions: {len(result)} items")
        return JSONResponse(content=result, headers={"Content-Type": "application/json; charset=utf-8"})
    
    # Fetch from YouTube API (use normalized query with original case)
    print(f"[DEBUG] Fetching from YouTube API for: '{normalized}'")
    suggestions = await fetch_youtube_suggestions(normalized)
    
    print(f"[DEBUG] Received {len(suggestions)} suggestions from YouTube API")
    
    # Ensure we have a list (even if empty)
    if not isinstance(suggestions, list):
        suggestions = []
    
    # Cache the result (use lowercase key)
    await suggestion_cache.set(cache_key, suggestions)
    
    # Return as JSONResponse to ensure proper encoding headers
    return JSONResponse(content=suggestions, headers={"Content-Type": "application/json; charset=utf-8"})

@app.get("/search")
def search_youtube(q: str, mode: str = "karaoke"):
    """Full search endpoint using yt_dlp (for final search results with metadata).
    
    Note: This is separate from /suggest for real-time suggestions.
    Keep this for the actual song selection workflow.
    """
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

# ===== Cleanup on shutdown =====
@app.on_event("shutdown")
async def shutdown_event():
    await close_http_client()
    print("✓ HTTP client closed")

app = ASGIApp(sio, app)