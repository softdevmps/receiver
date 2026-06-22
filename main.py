import os
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


SECRET_TOKEN = os.environ.get("SHAREW_TOKEN", "")
STATIC_DIR = Path(__file__).parent / "static"

# State shared across WebSocket handlers
_latest_frame: bytes | None = None
_viewers: set[WebSocket] = set()
_sender_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SECRET_TOKEN:
        import sys
        print("WARNING: SHAREW_TOKEN is not set — all connections will be accepted", file=sys.stderr)
    yield


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _check_token(token: str) -> None:
    if SECRET_TOKEN and token != SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.websocket("/ws/sender")
async def sender_endpoint(ws: WebSocket, token: str = Query("")):
    """The macOS sender connects here and pushes JPEG frames."""
    _check_token(token)

    async with _sender_lock:
        # Only one sender at a time
        await ws.accept()

    global _latest_frame
    try:
        while True:
            frame = await ws.receive_bytes()
            _latest_frame = frame
            if _viewers:
                await asyncio.gather(
                    *[_send_to_viewer(v, frame) for v in list(_viewers)],
                    return_exceptions=True,
                )
    except WebSocketDisconnect:
        pass
    finally:
        _latest_frame = None


async def _send_to_viewer(viewer: WebSocket, frame: bytes) -> None:
    try:
        await viewer.send_bytes(frame)
    except Exception:
        _viewers.discard(viewer)


@app.websocket("/ws/viewer")
async def viewer_endpoint(ws: WebSocket, token: str = Query("")):
    """Browser viewers connect here to receive the stream."""
    _check_token(token)
    await ws.accept()
    _viewers.add(ws)

    # Send the latest frame immediately so the viewer doesn't start blank
    if _latest_frame:
        await _send_to_viewer(ws, _latest_frame)

    try:
        # Keep the connection alive; the sender drives all outbound traffic
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _viewers.discard(ws)


@app.get("/status")
async def status():
    return {
        "streaming": _latest_frame is not None,
        "viewers": len(_viewers),
    }
