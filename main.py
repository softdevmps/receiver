import os
import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from collections import deque

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


SECRET_TOKEN = os.environ.get("SHAREW_TOKEN", "")
STATIC_DIR = Path(__file__).parent / "static"
MAX_TELEMETRY = 100

_latest_frame: bytes | None = None
_telemetry_log: deque = deque(maxlen=MAX_TELEMETRY)
_viewers: set[WebSocket] = set()
_sender_connected: bool = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not SECRET_TOKEN:
        import sys
        print("WARNING: SHAREW_TOKEN not set — all connections accepted", file=sys.stderr)
    yield


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _check_token(token: str) -> None:
    if SECRET_TOKEN and token != SECRET_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/status")
async def status():
    return {
        "streaming": _latest_frame is not None,
        "sender_connected": _sender_connected,
        "viewers": len(_viewers),
    }


async def _heartbeat(ws: WebSocket) -> None:
    """Send a ping text every 15s so the sender can detect dead connections."""
    while True:
        await asyncio.sleep(15)
        try:
            await asyncio.wait_for(ws.send_text('{"ping":1}'), timeout=5.0)
        except Exception:
            return


@app.websocket("/ws/sender")
async def sender_endpoint(ws: WebSocket, token: str = Query("")):
    global _latest_frame, _sender_connected
    _check_token(token)
    await ws.accept()
    _sender_connected = True

    hb_task = asyncio.create_task(_heartbeat(ws))
    try:
        while True:
            msg = await ws.receive()

            if msg.get("bytes"):
                # Binary → video frame, broadcast to viewers
                _latest_frame = msg["bytes"]
                if _viewers:
                    await asyncio.gather(
                        *[_push_bytes(v, _latest_frame) for v in list(_viewers)],
                        return_exceptions=True,
                    )

            elif msg.get("text"):
                # Text → telemetry event, store and broadcast
                try:
                    event = json.loads(msg["text"])
                    _telemetry_log.append(event)
                    payload = json.dumps(event)
                    if _viewers:
                        await asyncio.gather(
                            *[_push_text(v, payload) for v in list(_viewers)],
                            return_exceptions=True,
                        )
                except json.JSONDecodeError:
                    pass

    except WebSocketDisconnect:
        pass
    finally:
        hb_task.cancel()
        _sender_connected = False
        _latest_frame = None


async def _push_bytes(viewer: WebSocket, data: bytes) -> None:
    try:
        await asyncio.wait_for(viewer.send_bytes(data), timeout=2.0)
    except Exception:
        _viewers.discard(viewer)


async def _push_text(viewer: WebSocket, text: str) -> None:
    try:
        await asyncio.wait_for(viewer.send_text(text), timeout=2.0)
    except Exception:
        _viewers.discard(viewer)


@app.websocket("/ws/viewer")
async def viewer_endpoint(ws: WebSocket, token: str = Query("")):
    _check_token(token)
    await ws.accept()
    _viewers.add(ws)

    # Send latest frame immediately so the viewer doesn't start blank
    if _latest_frame:
        await _push_bytes(ws, _latest_frame)

    # Send buffered telemetry log so the viewer catches up on history
    for event in list(_telemetry_log):
        await _push_text(ws, json.dumps(event))

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _viewers.discard(ws)
