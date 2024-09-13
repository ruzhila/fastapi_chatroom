from fastapi import FastAPI, HTTPException,  WebSocket, WebSocketDisconnect, Query
from fastapi.responses import FileResponse
import logging
import uvicorn
import time
import asyncio
import datetime

logger = logging.getLogger(__file__)

app = FastAPI()
clients: dict[str, 'Client'] = {}
room_queue: asyncio.Queue['Message'] = asyncio.Queue()


class Message:
    def __init__(self, sender: str, text: str, ctime: datetime.datetime, event_type: str = 'user'):
        self.sender = sender
        self.text = text
        self.ctime = ctime
        self.event_type = event_type

    def to_json(self):
        return {"sender": self.sender, "text": self.text,  "ctime": str(self.ctime), "event": self.event_type}


class Client:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.tx: asyncio.Queue[Message] = asyncio.Queue()

    def send(self, message: Message):
        self.tx.put_nowait(message)

    async def task_recv_from_client(self, ws: WebSocket, rx: asyncio.Queue[Message]):
        while True:
            text = await ws.receive_text()
            message = Message(sender=self.user_id,
                              text=text, ctime=datetime.datetime.now())
            await rx.put(message)

    async def task_send_to_client(self, ws: WebSocket):
        while True:
            message = await self.tx.get()
            if not message:
                return
            await ws.send_json(message.to_json())

    async def serve(self, ws: WebSocket, tx: asyncio.Queue[Message]):
        try:
            await asyncio.gather(self.task_recv_from_client(ws, tx),
                                 self.task_send_to_client(ws))
        except WebSocketDisconnect:
            logging.info(f"ws: {self.user_id} disconnected")
        finally:
            # send None to signal the end of the `task_send_to_client`
            self.tx.put_nowait(None)
            ws.close()


@app.websocket("/chat")
async def chat(ws: WebSocket, user_id: str = Query(title="User ID", description="User ID")):
    if user_id.startswith("@"):
        return HTTPException(status_code=400, detail="User ID should not start with @")

    await ws.accept()

    if user_id in clients:
        return HTTPException(status_code=400, detail="User ID already exists")

    clients[user_id] = Client(user_id)
    join_message = Message(
        sender="@system", text=f"{user_id} joined", ctime=time.time(), event_type="join")
    await room_queue.put(join_message)

    try:
        await clients[user_id].serve(ws, room_queue)
    finally:
        del clients[user_id]
        leave_message = Message(
            sender="@system", text=f"{user_id} leave", ctime=time.time(), event_type="leave")
        await room_queue.put(leave_message)


@app.get("/clients")
async def get_clients():
    return clients.keys()


@app.get("/")
async def read_index():
    return FileResponse('./assets/index.html')


async def dispatch_message():
    logger.info("start dispatch_message")
    while True:
        msg = await room_queue.get()
        if not msg:
            return

        for client in clients.values():
            try:
                client.send(msg)
            except Exception as e:
                del clients[client.user_id]
                logging.error(e)


async def run_app():
    asyncio.create_task(dispatch_message())
    server = uvicorn.Server(uvicorn.Config(app, host='0.0.0.0', port=8000))

    await server.serve()


if __name__ == "__main__":
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        pass
    room_queue.put_nowait(None)
